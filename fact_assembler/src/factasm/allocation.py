"""月结分摊: 把已装配交易的金额按规则 DAG 分摊到最终成本中心, 交付独立账簿。

语义要点
--------
* 规则只有一份: ``nodes`` 列出全部节点 ID, ``edges`` 以 ``from``/``to``/``bps``
  描述分摊边, ``roots`` 把 ``contract_generation_id`` 映射到入口节点。
* 节点 ID 由 ASCII 字母/数字/下划线/连字符组成, 区分大小写。
* 无出边的节点是最终成本中心 (允许直接作为入口); 有出边的节点只中转不入账。
* 每条边是正整数基点; 同一节点全部出边之和必须恰为 10000。
* 重复节点、重复边、重复合同入口、未声明节点、非法权重、任意环都让规则无效
  —— 包括尚未被该版本交易使用的部分 (规则整体校验, 与使用量无关)。
* 一个最终中心的份额 = 从入口到它的所有路径的权重乘积之和。先合并精确份额
  (``fractions.Fraction``), 再一次性分到金额的最小存储单位 (1e-6), 不沿途取整,
  不用浮点比较余数。
* 分摊算法: 对金额取绝对值的整数单位, 每个中心先取精确应得的整数下界,
  剩余单位按小数余数从大到小、同余数按中心 ID 的 ASCII 字典序分配, 最后恢复符号。
  每个可达中心恰有一行 (即使为零); 每笔有符号子额之和恒等于原额。
* 规则可反复汇合: 份额用 DAG 上的动态规划一次算清 (按节点记忆化), 不枚举路径,
  也不对共用入口的交易重复展开子图。
* 事实按键集分页分批读出, 明细分批写入账簿; Python 内存不随事实总量增长。

交付保证
--------
* 只消费指定版本的 ``facts``; 不重跑世代匹配, 不读隔离行, 不从当前版本补行。
* 事实库以只读方式打开, 分摊不会改变事实库。
* 目标已存在则拒绝; 账簿先完整写入同目录临时文件 (含核对数据), 提交并关闭后
  用 ``os.link`` 原子就位, 失败只残留明显标记的临时文件并被立即清理。
* 合法空版本 (已发布但 0 条事实) 交付只有规则与版本标识的空账簿。
"""
from __future__ import annotations

import dataclasses
import datetime as _dt
import hashlib
import heapq
import json
import os
import pathlib
import re
from decimal import Decimal
from fractions import Fraction
from typing import Any, Iterator

from .db import connect_persistent
from .errors import (
    AllocationError,
    FactDatabaseNotFoundError,
    InvalidRulesError,
    LedgerExistsError,
    MissingRootEntryError,
    VersionNotFoundError,
)
from .util import canonical_json

#: 金额最小存储单位: 原金额小数点后六位 -> 整数单位
UNIT_SCALE = 1_000_000
#: 事实分批读取的批大小 (键集分页, 内存不随版本事实总量增长)
FACT_BATCH_SIZE = 1_000
#: 账簿 schema 版本, 写入 ledger_meta 供后续兼容判断
LEDGER_SCHEMA_VERSION = 1

_NODE_ID_RE = re.compile(r"[A-Za-z0-9_-]+")
_TOTAL_BPS = 10_000


# --------------------------------------------------------------------------- #
# 规则解析与校验
# --------------------------------------------------------------------------- #
def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """JSON object_pairs_hook: 任何对象内的重复键都拒绝 (重复合同入口由此拦截)。"""
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise InvalidRulesError(
                f"duplicate key {key!r} in rules JSON object",
                details={"duplicate_key": key},
            )
        out[key] = value
    return out


@dataclasses.dataclass(frozen=True)
class AllocationRules:
    """校验通过的分摊规则; 构造时即完成全部静态校验与份额预计算。"""

    nodes: tuple[str, ...]                       # 声明顺序
    edges: tuple[tuple[str, str, int], ...]      # (from, to, bps), 声明顺序
    roots: dict[str, str]                        # contract_generation_id -> 入口节点
    canonical: str                               # 规范化 JSON (内容)
    digest: str                                  # 内容摘要 sha256
    _adj: dict[str, tuple[tuple[str, Fraction], ...]]
    _dist: dict[str, dict[str, Fraction]]        # 节点 -> {最终中心: 合并份额}

    # ------------------------------------------------------------------ #
    @classmethod
    def parse(cls, text: str) -> "AllocationRules":
        """解析并整体校验规则文本; 任何违规都抛 :class:`InvalidRulesError`。"""
        try:
            raw = json.loads(text, object_pairs_hook=_strict_object)
        except InvalidRulesError:
            raise
        except json.JSONDecodeError as exc:
            raise InvalidRulesError(f"rules are not valid JSON: {exc}") from exc
        if not isinstance(raw, dict):
            raise InvalidRulesError("rules root must be a JSON object")
        unknown = set(raw) - {"nodes", "edges", "roots"}
        if unknown:
            raise InvalidRulesError(
                f"unknown top-level rule keys: {sorted(unknown)}",
                details={"unknown_keys": sorted(unknown)},
            )
        for key in ("nodes", "edges", "roots"):
            if key not in raw:
                raise InvalidRulesError(f"rules missing required key {key!r}")

        nodes = cls._parse_nodes(raw["nodes"])
        node_set = set(nodes)
        edges, adj_bps = cls._parse_edges(raw["edges"], node_set)
        roots = cls._parse_roots(raw["roots"], node_set)
        cls._check_out_bps(adj_bps)
        topo = cls._topological_order(nodes, adj_bps)

        adj: dict[str, tuple[tuple[str, Fraction], ...]] = {
            n: tuple((to, Fraction(bps, _TOTAL_BPS)) for to, bps in outs)
            for n, outs in adj_bps.items()
        }
        dist = cls._compute_shares(adj, topo)

        canonical = canonical_json({
            "nodes": list(nodes),
            "edges": [{"from": f, "to": t, "bps": b} for f, t, b in edges],
            "roots": roots,
        })
        return cls(
            nodes=nodes,
            edges=tuple(edges),
            roots=roots,
            canonical=canonical,
            digest=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
            _adj=adj,
            _dist=dist,
        )

    # ------------------------------------------------------------------ #
    @staticmethod
    def _parse_nodes(raw: Any) -> tuple[str, ...]:
        if not isinstance(raw, list):
            raise InvalidRulesError("'nodes' must be a list")
        nodes: list[str] = []
        seen: set[str] = set()
        for item in raw:
            if not isinstance(item, str) or not _NODE_ID_RE.fullmatch(item):
                raise InvalidRulesError(
                    f"invalid node id {item!r}: must be non-empty ASCII "
                    "[A-Za-z0-9_-] (case-sensitive)",
                    details={"node": item if isinstance(item, str) else repr(item)},
                )
            if item in seen:
                raise InvalidRulesError(f"duplicate node id {item!r}",
                                        details={"node": item})
            seen.add(item)
            nodes.append(item)
        return tuple(nodes)

    @staticmethod
    def _parse_edges(raw: Any, node_set: set[str]) -> tuple[
        list[tuple[str, str, int]], dict[str, list[tuple[str, int]]]
    ]:
        if not isinstance(raw, list):
            raise InvalidRulesError("'edges' must be a list")
        edges: list[tuple[str, str, int]] = []
        adj: dict[str, list[tuple[str, int]]] = {}
        seen: set[tuple[str, str]] = set()
        for item in raw:
            if not isinstance(item, dict):
                raise InvalidRulesError(f"edge must be an object, got {item!r}")
            unknown = set(item) - {"from", "to", "bps"}
            if unknown:
                raise InvalidRulesError(
                    f"edge has unknown keys {sorted(unknown)}",
                    details={"edge": item, "unknown_keys": sorted(unknown)},
                )
            frm, to, bps = item.get("from"), item.get("to"), item.get("bps")
            for label, value in (("from", frm), ("to", to)):
                if not isinstance(value, str) or value not in node_set:
                    raise InvalidRulesError(
                        f"edge references undeclared node {value!r} in {label!r}",
                        details={"edge": item},
                    )
            if not isinstance(bps, int) or isinstance(bps, bool) or not (1 <= bps <= _TOTAL_BPS):
                raise InvalidRulesError(
                    f"edge bps must be an integer in [1, 10000], got {bps!r}",
                    details={"edge": item},
                )
            if (frm, to) in seen:
                raise InvalidRulesError(
                    f"duplicate edge {frm!r} -> {to!r}",
                    details={"from": frm, "to": to},
                )
            seen.add((frm, to))
            edges.append((frm, to, bps))
            adj.setdefault(frm, []).append((to, bps))
        return edges, adj

    @staticmethod
    def _parse_roots(raw: Any, node_set: set[str]) -> dict[str, str]:
        if not isinstance(raw, dict):
            raise InvalidRulesError("'roots' must be an object mapping "
                                    "contract_generation_id to an entry node")
        roots: dict[str, str] = {}
        for gen_id, entry in raw.items():
            if not isinstance(gen_id, str) or not gen_id:
                raise InvalidRulesError(f"invalid root key {gen_id!r}")
            if not isinstance(entry, str) or entry not in node_set:
                raise InvalidRulesError(
                    f"root for {gen_id!r} points to undeclared node {entry!r}",
                    details={"contract_generation_id": gen_id, "entry": entry},
                )
            roots[gen_id] = entry
        return roots

    @staticmethod
    def _check_out_bps(adj: dict[str, list[tuple[str, int]]]) -> None:
        for node, outs in adj.items():
            total = sum(bps for _, bps in outs)
            if total != _TOTAL_BPS:
                raise InvalidRulesError(
                    f"node {node!r} out-edge bps sum is {total}, must be exactly 10000",
                    details={"node": node, "out_bps_sum": total},
                )

    @staticmethod
    def _topological_order(
        nodes: tuple[str, ...], adj: dict[str, list[tuple[str, int]]]
    ) -> list[str]:
        """Kahn 拓扑排序; 任意环 (含未被 roots 触及的部分) 都让规则无效。"""
        indeg: dict[str, int] = {n: 0 for n in nodes}
        for frm, outs in adj.items():
            for to, _ in outs:
                indeg[to] += 1
        heap = [n for n in nodes if indeg[n] == 0]
        heapq.heapify(heap)
        order: list[str] = []
        while heap:
            node = heapq.heappop(heap)
            order.append(node)
            for to, _ in adj.get(node, ()):
                indeg[to] -= 1
                if indeg[to] == 0:
                    heapq.heappush(heap, to)
        if len(order) != len(nodes):
            placed = set(order)
            cyclic = sorted(n for n in nodes if n not in placed)
            raise InvalidRulesError(
                f"rules contain a cycle involving nodes {cyclic}",
                details={"cyclic_nodes": cyclic},
            )
        return order

    @staticmethod
    def _compute_shares(
        adj: dict[str, tuple[tuple[str, Fraction], ...]],
        topo: list[str],
    ) -> dict[str, dict[str, Fraction]]:
        """DAG 上逆拓扑动态规划: 每个节点到各最终中心的合并精确份额。

        对整张图只算一次; 入口节点 (roots 的目标) 共用下游子图, 不枚举路径,
        也不按交易重复展开。
        """
        dist: dict[str, dict[str, Fraction]] = {}
        for node in reversed(topo):
            outs = adj.get(node)
            if not outs:
                dist[node] = {node: Fraction(1)}  # 最终成本中心: 全额落自己
                continue
            merged: dict[str, Fraction] = {}
            for child, weight in outs:
                for sink, share in dist[child].items():
                    merged[sink] = merged.get(sink, Fraction(0)) + weight * share
            dist[node] = merged
        return dist

    # ------------------------------------------------------------------ #
    def entry_for(self, contract_generation_id: str) -> str | None:
        return self.roots.get(contract_generation_id)

    def shares_for_entry(self, entry: str) -> dict[str, Fraction]:
        """入口节点到全部可达最终中心的合并份额 (精确分数, 和恒为 1)。"""
        return self._dist[entry]

    @property
    def cost_centers(self) -> list[str]:
        return sorted(n for n in self.nodes if n not in self._adj)


# --------------------------------------------------------------------------- #
# 金额分摊: 最大余数法 (精确分数, 不用浮点)
# --------------------------------------------------------------------------- #
def allocate_units(abs_units: int, shares: dict[str, Fraction]) -> dict[str, int]:
    """把非负整数单位按合并份额分到各中心。

    每中心先取精确应得的整数下界; 剩余单位按小数余数从大到小、同余数按
    中心 ID 的 ASCII 字典序各补 1。返回 {中心: 非负单位}, 和恒等于 abs_units。
    """
    floors: dict[str, int] = {}
    remainders: dict[str, Fraction] = {}
    total_floor = 0
    for center, share in shares.items():
        exact = abs_units * share
        floor = exact.numerator // exact.denominator
        floors[center] = floor
        remainders[center] = exact - floor
        total_floor += floor
    leftover = abs_units - total_floor  # 整数, 0 <= leftover < 中心数
    units = dict(floors)
    if leftover:
        by_remainder = sorted(shares, key=lambda c: (-remainders[c], c))
        for center in by_remainder[:leftover]:
            units[center] += 1
    return units


def amount_to_units(amount: Decimal) -> int:
    """DECIMAL(28,6) 金额 -> 整数最小存储单位 (精确, 不取舍)。"""
    scaled = amount * UNIT_SCALE
    integral = scaled.to_integral_value()
    if scaled != integral:  # 防御: facts.amount 恒为 6 位小数, 不应触发
        raise AllocationError(  # pragma: no cover - 防御路径
            f"amount {amount} has more than 6 decimal places"
        )
    return int(integral)


# --------------------------------------------------------------------------- #
# 账簿 schema
# --------------------------------------------------------------------------- #
_LEDGER_SCHEMA_SQL = """
CREATE TABLE ledger_meta (
    singleton            INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    schema_version       INTEGER NOT NULL,
    version_id           VARCHAR NOT NULL,   -- 所选已发布版本
    transaction_batch_id VARCHAR NOT NULL,   -- 该版本的交易批次
    rules_sha256         VARCHAR NOT NULL,   -- 规则内容摘要 (canonical JSON)
    rules_json           VARCHAR NOT NULL,   -- 规则内容 (canonical JSON)
    fact_count           BIGINT NOT NULL,
    allocation_count     BIGINT NOT NULL,
    created_at           TIMESTAMP NOT NULL
);

-- 逐笔控制数: 原行身份、入选合同世代、原额整数单位 (有符号)
CREATE TABLE control (
    txn_row_id             VARCHAR NOT NULL PRIMARY KEY,
    contract_generation_id VARCHAR NOT NULL,
    entry_node             VARCHAR NOT NULL,
    amount_units           HUGEINT NOT NULL
);

-- 入账明细: 以 (原行 ID, 成本中心) 为键; 份额分子/分母为任意精度整数文本
CREATE TABLE allocation (
    txn_row_id        VARCHAR NOT NULL,
    cost_center       VARCHAR NOT NULL,
    amount_units      HUGEINT NOT NULL,   -- 有符号整数单位
    share_numerator   VARCHAR NOT NULL,   -- 该中心合并后份额的精确分子
    share_denominator VARCHAR NOT NULL,   -- 精确分母
    PRIMARY KEY (txn_row_id, cost_center)
);

-- 自足核对: 每笔控制数与明细合计之差必须恒为 0
CREATE VIEW reconciliation AS
SELECT c.txn_row_id,
       c.amount_units AS control_units,
       COALESCE(SUM(a.amount_units), 0) AS allocated_units,
       c.amount_units - COALESCE(SUM(a.amount_units), 0) AS unallocated_units
FROM control c
LEFT JOIN allocation a ON a.txn_row_id = c.txn_row_id
GROUP BY c.txn_row_id, c.amount_units;
"""


# --------------------------------------------------------------------------- #
# 分摊执行
# --------------------------------------------------------------------------- #
@dataclasses.dataclass
class AllocationResult:
    out_path: str
    version_id: str
    transaction_batch_id: str
    rules_sha256: str
    fact_count: int
    allocation_count: int
    cost_centers: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": "DELIVERED",
            "out": self.out_path,
            "version_id": self.version_id,
            "transaction_batch_id": self.transaction_batch_id,
            "rules_sha256": self.rules_sha256,
            "fact_count": self.fact_count,
            "allocation_count": self.allocation_count,
            "cost_centers": self.cost_centers,
        }


def load_rules_file(path: str | os.PathLike[str]) -> AllocationRules:
    p = pathlib.Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise InvalidRulesError(f"cannot read rules file {p}: {exc}") from exc
    return AllocationRules.parse(text)


def allocate(
    db_path: str | os.PathLike[str],
    *,
    version_id: str,
    rules: AllocationRules,
    out_path: str | os.PathLike[str],
    fact_batch_size: int = FACT_BATCH_SIZE,
) -> AllocationResult:
    """对指定已发布版本执行月结分摊, 交付独立账簿文件。

    失败抛 :class:`AllocationError` 子类; 目标文件已存在拒绝覆盖;
    任何失败都不会在目标路径留下文件, 也不会改变事实库。
    """
    out = pathlib.Path(out_path)
    if out.exists():
        raise LedgerExistsError(f"ledger target already exists: {out}",
                                details={"out": str(out)})
    if not pathlib.Path(db_path).exists():
        raise FactDatabaseNotFoundError(
            f"fact database not found: {db_path}",
            details={"db": str(db_path)},
        )
    if fact_batch_size < 1:
        raise AllocationError(f"fact_batch_size must be >= 1, got {fact_batch_size}")
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise AllocationError(f"cannot create ledger directory {out.parent}: {exc}",
                              details={"out": str(out)}) from exc

    # 只读打开事实库: 不建 schema、不写 WAL, 分摊绝不改变事实库
    con = connect_persistent(db_path, read_only=True)
    try:
        version_row = con.execute(
            "SELECT version_id, transaction_batch_id, fact_count "
            "FROM fact_versions WHERE version_id = ?",
            [version_id],
        ).fetchone()
        if version_row is None:
            raise VersionNotFoundError(
                f"published version {version_id!r} not found",
                details={"version_id": version_id},
            )
        _, transaction_batch_id, _ = version_row

        # 先整体验证入口覆盖: 任一交易的合同世代缺入口 -> 整份账簿不出账
        used_generations = [
            r[0] for r in con.execute(
                "SELECT DISTINCT contract_generation_id FROM facts "
                "WHERE version_id = ? ORDER BY 1",
                [version_id],
            ).fetchall()
        ]
        missing = [g for g in used_generations if rules.entry_for(g) is None]
        if missing:
            raise MissingRootEntryError(
                f"rules have no root entry for contract generations used by "
                f"version {version_id}: {missing}",
                details={"version_id": version_id, "missing_roots": missing},
            )

        # 账簿先写同目录临时文件; 提交并关闭后才原子就位, 失败不留成品
        tmp = out.parent / f"{out.name}.tmp-{os.getpid()}"
        for suffix in ("", ".wal"):
            pathlib.Path(str(tmp) + suffix).unlink(missing_ok=True)

        fact_count = 0
        allocation_count = 0
        try:
            ledger = _connect_ledger(tmp)
            try:
                ledger.execute(_LEDGER_SCHEMA_SQL)
                ledger.execute("BEGIN TRANSACTION")
                for batch in _iter_fact_batches(con, version_id, fact_batch_size):
                    control_rows, alloc_rows = _allocate_batch(batch, rules)
                    if control_rows:
                        ledger.executemany("INSERT INTO control VALUES (?, ?, ?, ?)",
                                           control_rows)
                    if alloc_rows:
                        ledger.executemany(
                            "INSERT INTO allocation VALUES (?, ?, ?, ?, ?)",
                            alloc_rows,
                        )
                    fact_count += len(control_rows)
                    allocation_count += len(alloc_rows)
                ledger.execute(
                    "INSERT INTO ledger_meta VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [LEDGER_SCHEMA_VERSION, version_id, transaction_batch_id,
                     rules.digest, rules.canonical, fact_count, allocation_count,
                     _dt.datetime.utcnow()],
                )
                ledger.execute("COMMIT")
            finally:
                ledger.close()
            # 原子就位: os.link 在目标已存在时失败, 绝不覆盖既有账簿
            try:
                os.link(tmp, out)
            except FileExistsError as exc:
                raise LedgerExistsError(
                    f"ledger target appeared during delivery: {out}",
                    details={"out": str(out)},
                ) from exc
        finally:
            for suffix in ("", ".wal"):
                pathlib.Path(str(tmp) + suffix).unlink(missing_ok=True)
    finally:
        con.close()

    return AllocationResult(
        out_path=str(out),
        version_id=version_id,
        transaction_batch_id=transaction_batch_id,
        rules_sha256=rules.digest,
        fact_count=fact_count,
        allocation_count=allocation_count,
        cost_centers=rules.cost_centers,
    )


def _connect_ledger(path: pathlib.Path):
    import duckdb

    con = duckdb.connect(str(path))
    con.execute("PRAGMA memory_limit='256MB'")
    con.execute("PRAGMA threads=2")
    return con


def _iter_fact_batches(
    con, version_id: str, batch_size: int
) -> Iterator[list[tuple[str, str, Decimal]]]:
    """键集分页分批读出指定版本的 facts; 每批 (txn_row_id, 合同世代, 金额)。

    只读指定版本: 不重跑世代匹配, 不读隔离行, 不从当前版本补行。
    """
    last_id: str | None = None
    while True:
        if last_id is None:
            rows = con.execute(
                "SELECT txn_row_id, contract_generation_id, amount FROM facts "
                "WHERE version_id = ? ORDER BY txn_row_id LIMIT ?",
                [version_id, batch_size],
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT txn_row_id, contract_generation_id, amount FROM facts "
                "WHERE version_id = ? AND txn_row_id > ? "
                "ORDER BY txn_row_id LIMIT ?",
                [version_id, last_id, batch_size],
            ).fetchall()
        if not rows:
            return
        last_id = rows[-1][0]
        yield rows


def _allocate_batch(
    batch: list[tuple[str, str, Decimal]], rules: AllocationRules
) -> tuple[list[tuple], list[tuple]]:
    """分摊一批事实, 返回 (控制数行, 入账明细行); 调用方写库后即可释放。"""
    control_rows: list[tuple] = []
    alloc_rows: list[tuple] = []
    for txn_row_id, generation_id, amount in batch:
        entry = rules.entry_for(generation_id)
        if entry is None:  # pragma: no cover - allocate() 已整体验证入口覆盖
            raise MissingRootEntryError(
                f"no root entry for contract generation {generation_id!r}",
                details={"txn_row_id": txn_row_id,
                         "contract_generation_id": generation_id},
            )
        shares = rules.shares_for_entry(entry)
        units = amount_to_units(amount)
        sign = -1 if units < 0 else 1
        allocated = allocate_units(abs(units), shares)
        control_rows.append((txn_row_id, generation_id, entry, units))
        for center in sorted(allocated):
            share = shares[center]
            alloc_rows.append((
                txn_row_id,
                center,
                sign * allocated[center],
                str(share.numerator),
                str(share.denominator),
            ))
    return control_rows, alloc_rows
