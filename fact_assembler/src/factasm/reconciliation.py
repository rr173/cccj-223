"""回单勾稽台账: 把支付渠道回单与 ``allocate`` 账簿的入账明细持续对到一起。

背景与契约
----------
* 一张回单可覆盖多条入账明细, 一条明细也可被多张分期回单共同覆盖; 渠道只给
  回单总额与关联明细, **不给每条关联的拆分金额**。因此台账不从金额猜测关联,
  也不重新分摊/改写账簿: ``allocation`` 是固定入账侧。
* 一条入账明细以 ``(txn_row_id, cost_center)`` 标识, 回单以 ``receipt_id``
  标识 (非空字符串, 区分大小写)。
* 初始化只接收一份 **已有 ``allocate`` 生成的账簿**, 按只读方式复制所需数据
  (明细键、有符号整数单位) 与账簿版本/交易批次/规则摘要到勾稽库; 原账簿此后
  不再被打开。已有勾稽库拒绝重新初始化。
* 之后每批变更为 JSONL (``op=upsert`` 整行替换回单金额与关联, ``op=delete``
  撤销回单及全部关联)。批号从 1 连续递增; 已接收批号以完全相同的文件字节重送
  时幂等不重复应用, 内容不同/跳号/回退一律拒绝。
* 一批要么整批成功、要么整批拒绝: 回单、关联、成员、汇总、批号在同一事务内
  一起更新。
* 直接或间接关联的回单与明细属于同一组 (无向连通分量); 孤立节点各自成组。
  单边组 (只有回单或只有明细) 即使合计为零也不得报「相符」。
* 增量重算范围: 受影响的旧组 = 被改动回单原先所在的组 ∪ 本批新关联指向的
  明细原先所在的组; 只对这批旧组做 BFS 重算, 再处理只在新增节点内的新分量。
  不遍历无关组、不为定位受影响成员重扫全量关联、不重写其他组的成员及汇总。

台账内可直接用 DuckDB 查询的视图
---------------------------------
``recon_group_summary``   每组: 两侧金额合计、差额 (回单−入账)、状态
``recon_group_members``   组内每个节点; 回单可回到 ``receipt_id``/金额, 明细可
                          回到明细键/金额, ``direct_peer_count`` 为回单的直接关联数
``recon_receipt_links``   回单的直接关联 (回单 → 明细, 含两侧当前金额)
``recon_member_groups``   成员 → 当前所在组; 与 ``recon_group_members`` 连接
                          即得整组成员, 且回单 ID 与明细键不必长得像
``recon_batches``         已接收批次与文件指纹
``recon_metadata``        版本/交易批次/规则摘要与 schema 版本

重算范围的可核查依据写在 ``recon_recompute_log`` (每批每组的触发原因、成员
总数与本次实际扫描/访问的关联与节点数) 与 ``recon_batch_report`` (每批计数)。
"""
from __future__ import annotations

import dataclasses
import datetime as _dt
import hashlib
import json
import os
import pathlib
import re
from typing import Any, Iterable

import duckdb

from .errors import (
    BatchSequenceError,
    ChangesValidationError,
    ReconcileLedgerError,
    ReconciliationError,
    StateExistsError,
    StateNotFoundError,
)
from .util import canonical_json

#: 勾稽库 schema 版本 (写入 recon_metadata, 供后续兼容判断)
STATE_SCHEMA_VERSION = 1
#: 单条 SQL 的 IN (...) 参数上限; 超出则拆分执行, 参数始终走 DuckDB 绑定
IN_CHUNK_SIZE = 250


#: 明细节点键的内部编码 (canonical JSON 数组, 任意业务键都无歧义)
def _detail_node_key(txn_row_id: str, cost_center: str) -> str:
    return canonical_json([txn_row_id, cost_center])


#: 回单 ID 拒绝控制字符 (JSON 里的 \u0000 等仍算格式错误)
_BAD_RECEIPT_CHARS = re.compile(r"[\x00-\x1f\x7f]")
#: HUGEINT 有符号范围
_HUGEINT_MAX = 2 ** 127 - 1
_HUGEINT_MIN = -(2 ** 127)


# --------------------------------------------------------------------------- #
# 结果对象
# --------------------------------------------------------------------------- #
@dataclasses.dataclass
class ReconcileInitResult:
    state_path: str
    ledger_path: str
    version_id: str
    transaction_batch_id: str
    rules_sha256: str
    detail_count: int
    group_count: int
    ledger_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {"status": "INITIALIZED", "state": self.state_path,
                "ledger": self.ledger_path, "version_id": self.version_id,
                "transaction_batch_id": self.transaction_batch_id,
                "rules_sha256": self.rules_sha256,
                "detail_count": self.detail_count,
                "group_count": self.group_count,
                "ledger_sha256": self.ledger_sha256}


@dataclasses.dataclass
class ReconcileBatchResult:
    seq: int
    applied: bool
    reused: bool
    receipt_upserts: int
    receipt_deletes: int
    group_count: int
    status_counts: dict[str, int]
    scanned_link_endpoints: int
    visited_node_count: int
    affected_old_group_count: int
    unaffected_group_count: int
    input_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": "APPLIED" if self.applied else "REUSED",
            "seq": self.seq,
            "receipt_upserts": self.receipt_upserts,
            "receipt_deletes": self.receipt_deletes,
            "group_count": self.group_count,
            "status_counts": self.status_counts,
            "recompute_scope": {
                "affected_old_group_count": self.affected_old_group_count,
                "unaffected_group_count": self.unaffected_group_count,
                "scanned_link_endpoints": self.scanned_link_endpoints,
                "visited_node_count": self.visited_node_count,
            },
            "input_sha256": self.input_sha256,
        }


# --------------------------------------------------------------------------- #
# 状态库 schema
# --------------------------------------------------------------------------- #
_STATE_SCHEMA_SQL = """
CREATE TABLE recon_metadata (
    singleton               INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    schema_version          INTEGER NOT NULL,
    source_ledger_path      VARCHAR NOT NULL,
    ledger_sha256           VARCHAR NOT NULL,
    version_id              VARCHAR NOT NULL,
    transaction_batch_id    VARCHAR NOT NULL,
    rules_sha256            VARCHAR NOT NULL,
    rules_json              VARCHAR NOT NULL,
    ledger_schema_version   INTEGER NOT NULL,
    ledger_fact_count       BIGINT NOT NULL,
    ledger_allocation_count BIGINT NOT NULL,
    detail_count            BIGINT NOT NULL,
    status_json             VARCHAR NOT NULL,   -- 初始化后各状态组数 (批次增量计数的起点)
    initialized_at          TIMESTAMP NOT NULL
);

-- 固定入账侧: 初始化时只读复制自 allocate 账簿的 allocation, 之后不再打开原账簿
CREATE TABLE recon_ledger_detail (
    txn_row_id   VARCHAR NOT NULL,
    cost_center  VARCHAR NOT NULL,
    amount_units HUGEINT NOT NULL,
    PRIMARY KEY (txn_row_id, cost_center)
);

-- 成员节点: 回单或明细的稳定内部编号; 回单删除后节点行保留以便同 ID 复用
CREATE TABLE recon_node (
    node_id     INTEGER NOT NULL PRIMARY KEY,
    node_kind   VARCHAR NOT NULL CHECK (node_kind IN ('RECEIPT', 'DETAIL')),
    node_key    VARCHAR NOT NULL,
    txn_row_id  VARCHAR,
    cost_center VARCHAR,
    CHECK ((node_kind = 'DETAIL') = (txn_row_id IS NOT NULL)),
    CHECK ((node_kind = 'DETAIL') = (cost_center IS NOT NULL))
);
CREATE UNIQUE INDEX recon_node_kind_key_idx ON recon_node (node_kind, node_key);
CREATE UNIQUE INDEX recon_node_detail_idx ON recon_node (txn_row_id, cost_center);

CREATE TABLE recon_receipt (
    receipt_id   VARCHAR NOT NULL PRIMARY KEY,
    node_id      INTEGER NOT NULL UNIQUE,
    amount_units HUGEINT NOT NULL,
    last_seq     BIGINT NOT NULL,
    FOREIGN KEY (node_id) REFERENCES recon_node(node_id)
);

-- 关联以节点号存储 (回单节点, 明细节点); 同一回单内重复关联由主键兜底
CREATE TABLE recon_link (
    receipt_node_id INTEGER NOT NULL,
    detail_node_id  INTEGER NOT NULL,
    created_seq     BIGINT NOT NULL,
    PRIMARY KEY (receipt_node_id, detail_node_id),
    FOREIGN KEY (receipt_node_id) REFERENCES recon_node(node_id),
    FOREIGN KEY (detail_node_id) REFERENCES recon_node(node_id)
);
CREATE INDEX recon_link_detail_idx ON recon_link (detail_node_id);
CREATE INDEX recon_link_receipt_idx ON recon_link (receipt_node_id);

-- 成员: 每个现存回单/明细恰一行; group_id 为本台账分配的稳定分组号
CREATE TABLE recon_member (
    node_id  INTEGER NOT NULL PRIMARY KEY,
    group_id BIGINT NOT NULL,
    FOREIGN KEY (node_id) REFERENCES recon_node(node_id)
);
CREATE INDEX recon_member_group_idx ON recon_member (group_id);

CREATE TABLE recon_group (
    group_id      BIGINT NOT NULL PRIMARY KEY,
    status        VARCHAR NOT NULL CHECK (status IN
                  ('ONLY_RECEIPT', 'ONLY_DETAIL', 'MATCHED', 'DIFFERENCE')),
    receipt_total HUGEINT NOT NULL,
    detail_total  HUGEINT NOT NULL,
    difference    HUGEINT NOT NULL,
    receipt_count BIGINT NOT NULL,
    detail_count  BIGINT NOT NULL,
    last_seq      BIGINT NOT NULL,
    CHECK (difference = receipt_total - detail_total),
    CHECK (status <> 'MATCHED' OR (receipt_count > 0 AND detail_count > 0
                                   AND difference = 0))
);

-- 已接收批次 (seq 连续; 同 seq 同字节重送幂等)
CREATE TABLE recon_batch (
    seq          BIGINT NOT NULL PRIMARY KEY,
    input_sha256 VARCHAR NOT NULL,
    byte_count   BIGINT NOT NULL,
    upserts      BIGINT NOT NULL,
    deletes      BIGINT NOT NULL,
    applied_at   TIMESTAMP NOT NULL
);

-- 本批实际访问到的节点 (scope): 用于复跑核对「只触及了哪些组」, 不靠运行时间
CREATE TABLE recon_batch_scope_node (
    seq     BIGINT NOT NULL,
    node_id INTEGER NOT NULL,
    PRIMARY KEY (seq, node_id),
    FOREIGN KEY (seq) REFERENCES recon_batch(seq),
    FOREIGN KEY (node_id) REFERENCES recon_node(node_id)
);

CREATE TABLE recon_batch_report (
    seq                     BIGINT NOT NULL PRIMARY KEY,
    input_sha256            VARCHAR NOT NULL,
    status_json             VARCHAR NOT NULL,
    group_count             BIGINT NOT NULL,
    affected_old_group_count BIGINT NOT NULL,
    unaffected_group_count  BIGINT NOT NULL,
    scanned_link_endpoints  BIGINT NOT NULL,
    visited_node_count      BIGINT NOT NULL,
    applied_at              TIMESTAMP NOT NULL,
    FOREIGN KEY (seq) REFERENCES recon_batch(seq)
);

-- 每批每受重算单元一行: 为什么重算、旧/新组、成员数与本次实际扫描/访问量
CREATE TABLE recon_recompute_log (
    seq              BIGINT NOT NULL,
    old_group_id     BIGINT,            -- NULL = 本批新形成的组 / 新回单
    new_group_id     BIGINT,            -- NULL = 该旧组已整体消失
    reason           VARCHAR NOT NULL,  -- MODIFIED_RECEIPT / NEWLY_LINKED_DETAIL / NEW_RECEIPT
    old_member_count BIGINT NOT NULL,
    new_member_count BIGINT NOT NULL,
    old_receipt_count BIGINT NOT NULL,
    old_detail_count  BIGINT NOT NULL,
    retained_members BIGINT NOT NULL,   -- 旧组成员中仍留在该新分量里的
    removed_members  BIGINT NOT NULL,   -- 旧组成员中已不在任何分量里的
    inserted_members BIGINT NOT NULL,   -- 该新分量里不属于该旧组的成员
    scanned_links    BIGINT NOT NULL,   -- 本批为它实际读取的关联行数
    visited_nodes    BIGINT NOT NULL,   -- 形成该新分量实际访问的节点数
    FOREIGN KEY (seq) REFERENCES recon_batch(seq)
);

-- 单行设置: 下一个分组号/节点号 (单调递增, 不复用)
CREATE TABLE recon_counter (
    name  VARCHAR NOT NULL PRIMARY KEY,
    value BIGINT NOT NULL
);

CREATE VIEW recon_group_summary AS
SELECT group_id, status, receipt_total, detail_total, difference,
       receipt_count, detail_count, last_seq
FROM recon_group;

CREATE VIEW recon_receipt_links AS
SELECT rr.receipt_id,
       rr.amount_units AS receipt_amount_units,
       dn.txn_row_id,
       dn.cost_center,
       d.amount_units AS detail_amount_units,
       l.created_seq
FROM recon_link l
JOIN recon_receipt rr ON rr.node_id = l.receipt_node_id
JOIN recon_node dn ON dn.node_id = l.detail_node_id
LEFT JOIN recon_ledger_detail d
       ON d.txn_row_id = dn.txn_row_id AND d.cost_center = dn.cost_center;

CREATE VIEW recon_group_members AS
SELECT m.group_id,
       n.node_kind AS member_kind,
       n.node_id,
       CASE WHEN n.node_kind = 'RECEIPT' THEN r.receipt_id END AS receipt_id,
       n.txn_row_id,
       n.cost_center,
       CASE WHEN n.node_kind = 'RECEIPT' THEN r.amount_units
            ELSE d.amount_units END AS amount_units,
       CASE WHEN n.node_kind = 'RECEIPT' THEN
            (SELECT COUNT(*) FROM recon_link x WHERE x.receipt_node_id = n.node_id)
       END AS direct_peer_count
FROM recon_member m
JOIN recon_node n ON n.node_id = m.node_id
LEFT JOIN recon_receipt r ON r.node_id = n.node_id AND n.node_kind = 'RECEIPT'
LEFT JOIN recon_ledger_detail d
       ON d.txn_row_id = n.txn_row_id AND d.cost_center = n.cost_center
      AND n.node_kind = 'DETAIL';

CREATE VIEW recon_member_groups AS
SELECT m.group_id,
       n.node_kind AS member_kind,
       n.node_id,
       CASE WHEN n.node_kind = 'RECEIPT' THEN r.receipt_id END AS receipt_id,
       n.txn_row_id,
       n.cost_center
FROM recon_member m
JOIN recon_node n ON n.node_id = m.node_id
LEFT JOIN recon_receipt r ON r.node_id = n.node_id AND n.node_kind = 'RECEIPT';

CREATE VIEW recon_batches AS
SELECT seq, input_sha256, byte_count, upserts, deletes, applied_at
FROM recon_batch;
"""


# --------------------------------------------------------------------------- #
# 连接辅助
# --------------------------------------------------------------------------- #
def _new_state_connection(path: pathlib.Path):
    con = duckdb.connect(str(path))
    try:
        con.execute("PRAGMA memory_limit='256MB'")
        con.execute("PRAGMA threads=2")
        con.execute("PRAGMA enable_object_cache=false")
        con.execute(_STATE_SCHEMA_SQL)
    except BaseException:
        con.close()
        raise
    return con


def _open_state(path: pathlib.Path, *, read_only: bool = False):
    if not path.exists():
        raise StateNotFoundError(f"reconciliation state not found: {path}",
                                 details={"state": str(path)})
    con = duckdb.connect(str(path), read_only=read_only)
    try:
        ok = con.execute(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'recon_metadata'"
        ).fetchone()[0]
        if not ok:
            raise StateNotFoundError(
                f"{path} is not a reconciliation state (initialize it first)",
                details={"state": str(path)})
    except BaseException:
        con.close()
        raise
    return con


def _file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _chunks(seq: list[Any], size: int = IN_CHUNK_SIZE) -> Iterable[list[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _fetch_in(con, sql_template: str, values: list[Any]) -> list[tuple]:
    """执行 ``... IN (SELECT id FROM {v})`` 模板, 值全部走参数绑定。"""
    out: list[tuple] = []
    for chunk in _chunks(values):
        places = ", ".join(["(?)"] * len(chunk))
        table = f"(SELECT * FROM (VALUES {places}) AS v(id))"
        out.extend(con.execute(sql_template.format(v=table), list(chunk)).fetchall())
    return out


# --------------------------------------------------------------------------- #
# 初始化: 只读消费 allocate 账簿
# --------------------------------------------------------------------------- #
def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _read_ledger(ledger_path: pathlib.Path):
    """读取 allocate 账簿的 meta 与固定入账明细; 全程只读, 读完即关闭。"""
    con = duckdb.connect(str(ledger_path), read_only=True)
    try:
        tables = {r[0] for r in con.execute(
            "SELECT table_name FROM duckdb_tables()").fetchall()}
        if "ledger_meta" not in tables:
            raise ReconcileLedgerError(
                f"{ledger_path} has no ledger_meta; it was not produced by allocate",
                details={"ledger": str(ledger_path)})
        if "allocation" not in tables:
            raise ReconcileLedgerError(
                f"{ledger_path} has no allocation table; it was not produced by allocate",
                details={"ledger": str(ledger_path)})
        meta_rows = con.execute(
            "SELECT version_id, transaction_batch_id, rules_sha256, rules_json, "
            "schema_version, fact_count, allocation_count FROM ledger_meta"
        ).fetchall()
        if len(meta_rows) != 1:
            raise ReconcileLedgerError(
                f"ledger_meta must have exactly one row, got {len(meta_rows)}",
                details={"ledger": str(ledger_path)})
        version_id, txn_batch, rules_sha, rules_json, ledger_schema, fact_count, alloc_count = meta_rows[0]
        try:
            details = con.execute(
                "SELECT txn_row_id, cost_center, amount_units FROM allocation "
                "ORDER BY txn_row_id, cost_center").fetchall()
        except Exception as exc:
            raise ReconcileLedgerError(
                f"cannot read allocation.amount_units as integer units: {exc}",
                details={"ledger": str(ledger_path)}) from exc
    except ReconcileLedgerError:
        raise
    except Exception as exc:
        raise ReconcileLedgerError(f"cannot read ledger {ledger_path}: {exc}",
                                   details={"ledger": str(ledger_path)}) from exc
    finally:
        con.close()

    for txn_row_id, cost_center, units in details:
        if not isinstance(txn_row_id, str) or not txn_row_id:
            raise ReconcileLedgerError(f"ledger has invalid txn_row_id {txn_row_id!r}")
        if not isinstance(cost_center, str) or not cost_center:
            raise ReconcileLedgerError(f"ledger has invalid cost_center {cost_center!r}")
        if isinstance(units, bool) or not isinstance(units, int):
            raise ReconcileLedgerError(
                f"allocation.amount_units is not an integer for "
                f"{(txn_row_id, cost_center)}: {units!r}")
    if len(details) != int(alloc_count):
        raise ReconcileLedgerError(
            f"allocation row count {len(details)} does not match ledger_meta "
            f"allocation_count {alloc_count}", details={"ledger": str(ledger_path)})
    return (version_id, txn_batch, rules_sha, rules_json, int(ledger_schema),
            int(fact_count), int(alloc_count), details)


def initialize_state(
    ledger_path: str | os.PathLike[str],
    state_path: str | os.PathLike[str],
) -> ReconcileInitResult:
    """用一份 allocate 账簿初始化勾稽库; 原账簿只读, 已有勾稽库拒绝重新初始化。"""
    ledger = pathlib.Path(ledger_path)
    state = pathlib.Path(state_path)
    if not ledger.exists():
        raise ReconcileLedgerError(f"allocation ledger not found: {ledger}",
                                   details={"ledger": str(ledger)})
    if state.exists():
        raise StateExistsError(
            f"reconciliation state already exists, refusing to reinitialize: {state}",
            details={"state": str(state)})
    try:
        state.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ReconciliationError(f"cannot create state directory {state.parent}: {exc}",
                                  details={"state": str(state)}) from exc

    sha_before = _file_sha256(ledger)
    (version_id, txn_batch, rules_sha, rules_json, ledger_schema,
     fact_count, alloc_count, details) = _read_ledger(ledger)
    if _file_sha256(ledger) != sha_before:  # pragma: no cover - 并发写防御
        raise ReconcileLedgerError("ledger changed while being read",
                                   details={"ledger": str(ledger)})

    tmp = state.parent / f"{state.name}.tmp-{os.getpid()}"
    for suffix in ("", ".wal"):
        pathlib.Path(str(tmp) + suffix).unlink(missing_ok=True)
    try:
        con = _new_state_connection(tmp)
        try:
            con.execute("BEGIN TRANSACTION")
            initial_status = ({"ONLY_DETAIL": len(details)} if details else {})
            con.execute(
                "INSERT INTO recon_metadata VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [STATE_SCHEMA_VERSION, str(ledger), sha_before, version_id,
                 txn_batch, rules_sha, rules_json, ledger_schema, fact_count,
                 alloc_count, len(details), canonical_json(initial_status),
                 _dt.datetime.utcnow()],
            )
            if details:
                con.executemany(
                    "INSERT INTO recon_node VALUES (?, 'DETAIL', ?, ?, ?)",
                    [(i, _detail_node_key(txn, cc), txn, cc)
                     for i, (txn, cc, _u) in enumerate(details, start=1)])
                con.executemany(
                    "INSERT INTO recon_ledger_detail VALUES (?, ?, ?)", details)
                con.executemany(
                    "INSERT INTO recon_member VALUES (?, ?)",
                    [(i, i) for i in range(1, len(details) + 1)])
                con.executemany(
                    "INSERT INTO recon_group VALUES (?, 'ONLY_DETAIL', 0, ?, ?, 0, 1, 0)",
                    [(i, units, -units)
                     for i, (_t, _c, units) in enumerate(details, start=1)])
            con.execute("INSERT INTO recon_counter VALUES ('__next_node_id', ?)",
                        [len(details) + 1])
            con.execute("INSERT INTO recon_counter VALUES ('__next_group_id', ?)",
                        [len(details) + 1])
            con.execute("INSERT INTO recon_counter VALUES ('__group_count', ?)",
                        [len(details)])
            con.execute("COMMIT")
        finally:
            con.close()
        try:
            os.link(tmp, state)
        except FileExistsError as exc:
            raise StateExistsError(
                f"reconciliation state appeared during initialization: {state}",
                details={"state": str(state)}) from exc
    finally:
        for suffix in ("", ".wal"):
            pathlib.Path(str(tmp) + suffix).unlink(missing_ok=True)

    return ReconcileInitResult(
        state_path=str(state), ledger_path=str(ledger), version_id=version_id,
        transaction_batch_id=txn_batch, rules_sha256=rules_sha,
        detail_count=len(details), group_count=len(details),
        ledger_sha256=_file_sha256(ledger))


# --------------------------------------------------------------------------- #
# 变更文件解析 (严格 JSONL)
# --------------------------------------------------------------------------- #
@dataclasses.dataclass(frozen=True)
class ReceiptUpsert:
    receipt_id: str
    amount_units: int
    links: tuple[tuple[str, str], ...]   # 文件顺序、内部去重后


@dataclasses.dataclass
class _ChangeBatch:
    upserts: dict[str, ReceiptUpsert]
    deletes: list[str]

    @property
    def changed_receipt_ids(self) -> list[str]:
        return sorted(self.upserts) + sorted(self.deletes)


def _reject(message: str, *, seq: int, line_no: int | None = None,
            receipt_id: str | None = None, field: str | None = None):
    details: dict[str, Any] = {"seq": seq}
    if line_no is not None:
        details["line"] = line_no
    if receipt_id is not None:
        details["receipt_id"] = receipt_id
    if field is not None:
        details["field"] = field
    return ChangesValidationError(message, details=details)


def _validate_receipt_id(rid: Any, seq: int, line_no: int) -> str:
    if not isinstance(rid, str) or not rid:
        raise _reject(f"receipt_id must be a non-empty string, got {rid!r}",
                      seq=seq, line_no=line_no, field="receipt_id")
    if _BAD_RECEIPT_CHARS.search(rid):
        raise _reject("receipt_id contains control characters",
                      seq=seq, line_no=line_no, receipt_id=rid, field="receipt_id")
    return rid


def parse_changes(raw: bytes, *, seq: int) -> _ChangeBatch:
    """解析一批变更; 任何格式/语义问题都拒绝整批 (不返回部分结果)。"""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _reject(f"changes file is not valid UTF-8: {exc}", seq=seq) from exc
    if text.startswith("﻿"):
        text = text[1:]

    upserts: dict[str, ReceiptUpsert] = {}
    deletes: list[str] = []
    seen: set[str] = set()
    for line_no, line in enumerate(text.splitlines(), start=1):
        if line == "":
            continue
        if not line.strip():
            raise _reject("blank/whitespace-only line inside changes file",
                          seq=seq, line_no=line_no)
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise _reject(f"line is not valid JSON: {exc}",
                          seq=seq, line_no=line_no) from exc
        if not isinstance(obj, dict):
            raise _reject("each line must be a JSON object", seq=seq, line_no=line_no)

        op = obj.get("op")
        if op == "upsert":
            extra = set(obj) - {"op", "receipt_id", "amount_units", "links"}
            if extra:
                raise _reject(f"upsert has unknown keys {sorted(extra)}",
                              seq=seq, line_no=line_no)
            missing = {"receipt_id", "amount_units", "links"} - set(obj)
            if missing:
                raise _reject(f"upsert missing required keys {sorted(missing)}",
                              seq=seq, line_no=line_no)
            rid = _validate_receipt_id(obj["receipt_id"], seq, line_no)
            amount = obj["amount_units"]
            if isinstance(amount, bool) or not isinstance(amount, int):
                raise _reject(
                    "amount_units must be a signed integer at ledger precision, "
                    f"got {amount!r}", seq=seq, line_no=line_no,
                    receipt_id=rid, field="amount_units")
            if not (_HUGEINT_MIN <= amount <= _HUGEINT_MAX):
                raise _reject("amount_units is outside the signed 128-bit range",
                              seq=seq, line_no=line_no, receipt_id=rid,
                              field="amount_units")
            links_raw = obj["links"]
            if not isinstance(links_raw, list):
                raise _reject("links must be an array (possibly empty)",
                              seq=seq, line_no=line_no, receipt_id=rid, field="links")
            links: list[tuple[str, str]] = []
            seen_links: set[tuple[str, str]] = set()
            for pos, link in enumerate(links_raw):
                field = f"links[{pos}]"
                if not isinstance(link, dict):
                    raise _reject(f"{field} must be an object", seq=seq,
                                  line_no=line_no, receipt_id=rid, field=field)
                if set(link) != {"txn_row_id", "cost_center"}:
                    raise _reject(f"{field} must have exactly txn_row_id and cost_center",
                                  seq=seq, line_no=line_no, receipt_id=rid, field=field)
                txn, center = link["txn_row_id"], link["cost_center"]
                if not isinstance(txn, str) or not txn:
                    raise _reject(f"{field}.txn_row_id must be a non-empty string",
                                  seq=seq, line_no=line_no, receipt_id=rid,
                                  field=f"{field}.txn_row_id")
                if not isinstance(center, str) or not center:
                    raise _reject(f"{field}.cost_center must be a non-empty string",
                                  seq=seq, line_no=line_no, receipt_id=rid,
                                  field=f"{field}.cost_center")
                key = (txn, center)
                if key in seen_links:
                    raise _reject(
                        f"duplicate link to {(txn, center)!r} within receipt {rid!r}",
                        seq=seq, line_no=line_no, receipt_id=rid, field=field)
                seen_links.add(key)
                links.append(key)
            if rid in seen:
                raise _reject(
                    f"receipt {rid!r} is modified more than once in batch {seq}",
                    seq=seq, line_no=line_no, receipt_id=rid)
            seen.add(rid)
            upserts[rid] = ReceiptUpsert(rid, amount, tuple(links))
        elif op == "delete":
            # 撤销行只允许 op 与 receipt_id 两个键 (金额/关联一律不读)
            if set(obj) != {"op", "receipt_id"}:
                extra = sorted(set(obj) - {"op", "receipt_id"})
                missing = sorted({"receipt_id"} - set(obj))
                if extra:
                    raise _reject(f"delete has unknown keys {extra}",
                                  seq=seq, line_no=line_no)
                raise _reject(f"delete is missing keys {missing}",
                              seq=seq, line_no=line_no)
            rid = _validate_receipt_id(obj["receipt_id"], seq, line_no)
            if rid in seen:
                raise _reject(
                    f"receipt {rid!r} is modified more than once in batch {seq}",
                    seq=seq, line_no=line_no, receipt_id=rid)
            seen.add(rid)
            deletes.append(rid)
        else:
            raise _reject(f"op must be 'upsert' or 'delete', got {op!r}",
                          seq=seq, line_no=line_no, field="op")
    return _ChangeBatch(upserts, deletes)


# --------------------------------------------------------------------------- #
# 台账内状态读取 (全部限定在本批相关 key/节点, 不扫全量)
# --------------------------------------------------------------------------- #
def _node_id_for_receipt(con, receipt_id: str) -> int | None:
    """回单节点号 (即使该回单当前已撤销, 节点行仍保留以便同 ID 复用)。"""
    row = con.execute(
        "SELECT node_id FROM recon_node WHERE node_kind = 'RECEIPT' "
        "AND node_key = ?", [receipt_id]).fetchone()
    return None if row is None else int(row[0])


def _receipt_node_if_present(con, receipt_id: str) -> int | None:
    """仅当回单当前存在 (recon_receipt 有行) 时返回其节点号。"""
    row = con.execute(
        "SELECT node_id FROM recon_receipt WHERE receipt_id = ?", [receipt_id]).fetchone()
    return None if row is None else int(row[0])


def _detail_node_ids(con, keys: list[tuple[str, str]]) -> dict[tuple[str, str], int]:
    out: dict[tuple[str, str], int] = {}
    for chunk in _chunks(keys):
        places = ", ".join(["(?, ?)"] * len(chunk))
        params: list[Any] = []
        for txn, cc in chunk:
            params.extend([txn, cc])
        rows = con.execute(
            "SELECT txn_row_id, cost_center, node_id FROM recon_node "
            "WHERE node_kind = 'DETAIL' AND (txn_row_id, cost_center) IN "
            f"(SELECT * FROM (VALUES {places}) AS k(txn_row_id, cost_center))",
            params).fetchall()
        for txn, cc, nid in rows:
            out[(txn, cc)] = int(nid)
    return out


def _groups_for_node_ids(con, node_ids: list[int]) -> dict[int, int]:
    out: dict[int, int] = {}
    for nid, gid in _fetch_in(
            con, "SELECT node_id, group_id FROM recon_member "
                 "WHERE node_id IN (SELECT id FROM {v})", node_ids):
        out[int(nid)] = int(gid)
    return out


def _validate_against_state(con, batch: _ChangeBatch, seq: int) -> None:
    """引用完整性: 删除必须存在; 关联必须指向固定入账侧里的明细。"""
    for rid in batch.deletes:
        if _receipt_node_if_present(con, rid) is None:
            raise _reject(f"cannot delete unknown receipt {rid!r}",
                          seq=seq, receipt_id=rid)
    missing: list[tuple[str, str]] = []
    for upsert in batch.upserts.values():
        for key in upsert.links:
            row = con.execute(
                "SELECT 1 FROM recon_ledger_detail WHERE txn_row_id = ? "
                "AND cost_center = ?", list(key)).fetchone()
            if row is None:
                missing.append(key)
    if missing:
        raise ChangesValidationError(
            f"batch {seq} links to {len(missing)} detail key(s) not present in "
            "the allocation ledger",
            details={"seq": seq, "missing_detail_keys":
                     [{"txn_row_id": t, "cost_center": c} for t, c in missing]})


# --------------------------------------------------------------------------- #
# 增量重算
# --------------------------------------------------------------------------- #
def _next_counter(con, name: str) -> int:
    row = con.execute("SELECT value FROM recon_counter WHERE name = ?", [name]).fetchone()
    return int(row[0])


def _bump_counter(con, name: str, by: int) -> int:
    start = _next_counter(con, name)
    con.execute("UPDATE recon_counter SET value = value + ? WHERE name = ?", [by, name])
    return start


def _links_for_receipts(con, receipt_nodes: list[int]) -> tuple[set[tuple[int, int]], int]:
    """读取指定回单节点**当前**的关联 (调用时本批 upsert/delete 已写入)。

    不再按回单过滤旧行: 事务内数据库已是本批之后的关系, 只读 scope 内的回单。
    """
    links: set[tuple[int, int]] = set()
    scanned = 0
    for chunk in _chunks(receipt_nodes):
        places = ", ".join(["(?)"] * len(chunk))
        rows = con.execute(
            "SELECT receipt_node_id, detail_node_id FROM recon_link "
            f"WHERE receipt_node_id IN (SELECT * FROM (VALUES {places}) AS v(id))",
            list(chunk)).fetchall()
        scanned += len(rows)
        for r, d in rows:
            links.add((int(r), int(d)))
    return links, scanned


def _components(nodes: Iterable[int], edges: set[tuple[int, int]]) -> list[list[int]]:
    """无向图连通分量; 孤立节点各自成组。返回按最小节点号排序的分量。"""
    adj: dict[int, list[int]] = {n: [] for n in nodes}
    for r, d in edges:
        if r in adj and d in adj:
            adj[r].append(d)
            adj[d].append(r)
    seen: set[int] = set()
    comps: list[list[int]] = []
    for start in sorted(adj):
        if start in seen:
            continue
        stack = [start]
        comp: list[int] = []
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            comp.append(node)
            stack.extend(adj[node])
        comps.append(sorted(comp))
    comps.sort(key=lambda c: c[0])
    return comps


def _node_kinds(con, node_ids: list[int]) -> dict[int, str]:
    out: dict[int, str] = {}
    for nid, kind in _fetch_in(
            con, "SELECT node_id, node_kind FROM recon_node "
                 "WHERE node_id IN (SELECT id FROM {v})", node_ids):
        out[int(nid)] = kind
    return out


def _group_totals(con, receipt_nodes: list[int], detail_nodes: list[int]) -> tuple[
        dict[int, int], dict[int, int]]:
    receipt_amt: dict[int, int] = {}
    detail_amt: dict[int, int] = {}
    if receipt_nodes:
        for nid, units in _fetch_in(
                con, "SELECT node_id, amount_units FROM recon_receipt "
                     "WHERE node_id IN (SELECT id FROM {v})", receipt_nodes):
            receipt_amt[int(nid)] = int(units)
    if detail_nodes:
        for nid, units in _fetch_in(
                con,
                "SELECT n.node_id, d.amount_units FROM recon_node n "
                "JOIN recon_ledger_detail d ON d.txn_row_id = n.txn_row_id "
                "AND d.cost_center = n.cost_center "
                "WHERE n.node_id IN (SELECT id FROM {v})", detail_nodes):
            detail_amt[int(nid)] = int(units)
    return receipt_amt, detail_amt


def _status(receipt_count: int, detail_count: int, difference: int) -> str:
    if receipt_count == 0 and detail_count == 0:  # pragma: no cover - 空分量不落库
        raise ReconciliationError("empty group cannot be classified")
    if receipt_count == 0:
        return "ONLY_DETAIL"        # 单边: 即使 detail_total 为 0 也不得报 MATCHED
    if detail_count == 0:
        return "ONLY_RECEIPT"
    return "MATCHED" if difference == 0 else "DIFFERENCE"


def _chunked_values_sql(n: int) -> str:
    return ", ".join(["(?)"] * n)


def _apply_batch(con, seq: int, batch: _ChangeBatch,
                 sha: str, nbytes: int) -> ReconcileBatchResult:
    """在调用方开启的事务内应用一批变更; 任何异常都由调用方回滚。"""
    _validate_against_state(con, batch, seq)

    changed_ids = batch.changed_receipt_ids
    # 组数维护在 counter 里: 每批只做增量, 不扫全量 recon_group 去 COUNT(*)
    total_groups_before = _next_counter(con, "__group_count")
    # 上一状态计数: 优先取最近批次报告, 否则取初始化时写入的起点。
    # 两条路径都不扫全量 recon_group。
    previous_report = con.execute(
        "SELECT status_json FROM recon_batch_report ORDER BY seq DESC LIMIT 1"
    ).fetchone()
    if previous_report is not None:
        running_status = json.loads(previous_report[0])
    else:
        running_status = json.loads(con.execute(
            "SELECT status_json FROM recon_metadata").fetchone()[0])

    # -- 1. 记下旧状态 (只查本批相关 key) -------------------------------- #
    old_receipt_node: dict[str, int] = {}
    for rid in changed_ids:
        nid = _receipt_node_if_present(con, rid)
        if nid is not None:
            old_receipt_node[rid] = nid
    old_receipt_group = _groups_for_node_ids(con, list(old_receipt_node.values()))

    upsert_link_keys: list[tuple[str, str]] = []
    seen_keys: set[tuple[str, str]] = set()
    for upsert in batch.upserts.values():
        for key in upsert.links:
            if key not in seen_keys:
                seen_keys.add(key)
                upsert_link_keys.append(key)
    existing_detail_nodes = _detail_node_ids(con, upsert_link_keys)
    old_detail_group = _groups_for_node_ids(con, list(existing_detail_nodes.values()))

    reasons: dict[int, set[str]] = {}
    for nid in old_receipt_node.values():
        gid = old_receipt_group.get(nid)
        if gid is not None:
            reasons.setdefault(gid, set()).add("MODIFIED_RECEIPT")
    for nid in existing_detail_nodes.values():
        gid = old_detail_group.get(nid)
        if gid is not None:
            reasons.setdefault(gid, set()).add("NEWLY_LINKED_DETAIL")
    affected_groups = sorted(reasons)

    # 受影响旧组的全部旧成员 (只读这些组, 不扫全量成员)
    old_member_group: dict[int, int] = {}
    if affected_groups:
        for nid, gid in _fetch_in(
                con, "SELECT node_id, group_id FROM recon_member "
                     "WHERE group_id IN (SELECT id FROM {v})", affected_groups):
            old_member_group[int(nid)] = int(gid)
    old_member_kind = _node_kinds(con, list(old_member_group))
    old_status_by_group: dict[int, str] = {}
    if affected_groups:
        for gid, status in _fetch_in(
                con, "SELECT group_id, status FROM recon_group "
                     "WHERE group_id IN (SELECT id FROM {v})", affected_groups):
            old_status_by_group[int(gid)] = status

    # -- 2. 应用回单与关联变更 (仍在同一事务) ---------------------------- #
    # 明细节点号一次查全 (本批全部 links), 避免逐回单重复查询
    link_node_ids = _detail_node_ids(con, upsert_link_keys)
    for upsert in batch.upserts.values():
        nid = old_receipt_node.get(upsert.receipt_id)
        if nid is None:
            # 已撤销过的回单可能还留着节点行: 复用节点号, 重新插入回单
            nid = _node_id_for_receipt(con, upsert.receipt_id)
            if nid is None:
                nid = _bump_counter(con, "__next_node_id", 1)
                con.execute(
                    "INSERT INTO recon_node (node_id, node_kind, node_key) "
                    "VALUES (?, 'RECEIPT', ?)", [nid, upsert.receipt_id])
            con.execute(
                "INSERT INTO recon_receipt VALUES (?, ?, ?, ?)",
                [upsert.receipt_id, nid, upsert.amount_units, seq])
        else:
            con.execute(
                "UPDATE recon_receipt SET amount_units = ?, last_seq = ? "
                "WHERE receipt_id = ?", [upsert.amount_units, seq, upsert.receipt_id])
        old_receipt_node[upsert.receipt_id] = nid
        con.execute("DELETE FROM recon_link WHERE receipt_node_id = ?", [nid])
        if upsert.links:
            con.executemany(
                "INSERT INTO recon_link VALUES (?, ?, ?)",
                [(nid, link_node_ids[key], seq) for key in upsert.links])
    deleted_receipt_nodes: set[int] = set()
    for rid in batch.deletes:
        nid = old_receipt_node.get(rid) or _receipt_node_if_present(con, rid)
        if nid is None:  # pragma: no cover - 已由引用校验拦截
            raise _reject(f"cannot delete unknown receipt {rid!r}", seq=seq,
                          receipt_id=rid)
        con.execute("DELETE FROM recon_link WHERE receipt_node_id = ?", [nid])
        con.execute("DELETE FROM recon_receipt WHERE receipt_id = ?", [rid])
        deleted_receipt_nodes.add(nid)

    # -- 3. 重算范围: 受影响旧组成员 ∪ 本批新增/回写的节点 --------------- #
    upsert_nodes = {old_receipt_node[rid] for rid in batch.upserts}
    upsert_detail_nodes = set(link_node_ids.values())
    scope: set[int] = set(old_member_group) | upsert_nodes | upsert_detail_nodes
    scope -= deleted_receipt_nodes          # 回单已撤销, 不再是成员
    scope_list = sorted(scope)

    # 本批之后的关联只从 scope 内的回单读取; 跨 scope 的边不可能存在:
    # 若某明细属于受影响旧组, 其所有回单也属于同一旧组, 因而都在 scope。
    edges, scanned_links = _links_for_receipts(
        con, [n for n in scope_list if n in upsert_nodes
              or old_member_kind.get(n) == "RECEIPT"])
    components = _components(scope_list, edges)

    scope_kind = dict(old_member_kind)
    scope_kind.update(_node_kinds(con, [n for n in scope_list
                                        if n not in scope_kind]))
    receipt_nodes = [n for n in scope_list if scope_kind.get(n) == "RECEIPT"]
    detail_nodes = [n for n in scope_list if scope_kind.get(n) == "DETAIL"]
    receipt_amt, detail_amt = _group_totals(con, receipt_nodes, detail_nodes)


    # 分组号: 若某个新分量恰好完整继承一个旧组 (该旧组全部存活成员都在这个
    # 分量里), 沿用旧组号, 无关组的查询不会因本批而换号; 否则分配新号。
    surviving_by_group: dict[int, list[int]] = {g: [] for g in affected_groups}
    for n in scope_list:
        g = old_member_group.get(n)
        if g is not None:
            surviving_by_group[g].append(n)
    comp_by_node = {}
    for idx, comp in enumerate(components):
        for n in comp:
            comp_by_node[n] = idx
    inherited: dict[int, int] = {}   # 分量下标 -> 沿用的旧组号
    used_group_ids: set[int] = set()
    for gid, members in surviving_by_group.items():
        if not members:              # 整组消失 (例如撤销了只有关联的回单)
            continue
        idxs = {comp_by_node[n] for n in members}
        if len(idxs) != 1:
            continue                 # 拆成多个分量: 需要新号
        idx = idxs.pop()
        old_size = sum(1 for _, g in old_member_group.items() if g == gid)
        if idx in inherited or len(members) != old_size:
            continue                 # 只继承完整未拆的组
        inherited[idx] = gid
        used_group_ids.add(gid)

    next_group = _next_counter(con, "__next_group_id")
    new_group_ids: list[int] = []
    comp_of_node: dict[int, int] = {}
    for idx, comp in enumerate(components):
        if idx in inherited:
            gid = inherited[idx]
        else:
            gid = next_group
            next_group += 1
            used_group_ids.add(gid)
        new_group_ids.append(gid)
        for n in comp:
            comp_of_node[n] = gid
    con.execute("UPDATE recon_counter SET value = ? "
                "WHERE name = '__next_group_id'", [next_group])
    # 先清掉受影响旧组里不再沿用的成员与汇总; 沿用旧号的组稍后整体重写
    stale_groups = [g for g in affected_groups if g not in used_group_ids]
    # 先删旧成员: 沿用旧号的组会在下面整体重写, 拆组时旧成员不能残留到新组
    for chunk in _chunks(affected_groups):
        places = _chunked_values_sql(len(chunk))
        con.execute(f"DELETE FROM recon_member WHERE group_id IN "
                    f"(SELECT * FROM (VALUES {places}) AS v(id))", list(chunk))
    for chunk in _chunks(stale_groups):
        places = _chunked_values_sql(len(chunk))
        con.execute(f"DELETE FROM recon_group WHERE group_id IN "
                    f"(SELECT * FROM (VALUES {places}) AS v(id))", list(chunk))
    for gid in used_group_ids:
        con.execute("DELETE FROM recon_group WHERE group_id = ?", [gid])

    receipt_node_set = set(receipt_nodes)
    new_status_by_group: dict[int, str] = {}
    for comp, gid in zip(components, new_group_ids):
        comp_receipts = [n for n in comp if n in receipt_node_set]
        comp_details = [n for n in comp if n not in receipt_node_set]
        receipt_total = sum(receipt_amt.get(n, 0) for n in comp_receipts)
        detail_total = sum(detail_amt.get(n, 0) for n in comp_details)
        difference = receipt_total - detail_total
        status = _status(len(comp_receipts), len(comp_details), difference)
        con.execute(
            "INSERT INTO recon_group VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [gid, status, receipt_total, detail_total, difference,
             len(comp_receipts), len(comp_details), seq])
        con.executemany("INSERT INTO recon_member VALUES (?, ?)",
                        [(n, gid) for n in comp])
        new_status_by_group[gid] = status

    # 批次报告只对本批改动的组做计数增量 (不扫全表做 GROUP BY):
    # 每个受影响旧组整体替换为 scope 内若干新分量, 状态各扣/加一次。
    group_count = total_groups_before - len(affected_groups) + len(components)
    con.execute("UPDATE recon_counter SET value = ? WHERE name = '__group_count'",
                [group_count])
    status_counts = dict(running_status)
    for gid in affected_groups:
        old = old_status_by_group.get(gid)
        if old is not None:
            status_counts[old] = status_counts.get(old, 0) - 1
    for gid in new_group_ids:
        new = new_status_by_group[gid]
        status_counts[new] = status_counts.get(new, 0) + 1
    status_counts = {k: v for k, v in status_counts.items() if v}

    applied_at = _dt.datetime.utcnow()
    con.execute(
        "INSERT INTO recon_batch VALUES (?, ?, ?, ?, ?, ?)",
        [seq, sha, nbytes, len(batch.upserts), len(batch.deletes), applied_at])
    if scope_list:
        con.executemany(
            "INSERT INTO recon_batch_scope_node VALUES (?, ?)",
            [(seq, n) for n in scope_list])
    con.execute(
        "INSERT INTO recon_batch_report VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [seq, sha, canonical_json(status_counts), group_count,
         len(affected_groups), total_groups_before - len(affected_groups),
         scanned_links, sum(len(c) for c in components), applied_at])

    # -- 5. 记录重算依据 (可复核实际范围) -------------------------------- #
    # 每个受影响旧组对每个包含其旧成员的新分量各一行 (拆组可见);
    # 整组消失的旧组写一行 new_group_id=NULL; 本批新回单写 NEW_RECEIPT 行。
    # scanned_links / visited_nodes 是本批实际读取的关联行与分量访问节点数,
    # 不是估计值, 也不含无关组。
    comp_members: dict[int, list[int]] = {}
    for comp, gid in zip(components, new_group_ids):
        comp_members[gid] = comp
    edge_by_receipt: dict[int, int] = {}
    for r, _d in edges:
        edge_by_receipt[r] = edge_by_receipt.get(r, 0) + 1
    for gid in affected_groups:
        old_members = [n for n, g in old_member_group.items() if g == gid]
        old_receipts = [n for n in old_members if old_member_kind.get(n) == "RECEIPT"]
        old_details = [n for n in old_members if old_member_kind.get(n) == "DETAIL"]
        scans = sum(edge_by_receipt.get(n, 0) for n in old_receipts)
        per_new: dict[int, int] = {}
        for n in old_members:
            g = comp_of_node.get(n)
            if g is not None:
                per_new[g] = per_new.get(g, 0) + 1
        removed = sum(1 for n in old_members if n not in comp_of_node)
        if not per_new:
            con.execute(
                "INSERT INTO recon_recompute_log VALUES "
                "(?, ?, NULL, ?, ?, 0, ?, ?, 0, ?, 0, ?, 0)",
                [seq, gid, ",".join(sorted(reasons[gid])), len(old_members),
                 len(old_receipts), len(old_details), len(old_members), scans])
        for new_gid, retained in sorted(per_new.items()):
            comp = comp_members[new_gid]
            con.execute(
                "INSERT INTO recon_recompute_log VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [seq, gid, new_gid, ",".join(sorted(reasons[gid])),
                 len(old_members), len(comp), len(old_receipts), len(old_details),
                 retained, removed, len(comp) - retained, scans, len(comp)])
    for rid in batch.upserts:
        nid = old_receipt_node[rid]
        if nid in old_receipt_group:
            continue
        gid = comp_of_node.get(nid)
        if gid is None:  # pragma: no cover - 新回单必在 scope
            continue
        comp = comp_members[gid]
        con.execute(
            "INSERT INTO recon_recompute_log VALUES "
            "(?, NULL, ?, 'NEW_RECEIPT', 0, ?, 0, 0, 0, 0, ?, ?, ?)",
            [seq, gid, len(comp), len(comp), edge_by_receipt.get(nid, 0), len(comp)])

    return ReconcileBatchResult(
        seq=seq, applied=True, reused=False,
        receipt_upserts=len(batch.upserts), receipt_deletes=len(batch.deletes),
        group_count=group_count, status_counts=status_counts,
        scanned_link_endpoints=scanned_links,
        visited_node_count=sum(len(c) for c in components),
        affected_old_group_count=len(affected_groups),
        unaffected_group_count=total_groups_before - len(affected_groups),
        input_sha256=sha)


# --------------------------------------------------------------------------- #
# 批次入口: 幂等 / 连续性 / 原子事务
# --------------------------------------------------------------------------- #
def reconcile_batch(
    state_path: str | os.PathLike[str],
    *,
    seq: int,
    changes_path: str | os.PathLike[str],
    changes_bytes: bytes | None = None,
) -> ReconcileBatchResult:
    """应用一个连续批号的变更文件; 同字节重送幂等, 其余不连续/冲突拒绝。

    ``changes_bytes`` 仅供测试直接注入字节; 正常 CLI 从 ``changes_path`` 读取。
    """
    state = pathlib.Path(state_path)
    if isinstance(seq, bool) or not isinstance(seq, int) or seq < 1:
        raise BatchSequenceError(f"seq must be a positive integer, got {seq!r}",
                                 details={"seq": seq})
    if changes_bytes is None:
        path = pathlib.Path(changes_path)
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise ReconciliationError(f"cannot read changes file {path}: {exc}",
                                      details={"changes": str(path)}) from exc
    else:
        raw = changes_bytes
    sha = hashlib.sha256(raw).hexdigest()
    nbytes = len(raw)

    con = _open_state(state)
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            existing = con.execute(
                "SELECT input_sha256, byte_count, upserts, deletes FROM recon_batch "
                "WHERE seq = ?", [seq]).fetchone()
            max_seq = int(con.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM recon_batch").fetchone()[0])
            if existing is not None:
                stored_sha, stored_bytes = existing[0], int(existing[1])
                if stored_sha == sha and stored_bytes == nbytes:
                    report = con.execute(
                        "SELECT status_json, group_count, affected_old_group_count, "
                        "unaffected_group_count, scanned_link_endpoints, "
                        "visited_node_count FROM recon_batch_report WHERE seq = ?",
                        [seq]).fetchone()
                    con.execute("ROLLBACK")
                    return ReconcileBatchResult(
                        seq=seq, applied=False, reused=True,
                        receipt_upserts=int(existing[2]), receipt_deletes=int(existing[3]),
                        group_count=int(report[1]),
                        status_counts=json.loads(report[0]),
                        scanned_link_endpoints=int(report[4]),
                        visited_node_count=int(report[5]),
                        affected_old_group_count=int(report[2]),
                        unaffected_group_count=int(report[3]),
                        input_sha256=sha)
                raise BatchSequenceError(
                    f"batch {seq} was already applied with different file content",
                    details={"seq": seq, "stored_sha256": stored_sha,
                             "input_sha256": sha})
            if seq != max_seq + 1:
                raise BatchSequenceError(
                    f"batch seq must be contiguous: expected {max_seq + 1}, got {seq}",
                    details={"seq": seq, "expected": max_seq + 1,
                             "last_applied": max_seq})

            # 先解析与引用校验, 再进写入阶段。整个批次 (回单/关联/成员/汇总/
            # 批号) 在同一个事务里; 任何一步失败都 ROLLBACK, 全部保持原状。
            batch = parse_changes(raw, seq=seq)
            result = _apply_batch(con, seq, batch, sha, nbytes)
            con.execute("COMMIT")
            return result
        except BaseException:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()


def verify_state(state_path: str | os.PathLike[str]) -> dict[str, Any]:
    """只读自检: 成员/汇总/关联引用一致, 返回可复核的统计。"""
    state = pathlib.Path(state_path)
    con = _open_state(state, read_only=True)
    try:
        checks = {
            "every_receipt_has_member": con.execute(
                "SELECT COUNT(*) FROM recon_receipt r LEFT JOIN recon_member m "
                "ON m.node_id = r.node_id WHERE m.node_id IS NULL").fetchone()[0],
            "every_detail_has_member": con.execute(
                "SELECT COUNT(*) FROM recon_node n LEFT JOIN recon_member m "
                "ON m.node_id = n.node_id WHERE n.node_kind = 'DETAIL' "
                "AND m.node_id IS NULL").fetchone()[0],
            "orphan_links": con.execute(
                "SELECT COUNT(*) FROM recon_link l LEFT JOIN recon_receipt r "
                "ON r.node_id = l.receipt_node_id WHERE r.node_id IS NULL").fetchone()[0],
            "receipts": con.execute("SELECT COUNT(*) FROM recon_receipt").fetchone()[0],
            "details": con.execute(
                "SELECT COUNT(*) FROM recon_node WHERE node_kind = 'DETAIL'").fetchone()[0],
            "groups": con.execute("SELECT COUNT(*) FROM recon_group").fetchone()[0],
            "batches": con.execute("SELECT COUNT(*) FROM recon_batch").fetchone()[0],
            "last_seq": con.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM recon_batch").fetchone()[0],
        }
        # 汇总与成员逐组核对: 每组成员数/两侧合计/状态必须与明细表一致
        bad_groups = con.execute(
            "SELECT COUNT(*) FROM recon_group g WHERE NOT EXISTS ("
            "  SELECT 1 FROM recon_member m WHERE m.group_id = g.group_id)"
        ).fetchone()[0]
        checks["empty_groups"] = bad_groups
        recomputed = con.execute("""
            SELECT m.group_id,
                   COUNT(*) FILTER (WHERE n.node_kind = 'RECEIPT') AS rc,
                   COUNT(*) FILTER (WHERE n.node_kind = 'DETAIL') AS dc,
                   COALESCE(SUM(r.amount_units), 0) AS rt,
                   COALESCE(SUM(d.amount_units), 0) AS dt
            FROM recon_member m
            JOIN recon_node n ON n.node_id = m.node_id
            LEFT JOIN recon_receipt r
                   ON r.node_id = n.node_id AND n.node_kind = 'RECEIPT'
            LEFT JOIN recon_ledger_detail d
                   ON d.txn_row_id = n.txn_row_id AND d.cost_center = n.cost_center
                  AND n.node_kind = 'DETAIL'
            GROUP BY m.group_id
        """).fetchall()
        summary = {gid: (status, rt, dt_, diff, rc, dc) for
                   gid, status, rt, dt_, diff, rc, dc in con.execute(
                       "SELECT group_id, status, receipt_total, detail_total, "
                       "difference, receipt_count, detail_count FROM recon_group"
                   ).fetchall()}
        mismatched = 0
        for gid, rc, dc, rt, dt_ in recomputed:
            row = summary.get(int(gid))
            if row is None or row[4] != rc or row[5] != dc or row[1] != rt or row[2] != dt_:
                mismatched += 1
        checks["summary_mismatched_groups"] = mismatched
    finally:
        con.close()
    return {k: int(v) for k, v in checks.items()}
