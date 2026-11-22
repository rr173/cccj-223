"""离线事实镜像: 总部导出双版本差量包, 站点凭包与本地 DuckDB 镜像切版。

适用场景
--------
月结事实发布在总部库; 部分站点不能连总部库, 只能定期收到一个文件。站点镜像
以 ``txn_row_id`` 为键保存已发布 ``facts`` 的一个**投影** (列名与数据类型沿用
``facts``), 站点只用一份差量包 + 本地镜像就能从起点版本切换到目标版本; 首次
收数以空镜像 (``EMPTY``) 为起点, 走同一套流程。

投影列 (与 ``facts`` 同名同类型)
--------------------------------
``txn_row_id`` (VARCHAR, 键)、``event_time`` (TIMESTAMP)、``customer_key``
(VARCHAR)、``contract_key`` (VARCHAR)、``amount`` (DECIMAL(28,6))、
``customer_generation_id`` / ``contract_generation_id`` (VARCHAR)、
``customer_valid_from`` / ``contract_valid_from`` (TIMESTAMP NOT NULL)、
``customer_valid_to`` / ``contract_valid_to`` (TIMESTAMP, 开放区间末端为 SQL NULL)。
批次 id、请求 id、源行号等**不在投影里**: 同一 ``txn_row_id`` 只按投影判断
未变/替换, 不因批号、请求 id 或源行号变化就进入差量包。

差量语义
--------
* 导出显式指定起点版本与**不同的**目标版本, 可跨过中间发布, 绝不从当前版本补数。
* 每个有差异的键只发一笔变化: ``upsert`` (新增或整行替换) / ``delete``
  (删除必须带明确的键); 投影完全相同的键不发正文; 两版投影完全一致时,
  包可以没有任何行变更, 仍可用于切换版本。
* 只导已发布 ``facts``, 不读隔离清单, 不重跑装配。

内容指纹 (逻辑内容核对, 与文件行序、库文件布局无关)
----------------------------------------------------
分区: ``partition_id = sha256(txn_row_id 的 UTF-8 字节).digest()[0]``, 共 256 个
逻辑分区 (0..255)。每个分区的分区摘要:

1. 把分区内每一行按投影列固定顺序序列化为一条 JSON 数组行
   ``[txn_row_id, event_time, customer_key, contract_key, amount,
     customer_generation_id, contract_generation_id,
     customer_valid_from, customer_valid_to,
     contract_valid_from, contract_valid_to]``;
   时间统一为 ``YYYY-MM-DD HH:MM:SS.ffffff`` 文本 (NULL 为 JSON null),
   金额统一为定点十进制文本且恰带 6 位小数 (如 ``"0.000001"``、``"-10.000000"``),
   字符串按 JSON 规则转义;
2. 分区摘要 = ``sha256( 各 JSON 行按字典序升序排列后, 逐行追加行字节 + b"\\n" )``
   (空分区 = 对空输入求 sha256)。

镜像整体内容指纹 = ``sha256( 256 个分区按分区号升序, 逐个追加
分区号的 2 字节大端表示 + 该分区摘要的 32 字节 )``。空镜像指纹由此自然定义,
不依赖任何库文件。站点核对只比较指纹值, 与物理存储和行序无关。

两道内容核对
------------
* 落地**前**: 镜像当前版本 ID 必须等于包声明的起点版本, 且镜像当前内容指纹
  必须等于包内的**起点指纹目录** (按摘要目录整体核对, 不扫事实行)。
* 落地**后、切版前**: 只重读本次受影响分区的事实并重算其分区摘要, 与包内由
  导出端**根据目标事实**算出的目标分区摘要逐区核对; 再用 256 个分区摘要
  (受影响区取重算值, 其余取镜像摘要目录中的既有值) 合成整体指纹, 与包声明的
  目标整体指纹核对。期望值全部来自导出端事实, 绝不从包里的操作反推。
  因此「漏掉一条删除但保留原目标指纹」的包必然在落地后核对中被拒绝。

分区局部性 (可核查)
-------------------
受影响分区由**键的哈希**判定 (删除同样计入), 包内的分区声明不能替代该判定。
正常更新只读固定大小 (256 行) 的摘要目录、只重读受影响分区的事实、只重写受
影响分区; 不重扫未受影响分区的事实、不逐行校验全部记录、不重写未受影响分区。
实际触及的分区与每区读/写/删行数记录在镜像的 ``mirror_apply_partition_touch`` /
``mirror_apply_log`` 中, 测试据此断言, 而不是看耗时或只打印预期范围。

事务与失败语义
--------------
镜像事实、摘要目录、当前版本/指纹状态、应用证据在**单个事务**内一起落定。
起点不符 (含对已前进镜像重复投递旧包)、无效包、目标核对失败时事务回滚: 原镜像
仍可查且版本不前进。首次收数在同目录临时库上完成全部核对, 通过后才 ``os.link``
原子就位; 首次失败不留下任何可冒充已同步的镜像文件。

本功能只校验传输与应用的一致性, 不做签名认证; 站点经本功能建镜像/更新, 平时
只读, 不负责检测有人绕开工具直接改库。
"""
from __future__ import annotations

import dataclasses
import datetime as _dt
import hashlib
import json
import os
import pathlib
from decimal import Decimal
from typing import Any, Iterable

import duckdb

from .db import connect_persistent
from .errors import (
    MirrorBaseMismatchError,
    MirrorInvalidBaseError,
    MirrorPackageInvalidError,
    MirrorStateInvalidError,
    MirrorTargetCheckError,
    MirrorVersionNotFoundError,
)
from .util import parse_optional_timestamp, parse_timestamp

#: 差量包/镜像 schema 版本
MIRROR_SCHEMA_VERSION = 1
#: 逻辑分区数: txn_row_id 的 UTF-8 SHA-256 首字节取值空间
PARTITION_COUNT = 256
#: 空镜像起点的显式哨兵 (不是一个真实 version_id)
EMPTY_BASE = "EMPTY"
#: 投影列: 键 + 保留列, 名称与 facts 一致
KEY_COLUMN = "txn_row_id"
PROJECTION_COLUMNS: tuple[str, ...] = (
    "txn_row_id",
    "event_time",
    "customer_key",
    "contract_key",
    "amount",
    "customer_generation_id",
    "contract_generation_id",
    "customer_valid_from",
    "customer_valid_to",
    "contract_valid_from",
    "contract_valid_to",
)
#: 非空 (NOT NULL) 的投影列; 其余可空列中两个 valid_to 允许 SQL NULL
NOT_NULL_COLUMNS: frozenset[str] = frozenset({
    "txn_row_id", "event_time", "customer_key", "contract_key", "amount",
    "customer_generation_id", "contract_generation_id",
    "customer_valid_from", "contract_valid_from",
})
_FACT_BATCH_SIZE = 1_000
_AMOUNT_QUANTUM = Decimal("0.000001")

# --------------------------------------------------------------------------- #
# 镜像库 schema
# --------------------------------------------------------------------------- #
_MIRROR_SCHEMA_SQL = """
CREATE TABLE mirror_state (
    singleton        INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    schema_version   INTEGER NOT NULL,
    current_version  VARCHAR,                  -- NULL 仅在理论上出现; 首次成功即非空
    content_fingerprint VARCHAR NOT NULL,      -- 当前镜像整体内容指纹
    updated_at       TIMESTAMP NOT NULL
);

-- 镜像事实: 列名/类型沿用 facts 的投影; txn_row_id 为键, 物理按分区聚簇
CREATE TABLE mirror_facts (
    txn_row_id              VARCHAR NOT NULL,
    partition_id            INTEGER NOT NULL,
    event_time              TIMESTAMP NOT NULL,
    customer_key            VARCHAR NOT NULL,
    contract_key            VARCHAR NOT NULL,
    amount                  DECIMAL(28, 6) NOT NULL,
    customer_generation_id  VARCHAR NOT NULL,
    contract_generation_id  VARCHAR NOT NULL,
    customer_valid_from     TIMESTAMP NOT NULL,
    customer_valid_to       TIMESTAMP,
    contract_valid_from     TIMESTAMP NOT NULL,
    contract_valid_to       TIMESTAMP,
    PRIMARY KEY (txn_row_id)
);

-- 固定大小的摘要目录: 恒为 256 行, 正常更新只改受影响分区
CREATE TABLE mirror_partition_catalog (
    partition_id   INTEGER NOT NULL PRIMARY KEY,
    row_count      BIGINT NOT NULL,
    partition_digest VARCHAR NOT NULL
);

-- 每次应用的证据 (随镜像事实同事务落定)
CREATE TABLE mirror_apply_log (
    apply_id        VARCHAR NOT NULL PRIMARY KEY,
    base_version    VARCHAR,
    target_version  VARCHAR NOT NULL,
    package_sha256  VARCHAR NOT NULL,
    touched_partition_count BIGINT NOT NULL,
    upsert_count    BIGINT NOT NULL,
    delete_count    BIGINT NOT NULL,
    applied_at      TIMESTAMP NOT NULL
);

-- 每个受影响分区一行: 实际读取/插入/更新/删除的事实行数
CREATE TABLE mirror_apply_partition_touch (
    apply_id      VARCHAR NOT NULL,
    partition_id  INTEGER NOT NULL,
    rows_read     BIGINT NOT NULL,
    rows_inserted BIGINT NOT NULL,
    rows_updated  BIGINT NOT NULL,
    rows_deleted  BIGINT NOT NULL,
    PRIMARY KEY (apply_id, partition_id)
);

-- 查询口径与 facts 投影完全一致 (不含内部 partition_id)
CREATE VIEW mirror_fact_projection AS
SELECT txn_row_id, event_time, customer_key, contract_key, amount,
       customer_generation_id, contract_generation_id,
       customer_valid_from, customer_valid_to,
       contract_valid_from, contract_valid_to
FROM mirror_facts;
"""


# --------------------------------------------------------------------------- #
# 指纹原语
# --------------------------------------------------------------------------- #
def partition_of(txn_row_id: str) -> int:
    """按行 ID 的 UTF-8 字节做 SHA-256, 取首字节 (0..255) 划分逻辑分区。"""
    if not isinstance(txn_row_id, str) or not txn_row_id:
        raise MirrorPackageInvalidError(
            "txn_row_id must be a non-empty string",
            details={"txn_row_id": txn_row_id},
        )
    return hashlib.sha256(txn_row_id.encode("utf-8")).digest()[0]


def _format_timestamp(value: Any) -> str | None:
    """时间规范化为微秒精度的 ``YYYY-MM-DD HH:MM:SS.ffffff`` 文本; NULL 透传。"""
    if value is None:
        return None
    return f"{value.year:04d}-{value.month:02d}-{value.day:02d} " \
           f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}"


def _format_amount(value: Decimal) -> str:
    """DECIMAL(28,6) 规范化为恰带 6 位小数的定点文本, 不用浮点/科学计数法。"""
    q = value.quantize(_AMOUNT_QUANTUM)
    if q != value:
        raise MirrorPackageInvalidError(
            f"amount {value} has finer precision than DECIMAL(28,6)",
            details={"amount": str(value)},
        )
    return format(q, "f")


def _row_values(row: dict[str, Any]) -> list[Any]:
    """按投影列固定顺序取出可 JSON 序列化的值 (时间转文本, 金额转定点文本)。"""
    return [
        row["txn_row_id"],
        _format_timestamp(row["event_time"]),
        row["customer_key"],
        row["contract_key"],
        _format_amount(row["amount"]),
        row["customer_generation_id"],
        row["contract_generation_id"],
        _format_timestamp(row["customer_valid_from"]),
        _format_timestamp(row["customer_valid_to"]),
        _format_timestamp(row["contract_valid_from"]),
        _format_timestamp(row["contract_valid_to"]),
    ]


def canonical_row_line(row: dict[str, Any]) -> str:
    """一行投影的规范化 JSON 数组文本 (指纹与差量包共用同一序列化口径)。"""
    return json.dumps(
        _row_values(row),
        sort_keys=False,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def digest_partition(lines: Iterable[str]) -> str:
    """分区摘要: 行文本按字典序排序后逐行 sha256 (空分区=空输入摘要)。"""
    digest = hashlib.sha256()
    for line in sorted(lines):
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def combine_partition_digests(partition_digests: list[str]) -> str:
    """256 个分区摘要合成镜像整体内容指纹 (按分区号升序, 与布局/行序无关)。"""
    if len(partition_digests) != PARTITION_COUNT:
        raise ValueError(f"expected {PARTITION_COUNT} partition digests, got {len(partition_digests)}")
    digest = hashlib.sha256()
    for partition_id, part_digest in enumerate(partition_digests):
        digest.update(partition_id.to_bytes(2, "big"))
        digest.update(bytes.fromhex(part_digest))
    return digest.hexdigest()


def empty_partition_digest() -> str:
    return digest_partition(())


def empty_content_fingerprint() -> str:
    return combine_partition_digests([empty_partition_digest()] * PARTITION_COUNT)


# --------------------------------------------------------------------------- #
# 结果对象
# --------------------------------------------------------------------------- #
@dataclasses.dataclass
class ExportResult:
    package_path: str
    base_version: str
    target_version: str
    change_count: int                 # 包内行变更数 (新增+替换+删除)
    upsert_count: int
    delete_count: int
    touched_partitions: list[int]
    base_fingerprint: str
    target_fingerprint: str
    package_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": "EXPORTED",
            "package": self.package_path,
            "base_version": self.base_version,
            "target_version": self.target_version,
            "change_count": self.change_count,
            "upsert_count": self.upsert_count,
            "delete_count": self.delete_count,
            "touched_partition_count": len(self.touched_partitions),
            "base_fingerprint": self.base_fingerprint,
            "target_fingerprint": self.target_fingerprint,
            "package_sha256": self.package_sha256,
        }


@dataclasses.dataclass
class ApplyResult:
    mirror_path: str
    base_version: str | None
    target_version: str
    bootstrap: bool
    touched_partitions: list[int]
    partition_touch: dict[int, dict[str, int]]
    upsert_count: int
    delete_count: int
    package_sha256: str
    content_fingerprint: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": "APPLIED",
            "mirror": self.mirror_path,
            "bootstrap": self.bootstrap,
            "base_version": self.base_version,
            "target_version": self.target_version,
            "touched_partitions": self.touched_partitions,
            "touched_partition_count": len(self.touched_partitions),
            "partition_touch": {str(pid): stats for pid, stats in sorted(self.partition_touch.items())},
            "upsert_count": self.upsert_count,
            "delete_count": self.delete_count,
            "package_sha256": self.package_sha256,
            "content_fingerprint": self.content_fingerprint,
        }


# --------------------------------------------------------------------------- #
# 导出端: 从两个已发布版本产出差量包
# --------------------------------------------------------------------------- #
def _require_published_version(con, version_id: str, role: str) -> None:
    row = con.execute(
        "SELECT 1 FROM fact_versions WHERE version_id = ?", [version_id]
    ).fetchone()
    if row is None:
        raise MirrorVersionNotFoundError(
            f"{role} version {version_id!r} is not a published version",
            details={"role": role, "version_id": version_id},
        )


def _iter_version_projection(con, version_id: str) -> Iterable[dict[str, Any]]:
    """键集分页流式读出某已发布版本 facts 的投影; 不读隔离、不重跑装配。"""
    columns = ", ".join(PROJECTION_COLUMNS)
    last_id: str | None = None
    while True:
        if last_id is None:
            rows = con.execute(
                f"SELECT {columns} FROM facts WHERE version_id = ? "
                f"ORDER BY txn_row_id LIMIT ?",
                [version_id, _FACT_BATCH_SIZE],
            ).fetchall()
        else:
            rows = con.execute(
                f"SELECT {columns} FROM facts WHERE version_id = ? AND txn_row_id > ? "
                f"ORDER BY txn_row_id LIMIT ?",
                [version_id, last_id, _FACT_BATCH_SIZE],
            ).fetchall()
        if not rows:
            return
        for values in rows:
            yield dict(zip(PROJECTION_COLUMNS, values))
        last_id = rows[-1][0]


def _index_version(con, version_id: str) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """把一个版本的投影按分区收进内存, 返回 (按键行映射, 256 个分区摘要)。

    只保存该版本自身的投影行 (差量比较所需), 不随历史版本累积。
    """
    by_key: dict[str, dict[str, Any]] = {}
    lines_by_partition: list[list[str]] = [[] for _ in range(PARTITION_COUNT)]
    for row in _iter_version_projection(con, version_id):
        key = row[KEY_COLUMN]
        if key in by_key:  # facts 的 (version_id, txn_row_id) 是主键, 防御
            raise RuntimeError(
                f"duplicate txn_row_id {key!r} in published version {version_id!r}"
            )
        by_key[key] = row
        lines_by_partition[partition_of(key)].append(canonical_row_line(row))
    return by_key, [digest_partition(lines) for lines in lines_by_partition]


def export_delta(
    db_path: str | os.PathLike[str],
    *,
    base_version: str,
    target_version: str,
    out_path: str | os.PathLike[str],
) -> ExportResult:
    """从总部事实库导出 ``base_version -> target_version`` 的差量包。

    ``base_version`` 可为 :data:`EMPTY_BASE` 表示以空镜像为起点 (首次收数);
    否则必须是两个**不同的**已发布版本, 可跨过中间发布, 绝不从当前版本补数。
    目标已存在时拒绝覆盖。
    """
    out = pathlib.Path(out_path)
    if out.exists():
        raise MirrorInvalidBaseError(
            f"delta package target already exists: {out}",
            details={"out": str(out)},
        )
    if not isinstance(target_version, str) or not target_version:
        raise MirrorInvalidBaseError("target_version must be a non-empty string")
    if target_version == EMPTY_BASE:
        raise MirrorInvalidBaseError("target version cannot be the EMPTY sentinel")
    if base_version != EMPTY_BASE and base_version == target_version:
        raise MirrorInvalidBaseError(
            "base and target must be distinct published versions",
            details={"base_version": base_version, "target_version": target_version},
        )

    con = connect_persistent(db_path, read_only=True)
    try:
        _require_published_version(con, target_version, "target")
        if base_version == EMPTY_BASE:
            base_by_key: dict[str, dict[str, Any]] = {}
            base_digests = [empty_partition_digest()] * PARTITION_COUNT
        else:
            _require_published_version(con, base_version, "base")
            base_by_key, base_digests = _index_version(con, base_version)
        target_by_key, target_digests = _index_version(con, target_version)

        changes: list[dict[str, Any]] = []
        upsert_count = 0
        delete_count = 0
        touched: set[int] = set()
        all_keys = set(base_by_key) | set(target_by_key)
        for key in sorted(all_keys):
            old = base_by_key.get(key)
            new = target_by_key.get(key)
            if old is not None and new is not None:
                if canonical_row_line(old) == canonical_row_line(new):
                    continue  # 投影未变: 批号/请求 id/源行号差异不产生正文
                op = "upsert"  # 整行替换
            elif new is not None:
                op = "upsert"  # 新增
            else:
                op = "delete"  # 删除必须有明确的键
            pid = partition_of(key)
            touched.add(pid)
            if op == "delete":
                changes.append({"op": "delete", "txn_row_id": key, "partition_id": pid})
                delete_count += 1
            else:
                changes.append({
                    "op": "upsert",
                    "txn_row_id": key,
                    "partition_id": pid,
                    "row": _row_values(new),
                })
                upsert_count += 1

        base_fingerprint = combine_partition_digests(base_digests)
        target_fingerprint = combine_partition_digests(target_digests)
        package = {
            "schema_version": MIRROR_SCHEMA_VERSION,
            "base_version": None if base_version == EMPTY_BASE else base_version,
            "base_is_empty": base_version == EMPTY_BASE,
            "target_version": target_version,
            "projection_columns": list(PROJECTION_COLUMNS),
            "base_partition_digests": base_digests,
            "target_partition_digests": target_digests,
            "base_content_fingerprint": base_fingerprint,
            "target_content_fingerprint": target_fingerprint,
            "declared_touched_partitions": sorted(touched),
            "changes": changes,
        }
    finally:
        con.close()

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.parent / f"{out.name}.tmp-{os.getpid()}"
    try:
        with tmp.open("w", encoding="utf-8", newline="\n") as fh:
            json.dump(package, fh, ensure_ascii=False, indent=2, sort_keys=False)
            fh.write("\n")
        try:
            os.link(tmp, out)
        except FileExistsError as exc:
            raise MirrorInvalidBaseError(
                f"delta package target appeared during delivery: {out}",
                details={"out": str(out)},
            ) from exc
    finally:
        tmp.unlink(missing_ok=True)

    package_sha256 = _file_sha256(out)
    return ExportResult(
        package_path=str(out),
        base_version=base_version,
        target_version=target_version,
        change_count=len(changes),
        upsert_count=upsert_count,
        delete_count=delete_count,
        touched_partitions=sorted(touched),
        base_fingerprint=base_fingerprint,
        target_fingerprint=target_fingerprint,
        package_sha256=package_sha256,
    )


# --------------------------------------------------------------------------- #
# 站点端: 差量包解析与校验
# --------------------------------------------------------------------------- #
def load_package(path: str | os.PathLike[str]) -> dict[str, Any]:
    """读入并结构性校验差量包; 任何结构/语义问题抛 :class:`MirrorPackageInvalidError`。"""
    p = pathlib.Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise MirrorPackageInvalidError(f"cannot read delta package {p}: {exc}",
                                        details={"package": str(p)}) from exc
    try:
        pkg = json.loads(text)
    except json.JSONDecodeError as exc:
        raise MirrorPackageInvalidError(f"delta package is not valid JSON: {exc}",
                                        details={"package": str(p)}) from exc
    if not isinstance(pkg, dict):
        raise MirrorPackageInvalidError("delta package root must be a JSON object")

    required = {
        "schema_version", "base_version", "base_is_empty", "target_version",
        "projection_columns", "base_partition_digests", "target_partition_digests",
        "base_content_fingerprint", "target_content_fingerprint",
        "declared_touched_partitions", "changes",
    }
    missing = required - set(pkg)
    if missing:
        raise MirrorPackageInvalidError(f"package missing keys: {sorted(missing)}",
                                        details={"missing": sorted(missing)})
    unknown = set(pkg) - required
    if unknown:
        raise MirrorPackageInvalidError(f"package has unknown keys: {sorted(unknown)}",
                                        details={"unknown": sorted(unknown)})
    if pkg["schema_version"] != MIRROR_SCHEMA_VERSION:
        raise MirrorPackageInvalidError(
            f"unsupported schema_version {pkg['schema_version']!r}, expected {MIRROR_SCHEMA_VERSION}"
        )
    if not isinstance(pkg["base_is_empty"], bool):
        raise MirrorPackageInvalidError("base_is_empty must be boolean")
    if pkg["base_is_empty"]:
        if pkg["base_version"] is not None:
            raise MirrorPackageInvalidError("base_is_empty=true requires base_version=null")
    else:
        if not isinstance(pkg["base_version"], str) or not pkg["base_version"]:
            raise MirrorPackageInvalidError("non-empty base requires a string base_version")
    if not isinstance(pkg["target_version"], str) or not pkg["target_version"]:
        raise MirrorPackageInvalidError("target_version must be a non-empty string")
    if not pkg["base_is_empty"] and pkg["base_version"] == pkg["target_version"]:
        raise MirrorPackageInvalidError("base and target versions must differ")
    if pkg["projection_columns"] != list(PROJECTION_COLUMNS):
        raise MirrorPackageInvalidError(
            "projection_columns do not match the supported mirror projection",
            details={"expected": list(PROJECTION_COLUMNS),
                     "actual": pkg["projection_columns"]},
        )
    for label in ("base_partition_digests", "target_partition_digests"):
        digests = pkg[label]
        if not isinstance(digests, list) or len(digests) != PARTITION_COUNT \
                or not all(isinstance(d, str) and _is_sha256_hex(d) for d in digests):
            raise MirrorPackageInvalidError(f"{label} must be {PARTITION_COUNT} sha256 hex strings")
    for label in ("base_content_fingerprint", "target_content_fingerprint"):
        if not isinstance(pkg[label], str) or not _is_sha256_hex(pkg[label]):
            raise MirrorPackageInvalidError(f"{label} must be a sha256 hex string")
    # 声明的整体指纹必须与其分区目录自洽 (校验包本身, 不用于反推目标期望)
    if combine_partition_digests(pkg["base_partition_digests"]) != pkg["base_content_fingerprint"]:
        raise MirrorPackageInvalidError("base fingerprint does not match base partition digests")
    if combine_partition_digests(pkg["target_partition_digests"]) != pkg["target_content_fingerprint"]:
        raise MirrorPackageInvalidError("target fingerprint does not match target partition digests")

    declared = pkg["declared_touched_partitions"]
    if not isinstance(declared, list) or any(not isinstance(p, int) or isinstance(p, bool)
                                             or not (0 <= p < PARTITION_COUNT) for p in declared):
        raise MirrorPackageInvalidError("declared_touched_partitions must be partition ids 0..255")
    if len(set(declared)) != len(declared):
        raise MirrorPackageInvalidError("declared_touched_partitions contains duplicates")

    changes = pkg["changes"]
    if not isinstance(changes, list):
        raise MirrorPackageInvalidError("changes must be a list")
    seen_keys: set[str] = set()
    derived_touched: set[int] = set()
    normalized: list[dict[str, Any]] = []
    for idx, change in enumerate(changes):
        normalized.append(_validate_change(change, idx, seen_keys, derived_touched))
    # 分区声明不能替代从键判断: 二者必须完全一致 (删除也计入)
    if set(declared) != derived_touched:
        raise MirrorPackageInvalidError(
            "declared_touched_partitions does not match partitions derived from change keys",
            details={"declared": sorted(declared), "derived": sorted(derived_touched)},
        )
    pkg["_normalized_changes"] = normalized
    pkg["_derived_touched"] = derived_touched
    return pkg


def _is_sha256_hex(value: str) -> bool:
    return len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _validate_change(change: Any, idx: int, seen_keys: set[str],
                     derived_touched: set[int]) -> dict[str, Any]:
    if not isinstance(change, dict):
        raise MirrorPackageInvalidError(f"change #{idx} must be an object")
    op = change.get("op")
    if op not in ("upsert", "delete"):
        raise MirrorPackageInvalidError(f"change #{idx} has invalid op {op!r}")
    key = change.get("txn_row_id")
    if not isinstance(key, str) or not key:
        raise MirrorPackageInvalidError(f"change #{idx} must carry an explicit txn_row_id")
    if key in seen_keys:
        raise MirrorPackageInvalidError(f"change #{idx} repeats txn_row_id {key!r}: each key has at most one change")
    seen_keys.add(key)
    pid = partition_of(key)
    declared_pid = change.get("partition_id")
    if not isinstance(declared_pid, int) or isinstance(declared_pid, bool) or declared_pid != pid:
        raise MirrorPackageInvalidError(
            f"change #{idx} partition_id must be derived from the key, got {declared_pid!r}",
            details={"txn_row_id": key, "expected_partition": pid},
        )
    derived_touched.add(pid)

    if op == "delete":
        if set(change) != {"op", "txn_row_id", "partition_id"}:
            raise MirrorPackageInvalidError(f"delete change #{idx} must contain only op/txn_row_id/partition_id")
        return {"op": "delete", "txn_row_id": key, "partition_id": pid}

    if set(change) != {"op", "txn_row_id", "partition_id", "row"}:
        raise MirrorPackageInvalidError(f"upsert change #{idx} must contain exactly op/txn_row_id/partition_id/row")
    raw_row = change["row"]
    if not isinstance(raw_row, list) or len(raw_row) != len(PROJECTION_COLUMNS):
        raise MirrorPackageInvalidError(
            f"upsert change #{idx} row must be a {len(PROJECTION_COLUMNS)}-element array in projection order"
        )
    if raw_row[0] != key:
        raise MirrorPackageInvalidError(f"upsert change #{idx} row key differs from txn_row_id")
    row = _coerce_projection_row(raw_row, idx)
    return {"op": "upsert", "txn_row_id": key, "partition_id": pid, "row": row}


def _coerce_projection_row(values: list[Any], idx: int) -> dict[str, Any]:
    """把包内 JSON 值转回镜像行类型; 类型/空值/金额精度不符即无效包。"""
    row = dict(zip(PROJECTION_COLUMNS, values))
    for col in NOT_NULL_COLUMNS:
        if row[col] is None:
            raise MirrorPackageInvalidError(f"change #{idx} column {col!r} must not be null")
    for col in ("customer_key", "contract_key", "customer_generation_id",
                "contract_generation_id", "txn_row_id"):
        if not isinstance(row[col], str) or not row[col]:
            raise MirrorPackageInvalidError(f"change #{idx} column {col!r} must be a non-empty string")
    try:
        row["event_time"] = parse_timestamp(row["event_time"])
        row["customer_valid_from"] = parse_timestamp(row["customer_valid_from"])
        row["contract_valid_from"] = parse_timestamp(row["contract_valid_from"])
        row["customer_valid_to"] = parse_optional_timestamp(row["customer_valid_to"])
        row["contract_valid_to"] = parse_optional_timestamp(row["contract_valid_to"])
    except ValueError as exc:
        raise MirrorPackageInvalidError(f"change #{idx} has an unparseable timestamp: {exc}") from exc
    amount = row["amount"]
    if isinstance(amount, bool) or not isinstance(amount, (str, int, float)):
        raise MirrorPackageInvalidError(f"change #{idx} amount must be a JSON number/string, got {amount!r}")
    try:
        dec = Decimal(str(amount))
    except Exception as exc:  # Decimal 自身异常族
        raise MirrorPackageInvalidError(f"change #{idx} amount unparseable: {amount!r}") from exc
    if not dec.is_finite():
        raise MirrorPackageInvalidError(f"change #{idx} amount must be finite: {amount!r}")
    quantized = dec.quantize(_AMOUNT_QUANTUM)
    if quantized != dec:
        raise MirrorPackageInvalidError(
            f"change #{idx} amount {amount!r} exceeds DECIMAL(28,6) precision"
        )
    row["amount"] = quantized
    return row


# --------------------------------------------------------------------------- #
# 站点端: 应用差量包
# --------------------------------------------------------------------------- #
def _connect_mirror(path: pathlib.Path):
    con = duckdb.connect(str(path))
    con.execute("PRAGMA memory_limit='256MB'")
    con.execute("PRAGMA threads=2")
    con.execute("PRAGMA enable_object_cache=false")
    return con


def _create_fresh_mirror(path: pathlib.Path):
    con = _connect_mirror(path)
    try:
        con.execute(_MIRROR_SCHEMA_SQL)
        con.executemany(
            "INSERT INTO mirror_partition_catalog VALUES (?, ?, ?)",
            [(pid, 0, empty_partition_digest()) for pid in range(PARTITION_COUNT)],
        )
        con.execute(
            "INSERT INTO mirror_state VALUES (1, ?, ?, ?, ?)",
            [MIRROR_SCHEMA_VERSION, None, empty_content_fingerprint(),
             _utcnow()],
        )
    finally:
        con.close()


def _open_mirror(path: pathlib.Path, *, read_only: bool = False):
    if not path.exists():
        raise MirrorStateInvalidError(
            f"mirror database not found: {path} (bootstrap first with an EMPTY-base package)",
            details={"mirror": str(path)},
        )
    con = duckdb.connect(str(path), read_only=read_only)
    try:
        ok = con.execute(
            "SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = 'mirror_state'"
        ).fetchone()[0]
        if not ok:
            raise MirrorStateInvalidError(
                f"{path} is not a fact mirror (missing mirror_state)",
                details={"mirror": str(path)},
            )
        row = con.execute(
            "SELECT schema_version, current_version, content_fingerprint FROM mirror_state"
        ).fetchone()
        if row is None or row[0] != MIRROR_SCHEMA_VERSION:
            raise MirrorStateInvalidError(f"{path} has an unsupported mirror schema version")
    except BaseException:
        con.close()
        raise
    return con


def _utcnow():
    return _dt.datetime.utcnow()


def _read_catalog(con) -> list[tuple[int, int, str]]:
    """只读固定大小的摘要目录 (256 行), 不触事实表。"""
    return con.execute(
        "SELECT partition_id, row_count, partition_digest FROM mirror_partition_catalog "
        "ORDER BY partition_id"
    ).fetchall()


def apply_delta(
    mirror_path: str | os.PathLike[str],
    package_path: str | os.PathLike[str],
) -> ApplyResult:
    """把差量包应用到本地镜像并切换到目标版本。

    首次 (镜像文件不存在) 只接受 ``base_is_empty`` 的包, 在临时库上完成全部
    落地前后核对后原子就位; 任何失败都不留下镜像文件。
    后续更新在单事务内完成, 失败回滚, 原镜像仍可查且版本不前进。
    """
    mirror = pathlib.Path(mirror_path)
    pkg = load_package(package_path)
    package_sha256 = _file_sha256(pathlib.Path(package_path))
    changes = pkg["_normalized_changes"]
    touched = sorted(pkg["_derived_touched"])
    bootstrap = not mirror.exists()

    if bootstrap:
        return _bootstrap_mirror(mirror, pkg, changes, touched, package_sha256)
    return _update_mirror(mirror, pkg, changes, touched, package_sha256)


def _bootstrap_mirror(mirror: pathlib.Path, pkg, changes, touched, package_sha256) -> ApplyResult:
    if not pkg["base_is_empty"]:
        raise MirrorBaseMismatchError(
            f"mirror {mirror} does not exist; only an EMPTY-base package can bootstrap it",
            details={"package_base": pkg["base_version"]},
        )
    mirror.parent.mkdir(parents=True, exist_ok=True)
    tmp = mirror.parent / f"{mirror.name}.tmp-{os.getpid()}"
    for suffix in ("", ".wal"):
        pathlib.Path(str(tmp) + suffix).unlink(missing_ok=True)
    try:
        _create_fresh_mirror(tmp)
        con = _connect_mirror(tmp)
        try:
            con.execute("BEGIN TRANSACTION")
            try:
                # 落地前核对: 空镜像版本为 None 且整体指纹等于空内容指纹 (只查摘要目录)
                _check_base(con, pkg, expect_empty=True)
                catalog_rows = _read_catalog(con)
                catalog = {pid: (row_count, digest) for pid, row_count, digest in catalog_rows}
                partition_touch, upsert_count, delete_count = _apply_changes(con, changes, touched)
                new_digests = _post_check(con, pkg, touched, catalog)
                _commit_switch(
                    con, pkg, touched, partition_touch, upsert_count, delete_count,
                    package_sha256, new_digests
                )
                con.execute("COMMIT")
            except BaseException:
                con.execute("ROLLBACK")
                raise
        finally:
            con.close()
        try:
            os.link(tmp, mirror)
        except FileExistsError as exc:
            raise MirrorStateInvalidError(
                f"mirror target appeared during bootstrap: {mirror}",
                details={"mirror": str(mirror)},
            ) from exc
    except BaseException:
        # 首次失败: 清掉临时库, 目标路径从未产生, 不能冒充已同步
        for suffix in ("", ".wal"):
            pathlib.Path(str(tmp) + suffix).unlink(missing_ok=True)
        raise
    finally:
        for suffix in ("", ".wal"):
            pathlib.Path(str(tmp) + suffix).unlink(missing_ok=True)

    return ApplyResult(
        mirror_path=str(mirror),
        base_version=None,
        target_version=pkg["target_version"],
        bootstrap=True,
        touched_partitions=touched,
        partition_touch=partition_touch,
        upsert_count=upsert_count,
        delete_count=delete_count,
        package_sha256=package_sha256,
        content_fingerprint=pkg["target_content_fingerprint"],
    )


def _update_mirror(mirror: pathlib.Path, pkg, changes, touched, package_sha256) -> ApplyResult:
    con = _open_mirror(mirror)
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            # 落地前核对: 版本 ID + 内容指纹 (只读 256 行摘要目录, 不扫事实)
            _check_base(con, pkg, expect_empty=False)
            catalog_rows = _read_catalog(con)
            catalog = {pid: (row_count, digest) for pid, row_count, digest in catalog_rows}
            partition_touch, upsert_count, delete_count = _apply_changes(con, changes, touched)
            new_digests = _post_check(con, pkg, touched, catalog)
            _commit_switch(
                con, pkg, touched, partition_touch, upsert_count, delete_count,
                package_sha256, new_digests
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()

    return ApplyResult(
        mirror_path=str(mirror),
        base_version=pkg["base_version"],
        target_version=pkg["target_version"],
        bootstrap=False,
        touched_partitions=touched,
        partition_touch=partition_touch,
        upsert_count=upsert_count,
        delete_count=delete_count,
        package_sha256=package_sha256,
        content_fingerprint=pkg["target_content_fingerprint"],
    )


def _check_base(con, pkg, *, expect_empty: bool) -> None:
    """落地前核对: 版本 ID 与内容指纹都必须与包针对的起点一致。"""
    state = con.execute(
        "SELECT current_version, content_fingerprint FROM mirror_state"
    ).fetchone()
    current_version, current_fingerprint = state
    if expect_empty:
        if current_version is not None:
            raise MirrorBaseMismatchError(
                "fresh mirror expected no current version",
                details={"current_version": current_version},
            )
    else:
        if current_version != pkg["base_version"]:
            raise MirrorBaseMismatchError(
                "mirror current version does not match the package base version "
                "(stale package re-delivery or wrong starting point)",
                details={"mirror_version": current_version,
                         "package_base": pkg["base_version"]},
            )
    if current_fingerprint != pkg["base_content_fingerprint"]:
        raise MirrorBaseMismatchError(
            "mirror content fingerprint does not match the package base fingerprint",
            details={"mirror_fingerprint": current_fingerprint,
                     "package_base_fingerprint": pkg["base_content_fingerprint"]},
        )
    # 摘要目录本身也必须能合成该指纹 (固定 256 行, 不触事实表)
    catalog = _read_catalog(con)
    combined = combine_partition_digests([digest for _, _, digest in catalog])
    if combined != current_fingerprint:
        raise MirrorBaseMismatchError(
            "mirror partition catalog is internally inconsistent with its content fingerprint"
        )


def _apply_changes(con, changes, touched) -> tuple[dict[int, dict[str, int]], int, int]:
    """只重读/重写受影响分区; 返回每区实际读/插/更/删行数与总计数。

    受影响分区完全由键哈希推导 (调用方已校验与包声明一致), 删除同样计入。
    未受影响分区的事实不被扫描, 其摘要目录行也不被更新。
    """
    stats: dict[int, dict[str, int]] = {
        pid: {"rows_read": 0, "rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}
        for pid in touched
    }
    upsert_count = 0
    delete_count = 0

    # 按受影响分区组织操作, 保证只在这些分区内定位事实
    by_partition: dict[int, list[dict[str, Any]]] = {pid: [] for pid in touched}
    for change in changes:
        by_partition[change["partition_id"]].append(change)

    for pid in touched:
        part_changes = by_partition[pid]
        keys = [c["txn_row_id"] for c in part_changes]
        # 只重读这一个受影响分区的现有事实 (按键定位, 不扫其他分区)
        existing = con.execute(
            "SELECT txn_row_id FROM mirror_facts WHERE partition_id = ? "
            "AND txn_row_id IN (" + ",".join("?" for _ in keys) + ")",
            [pid, *keys],
        ).fetchall()
        stats[pid]["rows_read"] += len(existing)
        existing_keys = {r[0] for r in existing}

        for change in part_changes:
            key = change["txn_row_id"]
            if change["op"] == "delete":
                if key not in existing_keys:
                    # 无效包: 起点镜像里没有要删除的键 (删除必须命中明确存在的键)
                    raise MirrorPackageInvalidError(
                        f"delete change for {key!r} has no matching row in the base mirror",
                        details={"txn_row_id": key, "partition_id": pid},
                    )
                con.execute(
                    "DELETE FROM mirror_facts WHERE partition_id = ? AND txn_row_id = ?",
                    [pid, key],
                )
                stats[pid]["rows_deleted"] += 1
                delete_count += 1
                continue

            row = change["row"]
            params = [
                row["event_time"], row["customer_key"], row["contract_key"], row["amount"],
                row["customer_generation_id"], row["contract_generation_id"],
                row["customer_valid_from"], row["customer_valid_to"],
                row["contract_valid_from"], row["contract_valid_to"],
                key, pid,
            ]
            if key in existing_keys:
                con.execute(
                    "UPDATE mirror_facts SET event_time=?, customer_key=?, contract_key=?, "
                    "amount=?, customer_generation_id=?, contract_generation_id=?, "
                    "customer_valid_from=?, customer_valid_to=?, contract_valid_from=?, "
                    "contract_valid_to=? WHERE txn_row_id=? AND partition_id=?",
                    params,
                )
                stats[pid]["rows_updated"] += 1
            else:
                con.execute(
                    "INSERT INTO mirror_facts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [key, pid, row["event_time"], row["customer_key"], row["contract_key"],
                     row["amount"], row["customer_generation_id"], row["contract_generation_id"],
                     row["customer_valid_from"], row["customer_valid_to"],
                     row["contract_valid_from"], row["contract_valid_to"]],
                )
                stats[pid]["rows_inserted"] += 1
            upsert_count += 1
    return stats, upsert_count, delete_count


def _post_check(con, pkg, touched, catalog) -> list[str]:
    """落地后、切版前的内容核对。

    只重读受影响分区的事实并重算分区摘要, 与导出端按目标事实算出的目标分区
    摘要逐区核对; 未受影响分区取摘要目录既有值。整体指纹由 256 个分区摘要
    合成, 与包的目标整体指纹核对。期望值来自导出端, 不从包内操作反推。
    """
    target_digests = list(pkg["target_partition_digests"])
    # 未受影响分区直接沿用摘要目录既有值; 受影响分区下面用重读的事实重算
    new_digests: list[str] = [catalog[pid][1] for pid in range(PARTITION_COUNT)]
    for pid in touched:
        rows = con.execute(
            "SELECT txn_row_id, event_time, customer_key, contract_key, amount, "
            "customer_generation_id, contract_generation_id, customer_valid_from, "
            "customer_valid_to, contract_valid_from, contract_valid_to "
            "FROM mirror_facts WHERE partition_id = ?",
            [pid],
        ).fetchall()
        lines = [canonical_row_line(dict(zip(PROJECTION_COLUMNS, values))) for values in rows]
        actual_digest = digest_partition(lines)
        if actual_digest != target_digests[pid]:
            raise MirrorTargetCheckError(
                f"post-apply check failed in partition {pid}: "
                "mirror digest does not match exporter-computed target digest "
                "(a missing/extra change such as a dropped delete cannot be marked synced)",
                details={"partition_id": pid,
                         "mirror_digest": actual_digest,
                         "target_digest": target_digests[pid]},
            )
        new_digests[pid] = actual_digest

    overall = combine_partition_digests(new_digests)
    if overall != pkg["target_content_fingerprint"]:
        raise MirrorTargetCheckError(
            "post-apply overall content fingerprint does not match the package target fingerprint",
            details={"mirror_fingerprint": overall,
                     "target_fingerprint": pkg["target_content_fingerprint"]},
        )
    return new_digests


def _commit_switch(con, pkg, touched, partition_touch, upsert_count, delete_count,
                   package_sha256, new_digests) -> str:
    """事实、摘要目录、当前版本/指纹与应用证据在同一事务内一起落定并切版。"""
    apply_id = "apply-" + hashlib.sha256(
        (pkg["target_version"] + "|" + package_sha256 + "|" + _utcnow().isoformat())
        .encode("utf-8")
    ).hexdigest()[:16]

    for pid in touched:
        row_count = con.execute(
            "SELECT COUNT(*) FROM mirror_facts WHERE partition_id = ?", [pid]
        ).fetchone()[0]
        con.execute(
            "UPDATE mirror_partition_catalog SET row_count=?, partition_digest=? WHERE partition_id=?",
            [row_count, new_digests[pid], pid],
        )
    con.execute("DELETE FROM mirror_state")
    con.execute(
        "INSERT INTO mirror_state VALUES (1, ?, ?, ?, ?)",
        [MIRROR_SCHEMA_VERSION, pkg["target_version"],
         pkg["target_content_fingerprint"], _utcnow()],
    )
    con.execute(
        "INSERT INTO mirror_apply_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [apply_id, pkg["base_version"], pkg["target_version"], package_sha256,
         len(touched), upsert_count, delete_count, _utcnow()],
    )
    touch_rows = [
        (apply_id, pid, stats["rows_read"], stats["rows_inserted"],
         stats["rows_updated"], stats["rows_deleted"])
        for pid, stats in partition_touch.items()
    ]
    if touch_rows:  # 零行变更包没有受影响分区, DuckDB 不接受空 executemany
        con.executemany(
            "INSERT INTO mirror_apply_partition_touch VALUES (?, ?, ?, ?, ?, ?)",
            touch_rows,
        )
    return apply_id


def _file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# --------------------------------------------------------------------------- #
# 只读查询辅助 (站点平时只读使用)
# --------------------------------------------------------------------------- #
def mirror_status(mirror_path: str | os.PathLike[str]) -> dict[str, Any]:
    """读取镜像当前版本/指纹与最近一次应用证据 (只读打开)。"""
    mirror = pathlib.Path(mirror_path)
    con = _open_mirror(mirror, read_only=True)
    try:
        version, fingerprint = con.execute(
            "SELECT current_version, content_fingerprint FROM mirror_state"
        ).fetchone()
        last = con.execute(
            "SELECT apply_id, base_version, target_version, touched_partition_count, "
            "upsert_count, delete_count, applied_at FROM mirror_apply_log "
            "ORDER BY applied_at DESC LIMIT 1"
        ).fetchone()
        return {
            "mirror": str(mirror),
            "current_version": version,
            "content_fingerprint": fingerprint,
            "last_apply": None if last is None else {
                "apply_id": last[0], "base_version": last[1], "target_version": last[2],
                "touched_partition_count": last[3], "upsert_count": last[4],
                "delete_count": last[5], "applied_at": last[6],
            },
        }
    finally:
        con.close()
