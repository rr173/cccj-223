"""离线镜像的独立预言机: 不 import 任何 factasm.mirror 代码。

只用标准库按题目口径重算:
  * 行 ID 的 SHA-256 首字节分区;
  * 单行规范化 JSON、分区摘要、256 区合成的整体内容指纹;
  * 直接查询总部库指定版本的 facts 投影, 独立得到期望镜像内容与指纹。
测试拿它与镜像库实际内容/包内指纹对账, 而不是用差量实现自己生成预期。
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal

PROJECTION_COLUMNS = (
    "txn_row_id", "event_time", "customer_key", "contract_key", "amount",
    "customer_generation_id", "contract_generation_id",
    "customer_valid_from", "customer_valid_to",
    "contract_valid_from", "contract_valid_to",
)
_QUANTUM = Decimal("0.000001")


def oracle_partition(txn_row_id: str) -> int:
    return hashlib.sha256(txn_row_id.encode("utf-8")).digest()[0]


def _ts(value) -> str | None:
    if value is None:
        return None
    return f"{value.year:04d}-{value.month:02d}-{value.day:02d} " \
           f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{value.microsecond:06d}"


def _amount(value) -> str:
    return format(Decimal(str(value)).quantize(_QUANTUM), "f")


def oracle_row_line(row: dict) -> str:
    values = [
        row["txn_row_id"], _ts(row["event_time"]), row["customer_key"],
        row["contract_key"], _amount(row["amount"]),
        row["customer_generation_id"], row["contract_generation_id"],
        _ts(row["customer_valid_from"]), _ts(row["customer_valid_to"]),
        _ts(row["contract_valid_from"]), _ts(row["contract_valid_to"]),
    ]
    return json.dumps(values, sort_keys=False, ensure_ascii=False, separators=(",", ":"))


def oracle_partition_digest(lines) -> str:
    h = hashlib.sha256()
    for line in sorted(lines):
        h.update(line.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def oracle_fingerprint(partition_digests) -> str:
    h = hashlib.sha256()
    for pid, digest in enumerate(partition_digests):
        h.update(pid.to_bytes(2, "big"))
        h.update(bytes.fromhex(digest))
    return h.hexdigest()


def oracle_empty_digest() -> str:
    return hashlib.sha256(b"").hexdigest()


def oracle_projection_from_warehouse(con, version_id: str) -> dict[str, dict]:
    """直接查询总部库指定目标版 facts 的投影列 (预期镜像内容的唯一来源)。"""
    cols = ", ".join(PROJECTION_COLUMNS)
    cur = con.execute(
        f"SELECT {cols} FROM facts WHERE version_id = ? ORDER BY txn_row_id",
        [version_id],
    )
    return {r[0]: dict(zip(PROJECTION_COLUMNS, r)) for r in cur.fetchall()}


def oracle_fingerprint_of_rows(rows_by_key: dict[str, dict]) -> tuple[str, list[str], dict[int, int]]:
    """对一组投影行独立计算整体指纹、256 个分区摘要与每区行数。"""
    buckets: list[list[str]] = [[] for _ in range(256)]
    counts = {pid: 0 for pid in range(256)}
    for key, row in rows_by_key.items():
        pid = oracle_partition(key)
        buckets[pid].append(oracle_row_line(row))
        counts[pid] += 1
    digests = [oracle_partition_digest(b) for b in buckets]
    return oracle_fingerprint(digests), digests, counts
