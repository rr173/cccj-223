"""来源文件加载与批次登记。

输入格式: CSV 或 JSONL (按扩展名识别)。加载时 *不做* 类型强转, 全部以原始
字符串/对象保留, 类型校验交给 validation 阶段, 这样金额类型错误才能带着
原行身份进入隔离清单, 而不是在读取期抛异常丢掉整批。

批次契约:
  * 同名批次 (batch_id, source_kind) 重送且内容指纹一致 -> 幂等, 直接复用;
  * 同名批次内容不同 -> BatchConflictError, 拒绝;
  * 装配请求必须显式给出三个批次 id, 绝不按文件名猜测拼接。
"""
from __future__ import annotations

import csv
import datetime as _dt
import json
import pathlib
from typing import Any, Iterable

from .db import connect_persistent
from .errors import BatchConflictError
from .util import canonical_json, content_fingerprint

CUSTOMER = "customer"
CONTRACT = "contract"
TRANSACTION = "transaction"
VALID_KINDS = (CUSTOMER, CONTRACT, TRANSACTION)


def load_source_rows(path: str | pathlib.Path) -> list[dict[str, Any]]:
    """把 CSV/JSONL 读成 dict 列表 (值保留原始字符串)。

    每行会注入私有键 ``__source_row_no`` (从 1 开始), 作为可定位行身份。
    """
    p = pathlib.Path(path)
    suffix = p.suffix.lower()
    if suffix == ".jsonl":
        rows = _load_jsonl(p)
    elif suffix == ".csv":
        rows = _load_csv(p)
    else:
        raise ValueError(f"unsupported source format {suffix!r}, expected .csv or .jsonl")
    for idx, row in enumerate(rows, start=1):
        row["__source_row_no"] = idx
    return rows


def _load_csv(path: pathlib.Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"csv {path} has no header")
        return [dict(r) for r in reader]


def _load_jsonl(path: pathlib.Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_no}: {exc.msg}") from exc
            if not isinstance(obj, dict):
                raise ValueError(f"JSONL row {line_no} in {path} is not an object")
            rows.append(obj)
    return rows


def register_batch(
    db_path: str | pathlib.Path,
    *,
    batch_id: str,
    source_kind: str,
    path: str | pathlib.Path,
) -> dict[str, Any]:
    """登记一个来源批次, 返回注册信息。

    幂等: 同名同内容重复登记返回既有记录 (``reused=True``), 不重复写入。
    冲突: 同名不同内容抛 :class:`BatchConflictError`。
    """
    if source_kind not in VALID_KINDS:
        raise ValueError(f"source_kind must be one of {VALID_KINDS}, got {source_kind!r}")
    if not batch_id or not isinstance(batch_id, str):
        raise ValueError("batch_id must be a non-empty string")

    rows = load_source_rows(path)
    fp = content_fingerprint(rows)
    con = connect_persistent(db_path)
    try:
        existing = con.execute(
            "SELECT file_path, row_count, content_hash FROM batch_registry "
            "WHERE batch_id = ? AND source_kind = ?",
            [batch_id, source_kind],
        ).fetchone()
        if existing is not None:
            if existing[2] != fp:
                raise BatchConflictError(
                    f"batch {batch_id!r} ({source_kind}) already registered with different content",
                    details={
                        "batch_id": batch_id,
                        "source_kind": source_kind,
                        "registered_hash": existing[2],
                        "incoming_hash": fp,
                        "registered_file": existing[0],
                    },
                )
            return {
                "batch_id": batch_id,
                "source_kind": source_kind,
                "content_hash": fp,
                "row_count": existing[1],
                "reused": True,
            }
        con.execute(
            "INSERT INTO batch_registry VALUES (?, ?, ?, ?, ?, ?)",
            [batch_id, source_kind, str(path), len(rows), fp, _dt.datetime.utcnow()],
        )
        return {
            "batch_id": batch_id,
            "source_kind": source_kind,
            "content_hash": fp,
            "row_count": len(rows),
            "reused": False,
        }
    finally:
        con.close()


def inspect_batch(db_path: str | pathlib.Path, batch_id: str, source_kind: str) -> dict[str, Any]:
    con = connect_persistent(db_path, read_only=True)
    try:
        row = con.execute(
            "SELECT batch_id, source_kind, file_path, row_count, content_hash, registered_at "
            "FROM batch_registry WHERE batch_id = ? AND source_kind = ?",
            [batch_id, source_kind],
        ).fetchone()
        if row is None:
            return {}
        cols = [d[0] for d in con.description]
        return dict(zip(cols, row))
    finally:
        con.close()


def list_batches(db_path: str | pathlib.Path) -> list[dict[str, Any]]:
    con = connect_persistent(db_path, read_only=True)
    try:
        cols = [d[0] for d in con.execute("SELECT * FROM batch_registry LIMIT 0").description]
        return [dict(zip(cols, r)) for r in con.execute(
            "SELECT * FROM batch_registry ORDER BY source_kind, batch_id"
        ).fetchall()]
    finally:
        con.close()


def require_batches(
    con,
    *,
    customer_batch: str,
    contract_batch: str,
    transaction_batch: str,
) -> list[dict[str, Any]]:
    """装配前确认请求显式指定的三个批次都已登记, 返回三行登记信息。"""
    wanted = [
        (CUSTOMER, customer_batch),
        (CONTRACT, contract_batch),
        (TRANSACTION, transaction_batch),
    ]
    found: list[dict[str, Any]] = []
    for kind, bid in wanted:
        row = con.execute(
            "SELECT batch_id, source_kind, content_hash, row_count "
            "FROM batch_registry WHERE batch_id = ? AND source_kind = ?",
            [bid, kind],
        ).fetchone()
        if row is None:
            from .errors import BatchNotFoundError

            raise BatchNotFoundError(
                f"{kind} batch {bid!r} is not registered; "
                "assembly requires explicit, pre-registered batch ids"
            )
        found.append(dict(zip(["batch_id", "source_kind", "content_hash", "row_count"], row)))
    return found


def rows_canonical_payload(rows: Iterable[dict[str, Any]]) -> list[str]:
    return [canonical_json(r) for r in rows]
