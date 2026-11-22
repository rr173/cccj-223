"""通用工具: 时间/金额解析、内容指纹与隔离原因码。

时间内部统一用 ``datetime`` (含微秒) 处理, 落库时写 TIMESTAMP。
区间一律左闭右开 ``[valid_from, valid_to)``; ``valid_to`` 为 None 表示开放区间。
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
from decimal import Decimal, InvalidOperation
from typing import Any

# DuckDB TIMESTAMP 支持的边界, 用开放区间哨兵 (不入库, 仅参与区间比较)。
INFINITY = _dt.datetime(9999, 12, 31, 23, 59, 59)
#: 开放区间 (valid_to IS NULL) 在证据里展示的字符串。
OPEN_INTERVAL = "infinity"


# --------------------------------------------------------------------------- #
# 隔离原因码 (稳定契约, 调用方按 code 过滤)
# --------------------------------------------------------------------------- #
class Reason:
    MISSING_EVENT_TIME = "MISSING_EVENT_TIME"          # 交易发生时间缺失
    BAD_AMOUNT_TYPE = "BAD_AMOUNT_TYPE"                # 金额无法转 DECIMAL
    DUPLICATE_SOURCE_ROW = "DUPLICATE_SOURCE_ROW"      # 交易原行身份重复
    CROSS_BATCH_MISMATCH = "CROSS_BATCH_MISMATCH"      # 行声明批次与请求批次不一致
    NO_CUSTOMER_GENERATION = "NO_CUSTOMER_GENERATION"  # 客户键无有效世代
    NO_CONTRACT_GENERATION = "NO_CONTRACT_GENERATION"  # 合同键无有效世代
    OVERLAPPING_GENERATION = "OVERLAPPING_GENERATION"  # 命中多个世代 (来源冲突)
    INVALID_SOURCE_RECORD = "INVALID_SOURCE_RECORD"    # 来源行结构性错误
    UNKNOWN_BUSINESS_KEY_FIELD = "UNKNOWN_BUSINESS_KEY_FIELD"


# --------------------------------------------------------------------------- #
# 时间
# --------------------------------------------------------------------------- #
def parse_timestamp(value: Any) -> _dt.datetime:
    """宽松解析时间戳; None/空串 -> None。

    支持 ISO 8601 (``2026-01-15``、``2026-01-15T10:00:00``、带 Z/偏移)。
    带时区的时间统一转 UTC 后去掉 tzinfo, 保证与 TIMESTAMP 列可比。
    """
    if value is None:
        raise ValueError("timestamp is None")
    if isinstance(value, _dt.datetime):
        ts = value
    elif isinstance(value, _dt.date):
        ts = _dt.datetime(value.year, value.month, value.day)
    else:
        text = str(value).strip()
        if not text:
            raise ValueError("empty timestamp")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            ts = _dt.datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"unparseable timestamp {value!r}") from exc
    if ts.tzinfo is not None:
        ts = ts.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return ts


def parse_optional_timestamp(value: Any) -> _dt.datetime | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return parse_timestamp(value)


def ts_to_str(ts: _dt.datetime | None) -> str | None:
    if ts is None:
        return None
    return ts.isoformat(sep=" ")


# --------------------------------------------------------------------------- #
# 金额
# --------------------------------------------------------------------------- #
def parse_amount(value: Any) -> Decimal:
    """把金额严格解析为 Decimal; 失败抛 ValueError (调用方转隔离行)。

    布尔值被显式拒绝 (bool 是 int 子类, 但 True/False 不是合法金额)。
    空串/None 拒绝; 科学计数法字符串接受; NaN/Infinity 拒绝。
    """
    if isinstance(value, bool):
        raise ValueError(f"boolean is not an amount: {value!r}")
    if value is None:
        raise ValueError("amount is None")
    if isinstance(value, Decimal):
        dec = value
    elif isinstance(value, int):
        dec = Decimal(value)
    elif isinstance(value, float):
        dec = Decimal(str(value))  # 避开二进制浮点表示
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("empty amount")
        try:
            dec = Decimal(text)
        except InvalidOperation as exc:
            raise ValueError(f"unparseable amount {value!r}") from exc
    else:
        raise ValueError(f"unsupported amount type {type(value).__name__}: {value!r}")
    if not dec.is_finite():
        raise ValueError(f"non-finite amount {value!r}")
    return dec


# --------------------------------------------------------------------------- #
# 指纹
# --------------------------------------------------------------------------- #
def canonical_json(obj: Any) -> str:
    """确定性 JSON: 键排序、默认 str 兜底, 用于内容指纹。"""
    return json.dumps(
        obj,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def content_fingerprint(rows: list[dict]) -> str:
    """对一批来源行计算规范化指纹 (与行顺序无关, 与内容有关)。"""
    digest = hashlib.sha256()
    for row in sorted(canonical_json(r) for r in rows):
        digest.update(row.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def short_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
