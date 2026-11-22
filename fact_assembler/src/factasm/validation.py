"""来源校验与世代索引。

批次级硬错误 (整批拒绝, 不发布任何事实):
  * 维度行缺列/时间不可解析/区间逆序/修订号缺失或重复;
  * 行内 ``source_batch`` 与登记批次不一致 (来源自身冲突);
  * 同一业务键的两个世代区间在左闭右开语义下重叠 (边界相等不算重叠);
  * 同一业务键出现多个开放 (valid_to 为空) 世代。

行级隔离 (进入 quarantine, 不静默落到最新世代):
  * 交易发生时间缺失/不可解析; 金额类型错误; 原行身份缺失或重复;
  * 交易行声明的客户/合同批次与装配请求批次不一致;
  * 时态关联时 0 个或 >1 个候选世代。
"""
from __future__ import annotations

import dataclasses
import datetime as _dt
from decimal import Decimal
from typing import Any

from .util import INFINITY, OPEN_INTERVAL, Reason, canonical_json, parse_amount, parse_optional_timestamp, parse_timestamp


@dataclasses.dataclass(frozen=True)
class Generation:
    """一个客户/合同世代的完整身份与区间。"""

    generation_id: str
    batch_id: str
    business_key: str
    revision_no: int
    valid_from: _dt.datetime
    valid_to: _dt.datetime | None  # None = 开放区间
    attributes: dict[str, Any]
    source_row_no: int
    content_hash: str

    def contains(self, ts: _dt.datetime) -> bool:
        """左闭右开: valid_from <= ts < valid_to。"""
        if ts < self.valid_from:
            return False
        end = self.valid_to if self.valid_to is not None else INFINITY
        return ts < end

    def interval_text(self) -> str:
        to_text = self.valid_to.isoformat(sep=" ") if self.valid_to is not None else OPEN_INTERVAL
        return f"[{self.valid_from.isoformat(sep=' ')}, {to_text})"


@dataclasses.dataclass
class ParsedTransaction:
    txn_row_id: str
    source_row_no: int
    event_time: _dt.datetime | None
    customer_key: str | None
    contract_key: str | None
    amount: Decimal | None
    raw_payload: dict[str, Any]
    quarantine_reasons: list[tuple[str, str]]  # (reason_code, detail)


@dataclasses.dataclass
class ValidationOutcome:
    generations: dict[str, list[Generation]]  # business_key -> 按 valid_from 排序
    transactions: list[ParsedTransaction]
    quarantine: list[dict[str, Any]]         # 行级隔离 (尚未做时态匹配的部分)


class GenerationIndex:
    """内存世代索引: 只持有 *当前装配* 的批次数据, 不随历史批次增长。"""

    def __init__(self, generations: dict[str, list[Generation]]):
        self._by_key = generations

    def keys(self) -> list[str]:
        return sorted(self._by_key)

    def all_for(self, business_key: str) -> list[Generation]:
        return list(self._by_key.get(business_key, ()))

    def candidates_at(self, business_key: str, ts: _dt.datetime) -> list[Generation]:
        """枚举 ts 时刻命中的全部候选 (证据用; 校验通过后正常只返回 0 或 1 个)。"""
        return [g for g in self._by_key.get(business_key, ()) if g.contains(ts)]


# --------------------------------------------------------------------------- #
# 维度 (客户/合同) 解析与批次级校验
# --------------------------------------------------------------------------- #
_DIM_RESERVED = {"business_key", "valid_from", "valid_to", "source_batch", "revision_no", "__source_row_no"}


def parse_dimension_rows(
    rows: list[dict[str, Any]],
    *,
    kind: str,
    batch_id: str,
    batch_content_hash: str,
) -> dict[str, list[Generation]]:
    """解析客户/合同行并做批次级校验, 返回按业务键分组的世代列表。

    任何批次级冲突都抛 :class:`ValidationError`, 由装配器拒绝整批。
    """
    from .errors import ValidationError

    grouped: dict[str, list[Generation]] = {}
    seen_revisions: dict[tuple[str, int], int] = {}

    for row in rows:
        row_no = row["__source_row_no"]
        try:
            business_key = _require_str(row, "business_key", row_no, kind)
            revision_no = _require_int(row, "revision_no", row_no, kind)
            valid_from = parse_timestamp(row.get("valid_from"))
            valid_to = parse_optional_timestamp(row.get("valid_to"))
            declared_batch = _require_str(row, "source_batch", row_no, kind)
        except (ValueError, KeyError) as exc:
            raise ValidationError(
                f"{kind} row {row_no} is structurally invalid: {exc}",
                details={"source_kind": kind, "source_row_no": row_no},
            ) from exc

        if declared_batch != batch_id:
            raise ValidationError(
                f"{kind} row {row_no} declares source_batch={declared_batch!r} "
                f"but is registered under batch {batch_id!r}",
                details={
                    "source_kind": kind,
                    "source_row_no": row_no,
                    "declared": declared_batch,
                    "registered_batch": batch_id,
                },
            )
        if valid_to is not None and valid_from >= valid_to:
            raise ValidationError(
                f"{kind} row {row_no} has non half-open interval: "
                f"valid_from={valid_from} >= valid_to={valid_to}",
                details={"source_kind": kind, "source_row_no": row_no},
            )
        rev_key = (business_key, revision_no)
        if rev_key in seen_revisions:
            raise ValidationError(
                f"{kind} key {business_key!r} has duplicate revision_no={revision_no} "
                f"at rows {seen_revisions[rev_key]} and {row_no}",
                details={"source_kind": kind, "business_key": business_key,
                         "revision_no": revision_no, "rows": [seen_revisions[rev_key], row_no]},
            )
        seen_revisions[rev_key] = row_no

        attributes = {k: v for k, v in row.items() if k not in _DIM_RESERVED and not k.startswith("__")}
        generation_id = f"{kind}:{batch_id}:{business_key}:r{revision_no}"
        gen = Generation(
            generation_id=generation_id,
            batch_id=batch_id,
            business_key=business_key,
            revision_no=revision_no,
            valid_from=valid_from,
            valid_to=valid_to,
            attributes=attributes,
            source_row_no=row_no,
            content_hash=batch_content_hash,
        )
        grouped.setdefault(business_key, []).append(gen)

    # 同键区间重叠检查 (左闭右开; 边界相等允许) 与开放区间唯一性检查
    for business_key, gens in grouped.items():
        gens.sort(key=lambda g: (g.valid_from, g.revision_no))
        open_ends = [g for g in gens if g.valid_to is None]
        if len(open_ends) > 1:
            raise ValidationError(
                f"{kind} key {business_key!r} has {len(open_ends)} open-ended generations "
                f"(rows {[g.source_row_no for g in open_ends]})",
                details={"source_kind": kind, "business_key": business_key,
                         "rows": [g.source_row_no for g in open_ends]},
            )
        for prev, nxt in zip(gens, gens[1:]):
            prev_end = prev.valid_to if prev.valid_to is not None else INFINITY
            if nxt.valid_from < prev_end:
                raise ValidationError(
                    f"{kind} key {business_key!r} has overlapping generations at row "
                    f"{prev.source_row_no} {prev.interval_text()} and row "
                    f"{nxt.source_row_no} {nxt.interval_text()} "
                    "(half-open touching boundaries are allowed, real overlap is not)",
                    details={
                        "source_kind": kind,
                        "business_key": business_key,
                        "row_a": prev.source_row_no,
                        "row_b": nxt.source_row_no,
                        "interval_a": prev.interval_text(),
                        "interval_b": nxt.interval_text(),
                    },
                )
    return grouped


def _require_str(row: dict, field: str, row_no: int, kind: str) -> str:
    value = row.get(field)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"missing required field {field!r}")
    return str(value).strip()


def _require_int(row: dict, field: str, row_no: int, kind: str) -> int:
    value = row.get(field)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"missing required field {field!r}")
    if isinstance(value, bool):
        raise ValueError(f"boolean is not a valid {field}")
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"field {field!r}={value!r} is not an integer") from exc


# --------------------------------------------------------------------------- #
# 交易解析与行级隔离
# --------------------------------------------------------------------------- #
def parse_transaction_rows(
    rows: list[dict[str, Any]],
    *,
    batch_id: str,
    customer_batch_id: str,
    contract_batch_id: str,
) -> tuple[list[ParsedTransaction], list[dict[str, Any]]]:
    """解析交易行, 返回 (可进入时态匹配的交易, 隔离清单)。

    每行可同时携带多个隔离原因; 任意原因存在即不参与关联。
    重复 ``txn_row_id``: 首次出现保留, 后续行隔离 DUPLICATE_SOURCE_ROW。
    """
    seen_ids: dict[str, int] = {}
    parsed: list[ParsedTransaction] = []
    quarantine: list[dict[str, Any]] = []

    for row in rows:
        row_no = row["__source_row_no"]
        reasons: list[tuple[str, str]] = []
        payload = {k: v for k, v in row.items() if not k.startswith("__")}

        raw_id = row.get("txn_row_id")
        txn_row_id = None if raw_id is None or (isinstance(raw_id, str) and not raw_id.strip()) else str(raw_id).strip()
        if txn_row_id is None:
            reasons.append((Reason.INVALID_SOURCE_RECORD, "missing txn_row_id (source row identity)"))
        elif txn_row_id in seen_ids:
            reasons.append((
                Reason.DUPLICATE_SOURCE_ROW,
                f"txn_row_id {txn_row_id!r} first appeared at source row {seen_ids[txn_row_id]}",
            ))
        else:
            seen_ids[txn_row_id] = row_no

        # 交易文件自带 source_batch 时必须与登记批次一致 (来源自身冲突 -> 隔离该行)
        declared_self = row.get("source_batch")
        if declared_self is not None and str(declared_self).strip() and str(declared_self).strip() != batch_id:
            reasons.append((
                Reason.CROSS_BATCH_MISMATCH,
                f"row source_batch={declared_self!r} != registered transaction batch {batch_id!r}",
            ))
        # 显式声明的客户/合同批次必须与装配请求一致, 不允许跨来源隐式拼接
        for field, requested in (
            ("customer_batch_id", customer_batch_id),
            ("contract_batch_id", contract_batch_id),
        ):
            declared = row.get(field)
            if declared is not None and str(declared).strip() and str(declared).strip() != requested:
                reasons.append((
                    Reason.CROSS_BATCH_MISMATCH,
                    f"row {field}={declared!r} != requested batch {requested!r}",
                ))

        event_time = None
        try:
            event_time = parse_timestamp(row.get("event_time"))
        except ValueError:
            reasons.append((Reason.MISSING_EVENT_TIME,
                            f"event_time missing or unparseable: {row.get('event_time')!r}"))

        amount = None
        try:
            amount = parse_amount(row.get("amount"))
        except ValueError as exc:
            reasons.append((Reason.BAD_AMOUNT_TYPE, str(exc)))

        customer_key = _optional_str(row.get("customer_key"))
        contract_key = _optional_str(row.get("contract_key"))
        if customer_key is None:
            reasons.append((Reason.INVALID_SOURCE_RECORD, "missing customer_key"))
        if contract_key is None:
            reasons.append((Reason.INVALID_SOURCE_RECORD, "missing contract_key"))

        txn = ParsedTransaction(
            txn_row_id=txn_row_id or f"__row_{row_no}",
            source_row_no=row_no,
            event_time=event_time,
            customer_key=customer_key,
            contract_key=contract_key,
            amount=amount,
            raw_payload=payload,
            quarantine_reasons=reasons,
        )
        for code, detail in reasons:
            quarantine.append(_quarantine_row(batch_id, txn, code, detail, payload))
        # 携带任意隔离原因的行 (含重复原行身份) 都不进入时态关联,
        # 避免重复身份污染 staging 表, 也保证隔离行不会静默落到最新世代。
        if not reasons:
            parsed.append(txn)
    return parsed, quarantine


def _optional_str(value: Any) -> str | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return str(value).strip()


def _quarantine_row(batch_id: str, txn: ParsedTransaction, code: str, detail: str,
                    payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "transaction_batch_id": batch_id,
        "txn_row_id": txn.txn_row_id,
        "source_row_no": txn.source_row_no,
        "reason_code": code,
        "reason_detail": detail,
        "rejected_candidates": None,
        "raw_payload": canonical_json(payload),
    }
