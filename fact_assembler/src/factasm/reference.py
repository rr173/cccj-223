"""独立参考实现: 纯 Python 的区间枚举时态关联。

本模块刻意 *不使用* DuckDB, 与 :mod:`factasm.engine` 的关系计算走两条完全
独立的代码路径, 然后由 assembler 交叉核对; 两边结果不一致即拒绝发布
(ReferenceCheckError), 防止单条 SQL 静默选错世代或丢失匹配证据。

算法直白地枚举每个业务键的全部世代, 逐一判断左闭右开包含关系:
    候选数 == 1 -> 该世代唯一解释, 产出事实;
    候选数 == 0 -> NO_*_GENERATION 隔离;
    候选数 >  1 -> OVERLAPPING_GENERATION 隔离 (绝不回退到最新世代)。
"""
from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from typing import Any

from .util import INFINITY, OPEN_INTERVAL, Reason, canonical_json
from .validation import Generation, GenerationIndex, ParsedTransaction


def _candidate_view(gen: Generation, event_time: _dt.datetime, *, selected: bool,
                    exclude_reason: str | None = None) -> dict[str, Any]:
    end = gen.valid_to if gen.valid_to is not None else INFINITY
    return {
        "generation_id": gen.generation_id,
        "revision_no": gen.revision_no,
        "valid_from": gen.valid_from.isoformat(sep=" "),
        "valid_to": gen.valid_to.isoformat(sep=" ") if gen.valid_to is not None else OPEN_INTERVAL,
        "source_row_no": gen.source_row_no,
        "batch_id": gen.batch_id,
        "contains_event_time": event_time.isoformat(sep=" "),
        "comparison": {
            "rule": "half_open [valid_from, valid_to)",
            "ge_valid_from": event_time >= gen.valid_from,
            "lt_valid_to": event_time < end,
        },
        "selected": selected,
        "exclude_reason": exclude_reason,
    }


def correlate(
    transactions: list[ParsedTransaction],
    customer_index: GenerationIndex,
    contract_index: GenerationIndex,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """纯 Python 区间枚举。

    返回 ``(facts, quarantine, evidence)``:
      * facts  : 可发布事实行 (含证据/来源摘要);
      * quarantine: 时态匹配阶段产生的隔离行;
      * evidence: 每条交易一条的完整候选枚举 (含被排除原因)。
    """
    facts: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []

    for txn in transactions:
        assert txn.event_time is not None and txn.amount is not None  # 解析期已隔离
        cust_candidates = customer_index.candidates_at(txn.customer_key, txn.event_time)
        cont_candidates = contract_index.candidates_at(txn.contract_key, txn.event_time)

        record: dict[str, Any] = {
            "txn_row_id": txn.txn_row_id,
            "source_row_no": txn.source_row_no,
            "event_time": txn.event_time.isoformat(sep=" "),
            "customer_key": txn.customer_key,
            "contract_key": txn.contract_key,
            "amount": str(txn.amount),
            "customer": _dimension_evidence(txn.customer_key, cust_candidates, txn.event_time, "customer"),
            "contract": _dimension_evidence(txn.contract_key, cont_candidates, txn.event_time, "contract"),
        }
        evidence.append(record)

        reasons: list[tuple[str, str, dict[str, Any]]] = []
        if not cust_candidates:
            reasons.append((
                Reason.NO_CUSTOMER_GENERATION,
                f"no customer generation for key {txn.customer_key!r} containing event_time "
                f"{txn.event_time.isoformat(sep=' ')}",
                record["customer"],
            ))
        elif len(cust_candidates) > 1:
            reasons.append((
                Reason.OVERLAPPING_GENERATION,
                f"{len(cust_candidates)} overlapping customer generations for key "
                f"{txn.customer_key!r} at {txn.event_time.isoformat(sep=' ')}",
                record["customer"],
            ))
        if not cont_candidates:
            reasons.append((
                Reason.NO_CONTRACT_GENERATION,
                f"no contract generation for key {txn.contract_key!r} containing event_time "
                f"{txn.event_time.isoformat(sep=' ')}",
                record["contract"],
            ))
        elif len(cont_candidates) > 1:
            reasons.append((
                Reason.OVERLAPPING_GENERATION,
                f"{len(cont_candidates)} overlapping contract generations for key "
                f"{txn.contract_key!r} at {txn.event_time.isoformat(sep=' ')}",
                record["contract"],
            ))

        if reasons:
            for code, detail, dim_evidence in reasons:
                quarantine.append({
                    "transaction_batch_id": None,  # 由 assembler 补
                    "txn_row_id": txn.txn_row_id,
                    "source_row_no": txn.source_row_no,
                    "reason_code": code,
                    "reason_detail": detail,
                    "rejected_candidates": canonical_json(dim_evidence),
                    "raw_payload": canonical_json(txn.raw_payload),
                })
            continue

        cust = cust_candidates[0]
        cont = cont_candidates[0]
        facts.append({
            "txn_row_id": txn.txn_row_id,
            "source_row_no": txn.source_row_no,
            "event_time": txn.event_time,
            "customer_key": txn.customer_key,
            "contract_key": txn.contract_key,
            "amount": txn.amount,
            "customer_generation_id": cust.generation_id,
            "contract_generation_id": cont.generation_id,
            "customer_valid_from": cust.valid_from,
            "customer_valid_to": cust.valid_to,
            "contract_valid_from": cont.valid_from,
            "contract_valid_to": cont.valid_to,
            "match_evidence": canonical_json({
                "selection_rule": "exactly one half-open interval contains event_time",
                "customer": record["customer"],
                "contract": record["contract"],
            }),
        })
    return facts, quarantine, evidence


def _dimension_evidence(key: str, candidates: list[Generation], event_time: _dt.datetime,
                        dimension: str) -> dict[str, Any]:
    return {
        "dimension": dimension,
        "business_key": key,
        "candidate_count": len(candidates),
        "candidates_enumerated": [
            _candidate_view(g, event_time, selected=(i == 0 and len(candidates) == 1),
                            exclude_reason=None if (g.contains(event_time)) else "interval does not contain event_time")
            for i, g in enumerate(candidates)
        ],
    }


def build_source_summary(
    *,
    customer_batch: str,
    contract_batch: str,
    transaction_batch: str,
    batch_hashes: dict[tuple[str, str], str],
) -> str:
    """来源摘要: 显式批次组合 + 各批次内容指纹, 随事实落库可追溯。"""
    from .sources import CONTRACT, CUSTOMER, TRANSACTION

    return canonical_json({
        "customer_batch": customer_batch,
        "contract_batch": contract_batch,
        "transaction_batch": transaction_batch,
        "customer_batch_hash": batch_hashes.get((CUSTOMER, customer_batch)),
        "contract_batch_hash": batch_hashes.get((CONTRACT, contract_batch)),
        "transaction_batch_hash": batch_hashes.get((TRANSACTION, transaction_batch)),
    })
