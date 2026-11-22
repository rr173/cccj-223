"""完全独立的区间枚举参考核对器 (不 import factasm 任何模块)。

自己读原始 CSV、自己做半开区间枚举, 作为装配事实表的外部预言机:
  * 逐笔交易枚举客户/合同全部世代, 记录命中候选;
  * 候选恰为 1 -> 期望事实, 并给出期望世代 (业务键+修订号);
  * 候选 0 / >1 -> 期望隔离及原因;
  * 与装配器输出的事实表/隔离清单逐项比对。
"""
from __future__ import annotations

import csv
import datetime as dt
import pathlib


def _ts(text):
    text = (text or "").strip()
    if not text:
        return None
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00").replace("+00:00", ""))


def read_csv(path):
    with pathlib.Path(path).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def build_intervals(rows):
    """业务键 -> 世代列表 [(valid_from, valid_to_or_inf, revision_no, row_no)]。"""
    inf = dt.datetime.max
    grouped = {}
    for i, r in enumerate(rows, start=1):
        vf = _ts(r["valid_from"])
        vt = _ts(r["valid_to"]) or inf
        grouped.setdefault(r["business_key"], []).append(
            (vf, vt, int(r["revision_no"]), i)
        )
    for key in grouped:
        grouped[key].sort()
    return grouped


def enumerate_candidates(intervals, key, ts):
    """朴素枚举: 逐个世代判断 vf <= ts < vt, 返回全部命中。"""
    hits = []
    for vf, vt, rev, row_no in intervals.get(key, []):
        if vf <= ts < vt:
            hits.append((rev, row_no, vf, vt))
    return hits


def expected_outcomes(customers_csv, contracts_csv, transactions_csv):
    """返回 (期望事实 dict, 期望隔离 dict[txn_id] -> set(reason 关键词))。"""
    cust = build_intervals(read_csv(customers_csv))
    cont = build_intervals(read_csv(contracts_csv))
    txns = read_csv(transactions_csv)

    expected_facts = {}
    expected_quarantine = {}
    seen_ids = {}

    for i, t in enumerate(txns, start=1):
        tid = t["txn_row_id"]
        reasons = set()

        if tid in seen_ids:
            reasons.add("DUPLICATE_SOURCE_ROW")
        else:
            seen_ids[tid] = i

        et = _ts(t["event_time"])
        if et is None:
            reasons.add("MISSING_EVENT_TIME")

        amount = (t.get("amount") or "").strip()
        try:
            float(amount)
            assert amount not in ("", "nan", "inf", "-inf", "infinity")
        except (ValueError, AssertionError):
            reasons.add("BAD_AMOUNT_TYPE")

        sb = (t.get("source_batch") or "").strip()
        if sb and sb != "T-B1":
            reasons.add("CROSS_BATCH_MISMATCH")
        cb = (t.get("customer_batch_id") or "").strip()
        if cb and cb != "C-B1":
            reasons.add("CROSS_BATCH_MISMATCH")

        if not (t.get("customer_key") or "").strip():
            reasons.add("INVALID_SOURCE_RECORD")
        if not (t.get("contract_key") or "").strip():
            reasons.add("INVALID_SOURCE_RECORD")

        # 只有干净行才做时态枚举 (与装配器口径一致)
        if not reasons and et is not None:
            ch = enumerate_candidates(cust, t["customer_key"], et)
            kh = enumerate_candidates(cont, t["contract_key"], et)
            if len(ch) == 0:
                reasons.add("NO_CUSTOMER_GENERATION")
            elif len(ch) > 1:
                reasons.add("OVERLAPPING_GENERATION")
            if len(kh) == 0:
                reasons.add("NO_CONTRACT_GENERATION")
            elif len(kh) > 1:
                reasons.add("OVERLAPPING_GENERATION")
            if not reasons:
                expected_facts[tid] = {
                    "customer_revision": ch[0][0],
                    "contract_revision": kh[0][0],
                    "source_row_no": i,
                    "event_time": et,
                }
        if reasons:
            expected_quarantine.setdefault(tid, set()).update(reasons)

    return expected_facts, expected_quarantine


def assert_matches_assembly(expected_facts, expected_quarantine, facts, quarantine):
    """把装配器输出与独立期望逐项核对, 不匹配抛 AssertionError。"""
    actual_fact_ids = {f["txn_row_id"] for f in facts}
    assert actual_fact_ids == set(expected_facts), (
        f"fact id set mismatch: extra={actual_fact_ids - set(expected_facts)} "
        f"missing={set(expected_facts) - actual_fact_ids}"
    )
    fact_by_id = {f["txn_row_id"]: f for f in facts}
    for tid, exp in expected_facts.items():
        f = fact_by_id[tid]
        assert f"r{exp['customer_revision']}" in f["customer_generation_id"], (
            f"{tid}: expected customer r{exp['customer_revision']}, "
            f"got {f['customer_generation_id']}"
        )
        assert f"r{exp['contract_revision']}" in f["contract_generation_id"], (
            f"{tid}: expected contract r{exp['contract_revision']}, "
            f"got {f['contract_generation_id']}"
        )
        assert f["source_row_no"] == exp["source_row_no"]

    actual_q = {}
    for q in quarantine:
        actual_q.setdefault(q["txn_row_id"], set()).add(q["reason_code"])
    assert set(actual_q) == set(expected_quarantine), (
        f"quarantine id set mismatch: extra={set(actual_q) - set(expected_quarantine)} "
        f"missing={set(expected_quarantine) - set(actual_q)}"
    )
    for tid, exp_reasons in expected_quarantine.items():
        for reason in exp_reasons:
            assert reason in actual_q[tid], (
                f"{tid}: expected reason {reason}, got {sorted(actual_q[tid])}"
            )
    return True
