"""防御路径单元测试: 重叠枚举、半开边界、隔离去重/代理主键。

这些路径在正常装配中被批次级校验提前拦截, 这里直接构造内存对象测试,
确保参考实现与去重逻辑本身正确, 而不是依赖端到端间接覆盖。

运行: python3 -I tests/test_unit.py
"""
from __future__ import annotations

import datetime as dt
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import bootstrap  # noqa: F401,E402

from factasm import reference as reference_mod  # noqa: E402
from factasm.assembler import FactAssembler  # noqa: E402
from factasm.util import Reason, parse_amount  # noqa: E402
from factasm.validation import (  # noqa: E402
    Generation,
    GenerationIndex,
    ParsedTransaction,
    parse_dimension_rows,
    parse_transaction_rows,
)
from factasm.errors import ValidationError  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + ("" if cond else f" :: {detail}"))


def gen(gid, key, rev, vf, vt):
    return Generation(
        generation_id=gid, batch_id="B", business_key=key, revision_no=rev,
        valid_from=dt.datetime.fromisoformat(vf),
        valid_to=None if vt is None else dt.datetime.fromisoformat(vt),
        attributes={}, source_row_no=rev, content_hash="h",
    )


def txn(tid, ts, ck="C", kk="K"):
    return ParsedTransaction(
        txn_row_id=tid, source_row_no=1,
        event_time=dt.datetime.fromisoformat(ts),
        customer_key=ck, contract_key=kk, amount=parse_amount("10.00"),
        raw_payload={}, quarantine_reasons=[],
    )


def main() -> int:
    print("=== 半开区间边界 (相等边界不重叠, 命中规则 [from,to)) ===")
    g1 = gen("g1", "C", 1, "2026-01-01T00:00:00", "2026-03-01T00:00:00")
    g2 = gen("g2", "C", 2, "2026-03-01T00:00:00", None)
    idx = GenerationIndex({"C": [g1, g2]})
    check("boundary instant belongs to later generation (half-open)",
          [x.generation_id for x in idx.candidates_at("C", dt.datetime(2026, 3, 1))] == ["g2"])
    check("one microsecond before boundary belongs to earlier generation",
          [x.generation_id for x in idx.candidates_at("C", dt.datetime(2026, 2, 28, 23, 59, 59))] == ["g1"])
    check("open-ended generation contains far-future timestamp",
          [x.generation_id for x in idx.candidates_at("C", dt.datetime(2030, 1, 1))] == ["g2"])
    check("unknown key has no candidates", idx.candidates_at("ZZ", dt.datetime(2026, 2, 1)) == [])

    print("=== 参考实现: 两侧都重叠时产生两条同码不同证据的隔离原因 ===")
    ov1 = gen("o1", "X", 1, "2026-01-01T00:00:00", "2026-02-15T00:00:00")
    ov2 = gen("o2", "X", 2, "2026-02-01T00:00:00", None)
    om1 = gen("m1", "M", 1, "2026-01-01T00:00:00", "2026-02-15T00:00:00")
    om2 = gen("m2", "M", 2, "2026-02-10T00:00:00", None)
    cidx = GenerationIndex({"X": [ov1, ov2]})
    kidx = GenerationIndex({"M": [om1, om2]})
    facts, quar, evidence = reference_mod.correlate(
        [txn("T1", "2026-02-11T00:00:00", ck="X", kk="M")], cidx, kidx)
    check("overlap on both dimensions yields no fact", facts == [])
    codes = [q["reason_code"] for q in quar]
    check("two OVERLAPPING_GENERATION reasons (one per dimension)",
          codes == [Reason.OVERLAPPING_GENERATION, Reason.OVERLAPPING_GENERATION], str(codes))
    import json
    distinct_evidence = {json.loads(q["rejected_candidates"])["dimension"] for q in quar}
    check("the two same-code reasons carry distinct dimension evidence",
          distinct_evidence == {"customer", "contract"}, str(distinct_evidence))

    print("=== 隔离去重 + 代理主键 (同码同证据去重, 同码异证据保留) ===")
    rows = [
        {"txn_row_id": "T", "source_row_no": 1, "reason_code": Reason.BAD_AMOUNT_TYPE,
         "reason_detail": "x", "rejected_candidates": None, "raw_payload": "{}"},
        {"txn_row_id": "T", "source_row_no": 1, "reason_code": Reason.BAD_AMOUNT_TYPE,
         "reason_detail": "x", "rejected_candidates": None, "raw_payload": "{}"},  # 完全重复
        {"txn_row_id": "T", "source_row_no": 1, "reason_code": Reason.OVERLAPPING_GENERATION,
         "reason_detail": "cust", "rejected_candidates": '{"dimension":"customer"}', "raw_payload": "{}"},
        {"txn_row_id": "T", "source_row_no": 1, "reason_code": Reason.OVERLAPPING_GENERATION,
         "reason_detail": "cont", "rejected_candidates": '{"dimension":"contract"}', "raw_payload": "{}"},
    ]
    deduped = FactAssembler._dedupe_and_id_quarantine(rows, "req-1")
    check("exact duplicate collapsed, distinct-evidence same-code kept",
          len(deduped) == 3, str(len(deduped)))
    check("every surviving row has a unique q_id",
          len({r["q_id"] for r in deduped}) == 3)
    check("q_id is stable across repeated calls",
          FactAssembler._dedupe_and_id_quarantine(rows[:1], "req-1")[0]["q_id"]
          == FactAssembler._dedupe_and_id_quarantine(rows[:1], "req-1")[0]["q_id"])

    print("=== 维度批次级校验: 真重叠拒批, 相等边界放行 ===")
    touching = [
        {"business_key": "C", "revision_no": "1", "valid_from": "2026-01-01 00:00:00",
         "valid_to": "2026-03-01 00:00:00", "source_batch": "B", "__source_row_no": 1},
        {"business_key": "C", "revision_no": "2", "valid_from": "2026-03-01 00:00:00",
         "valid_to": "", "source_batch": "B", "__source_row_no": 2},
    ]
    grouped = parse_dimension_rows(touching, kind="customer", batch_id="B", batch_content_hash="h")
    check("touching half-open boundaries accepted", len(grouped["C"]) == 2)

    overlapping = [
        {"business_key": "X", "revision_no": "1", "valid_from": "2026-01-01 00:00:00",
         "valid_to": "2026-02-15 00:00:00", "source_batch": "B", "__source_row_no": 1},
        {"business_key": "X", "revision_no": "2", "valid_from": "2026-02-01 00:00:00",
         "valid_to": "", "source_batch": "B", "__source_row_no": 2},
    ]
    try:
        parse_dimension_rows(overlapping, kind="customer", batch_id="B", batch_content_hash="h")
        check("real overlap rejected", False, "accepted")
    except ValidationError:
        check("real overlap rejected", True)

    print("=== 交易解析: 多原因累积, 重复身份不进入关联 ===")
    txn_rows = [
        {"txn_row_id": "A", "event_time": "2026-02-01 00:00:00", "customer_key": "C",
         "contract_key": "K", "amount": "1.00", "source_batch": "TB", "__source_row_no": 1},
        {"txn_row_id": "A", "event_time": "", "customer_key": "C",
         "contract_key": "K", "amount": "bad", "source_batch": "TB", "__source_row_no": 2},
    ]
    clean, q = parse_transaction_rows(
        txn_rows, batch_id="TB", customer_batch_id="CB", contract_batch_id="KB")
    check("first identity kept for correlation", [t.txn_row_id for t in clean] == ["A"])
    second = [r for r in q if r["source_row_no"] == 2]
    codes2 = {r["reason_code"] for r in second}
    check("duplicate row accumulates all its reasons",
          Reason.DUPLICATE_SOURCE_ROW in codes2 and Reason.MISSING_EVENT_TIME in codes2
          and Reason.BAD_AMOUNT_TYPE in codes2, str(codes2))

    print("\n" + "=" * 56)
    print(f"UNIT TOTAL {len(PASS) + len(FAIL)}  PASS {len(PASS)}  FAIL {len(FAIL)}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
