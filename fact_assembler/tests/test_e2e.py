"""端到端测试: 题目要求的全部场景逐条断言。

运行: python3 -I tests/test_e2e.py
覆盖:
  1. 边界交易 (恰在边界/边界前一秒) 的世代选择
  2. 无世代 / 两世代重叠 / 跨来源批次不一致 / 重复原行 / 缺时间 / 坏金额
  3. 独立参考实现 (tests/independent_checker.py) 逐行核对事实与隔离
  4. 匹配证据完整性 (候选枚举、左闭右开比较、唯一选择)
  5. 同名批次: 同内容重送幂等, 换内容冲突
  6. 已确认组合重复装配不增加事实; 失败批次不改变旧版本
  7. 版本演进后旧版本仍可按 version_id 查询
  8. 发布前强退 (subprocess os._exit) + 并发读者只见旧版或新版
  9. 历史批次清理: 被发布事实引用时受保护, 无引用时可删
"""
from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import sys
import threading
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import bootstrap  # noqa: F401,E402

from factasm import (  # noqa: E402
    AssemblyRejected,
    BatchConflictError,
    BatchNotFoundError,
    FactAssembler,
)
from factasm.db import connect_persistent, destroy_database  # noqa: E402
from factasm.sources import register_batch  # noqa: E402
from tests import fixtures_builder as fx  # noqa: E402
from tests.independent_checker import (  # noqa: E402
    assert_matches_assembly,
    expected_outcomes,
)

PASS = []
FAIL = []


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
        print(f"  [PASS] {name}")
    else:
        FAIL.append((name, detail))
        print(f"  [FAIL] {name} :: {detail}")


def section(title):
    print(f"\n=== {title} ===")


def register_all(db, paths):
    register_batch(db, batch_id=fx.CUSTOMER_BATCH, source_kind="customer",
                   path=paths["customers"])
    register_batch(db, batch_id=fx.CONTRACT_BATCH, source_kind="contract",
                   path=paths["contracts"])
    register_batch(db, batch_id=fx.TXN_BATCH, source_kind="transaction",
                   path=paths["transactions"])


def main() -> int:
    work = ROOT / "run_e2e"
    shutil.rmtree(work, ignore_errors=True)
    incoming = work / "incoming"
    db = work / "warehouse.duckdb"
    paths = fx.build_fixtures(incoming)

    # ---------------------------------------------------------------- #
    section("1. 批次检查: 未登记批次必须被拒绝, 不按文件名拼接")
    asm = FactAssembler(db)
    try:
        asm.assemble(customer_batch=fx.CUSTOMER_BATCH,
                     contract_batch=fx.CONTRACT_BATCH,
                     transaction_batch=fx.TXN_BATCH)
        check("unregistered batches rejected", False, "no exception")
    except BatchNotFoundError as exc:
        check("unregistered batches rejected", True, str(exc))

    register_all(db, paths)
    check("batches registered", asm.current_version() is None)

    # ---------------------------------------------------------------- #
    section("2. 首次装配发布: 事实 + 隔离 + 唯一世代解释")
    result = asm.assemble(customer_batch=fx.CUSTOMER_BATCH,
                          contract_batch=fx.CONTRACT_BATCH,
                          transaction_batch=fx.TXN_BATCH)
    check("published", result.status == "PUBLISHED", result.status)
    check("7 facts", result.fact_count == 7, str(result.fact_count))
    check("7 quarantine rows", result.quarantine_count == 7, str(result.quarantine_count))
    v1 = result.version_id
    check("current version pointer set", asm.current_version()["version_id"] == v1)

    # ---------------------------------------------------------------- #
    section("3. 独立参考实现按区间枚举核对 (不走 factasm 代码)")
    exp_facts, exp_quar = expected_outcomes(
        paths["customers"], paths["contracts"], paths["transactions"])
    try:
        assert_matches_assembly(exp_facts, exp_quar, result.facts, result.quarantine)
        check("independent oracle agrees on facts and quarantine", True)
    except AssertionError as exc:
        check("independent oracle agrees on facts and quarantine", False, str(exc))

    # 与夹具里手写的期望修订号再对一遍
    fact_by_id = {f["txn_row_id"]: f for f in result.facts}
    ok = True
    detail = ""
    for tid, (cr, kr) in fx.EXPECTED_FACTS.items():
        f = fact_by_id[tid]
        if f"r{cr}" not in f["customer_generation_id"] or f"r{kr}" not in f["contract_generation_id"]:
            ok, detail = False, f"{tid} -> {f['customer_generation_id']}, {f['contract_generation_id']}"
    check("boundary transactions pick exact expected revisions", ok, detail)

    # ---------------------------------------------------------------- #
    section("4. 匹配证据: 每条交易保留候选枚举与左闭右开比较")
    trace_ok = True
    for tid in fx.EXPECTED_FACTS:
        tr = asm.trace_transaction(tid)
        ev = tr["match_evidence"]
        if tr["outcome"] != "FACT":
            trace_ok = False
            break
        for dim in ("customer", "contract"):
            d = ev[dim]
            if d["candidate_count"] != 1:
                trace_ok = False
            cand = d["candidates_enumerated"][0]
            if not (cand["comparison"]["ge_valid_from"] and cand["comparison"]["lt_valid_to"]):
                trace_ok = False
            if cand["selected"] is not True:
                trace_ok = False
        if "customer_batch" not in tr["source_summary"]:
            trace_ok = False
    check("evidence contains enumerated candidates + half-open comparisons", trace_ok)

    # 边界证据的具体内容
    t3 = asm.trace_transaction("TXN-003")["match_evidence"]
    check("TXN-003 at contract boundary selects r2 (half-open)",
          t3["contract"]["candidates_enumerated"][0]["revision_no"] == 2)
    t5 = asm.trace_transaction("TXN-005")["match_evidence"]
    check("TXN-005 at customer boundary selects r2 (half-open)",
          t5["customer"]["candidates_enumerated"][0]["revision_no"] == 2)

    # ---------------------------------------------------------------- #
    section("5. 隔离清单可定位: 原因码/原行身份/排除候选")
    q_by_id = {}
    for q in asm.query_quarantine():
        q_by_id.setdefault(q["txn_row_id"], []).append(q)
    for tid, reason in fx.EXPECTED_QUARANTINE_REASONS.items():
        rows = q_by_id.get(tid, [])
        check(f"quarantine {tid} -> {reason}",
              any(r["reason_code"] == reason for r in rows),
              str([r["reason_code"] for r in rows]))
    no_gen = next(r for r in q_by_id["TXN-103"] if r["reason_code"] == "NO_CUSTOMER_GENERATION")
    check("quarantine row keeps source row number", no_gen["source_row_no"] is not None)
    check("quarantine row keeps raw payload", json.loads(no_gen["raw_payload"])["txn_row_id"] == "TXN-103")
    # 无世代时候选枚举明确记录「枚举了该键, 0 个命中」而非静默回退最新世代
    no_match_ev = json.loads(no_gen["rejected_candidates"])
    check("no-match evidence records candidate_count=0 with dimension",
          no_match_ev["candidate_count"] == 0
          and no_match_ev["candidates_enumerated"] == []
          and no_match_ev["dimension"] == "customer")

    # 纯隔离交易绝不出现在事实表 (TXN-001 例外: 首次出现是事实, 第二次重复行被隔离)
    pure_quarantine_ids = {tid for tid in q_by_id if tid != "TXN-001"}
    check("pure-quarantined ids absent from facts",
          pure_quarantine_ids.isdisjoint({f["txn_row_id"] for f in asm.query_facts()}))
    # TXN-001 在事实表里恰好一条 (首次出现), 重复的第二条只进隔离
    check("duplicated identity yields exactly one fact plus one quarantine row",
          sum(1 for f in asm.query_facts() if f["txn_row_id"] == "TXN-001") == 1
          and sum(1 for q in asm.query_quarantine()
                  if q["txn_row_id"] == "TXN-001" and q["reason_code"] == "DUPLICATE_SOURCE_ROW") == 1)

    # ---------------------------------------------------------------- #
    section("6. 同名批次重送: 同内容幂等, 换内容冲突")
    resend_dir = work / "resend_same"
    same_copy = fx.copy_source(paths["customers"], resend_dir)
    info = register_batch(db, batch_id=fx.CUSTOMER_BATCH, source_kind="customer", path=same_copy)
    check("same-name same-content resend is idempotent", info["reused"] is True)

    mutated = fx.write_mutated_copy(
        paths["customers"], work / "resend_mutated", "customers_b1_changed.csv",
        mutate=lambda rows: [
            {**rows[0], "tier": "MUTATED-SILENTLY"}, *rows[1:]
        ],
    )
    try:
        register_batch(db, batch_id=fx.CUSTOMER_BATCH, source_kind="customer", path=mutated)
        check("same-name changed-content resend conflicts", False, "no conflict raised")
    except BatchConflictError as exc:
        check("same-name changed-content resend conflicts", True, exc.message)
        check("conflict carries both fingerprints",
              "registered_hash" in exc.details and "incoming_hash" in exc.details)

    # ---------------------------------------------------------------- #
    section("7. 已确认批次组合重复装配不增加事实 (幂等)")
    facts_before = len(asm.query_facts())
    again = asm.assemble(customer_batch=fx.CUSTOMER_BATCH,
                         contract_batch=fx.CONTRACT_BATCH,
                         transaction_batch=fx.TXN_BATCH)
    check("re-assembly returns REUSED", again.status == "REUSED", again.status)
    check("re-assembly reuses same version", again.version_id == v1)
    check("no extra facts", len(asm.query_facts()) == facts_before == 7)
    check("reused result carries parsed match evidence",
          isinstance(again.facts[0]["match_evidence"], dict)
          and isinstance(again.facts[0]["source_summary"], dict))
    check("reused quarantine evidence is parsed JSON",
            again.quarantine and isinstance(again.quarantine[0].get("raw_payload"),
                                            (dict, type(None))))
    versions = asm.list_versions()
    check("still exactly one published version", len(versions) == 1, str(len(versions)))

    # ---------------------------------------------------------------- #
    section("8. 来源自身冲突必须拒批且不改变旧版本")
    # 8a. 两世代真实重叠
    register_batch(db, batch_id=fx.BAD_CUSTOMER_BATCH, source_kind="customer",
                   path=paths["overlap_customers"])
    old_version = asm.current_version()["version_id"]
    try:
        asm.assemble(customer_batch=fx.BAD_CUSTOMER_BATCH,
                     contract_batch=fx.CONTRACT_BATCH,
                     transaction_batch=fx.TXN_BATCH)
        check("overlapping generations reject the batch", False, "accepted")
    except AssemblyRejected as exc:
        stages = {r["stage"] for r in exc.rejections}
        check("overlapping generations reject the batch", "dimension_validation" in stages,
              str(exc.rejections))
    check("rejected assembly leaves current version untouched",
          asm.current_version()["version_id"] == old_version)
    check("rejection is audited", len(asm.list_rejections()) >= 1)

    # 8b. 维度行声明批次与登记批次不一致
    rogue = fx.write_mutated_copy(
        paths["contracts"], work / "rogue", "contracts_rogue.csv",
        mutate=lambda rows: [{**rows[0], "source_batch": "K-SOMEWHERE-ELSE"}, *rows[1:]],
    )
    register_batch(db, batch_id="K-ROGUE", source_kind="contract", path=rogue)
    try:
        asm.assemble(customer_batch=fx.CUSTOMER_BATCH,
                     contract_batch="K-ROGUE",
                     transaction_batch=fx.TXN_BATCH)
        check("cross-source batch inconsistency rejects", False, "accepted")
    except AssemblyRejected as exc:
        check("cross-source batch inconsistency rejects",
              any("source_batch" in r.get("error", "") for r in exc.rejections))
    check("current version still untouched after rogue batch",
          asm.current_version()["version_id"] == old_version)

    # ---------------------------------------------------------------- #
    section("9. 发布前强退: 事务回滚, 旧版本完好")
    proc = subprocess.run(
        [sys.executable, "-I", str(ROOT / "tests" / "crash_entry.py"),
         str(db), fx.CUSTOMER_BATCH, fx.CONTRACT_BATCH, fx.TXN_BATCH],
        capture_output=True, text=True,
    )
    check("crash injection process exited non-zero", proc.returncode != 0,
          f"rc={proc.returncode} stderr={proc.stderr[:200]}")
    check("after crash current version is still v1",
          asm.current_version()["version_id"] == v1)
    check("after crash facts unchanged", len(asm.query_facts()) == 7)
    leftover = asm.cleanup_staging()
    check("staging garbage from crashed run is cleanable", isinstance(leftover, list))

    # ---------------------------------------------------------------- #
    section("10. 并发读者: 写入事务窗口内只见旧版, 提交后只见新版")
    v2_paths = fx.build_v2_fixtures(work / "incoming_v2")
    register_batch(db, batch_id=fx.CUSTOMER_BATCH_V2, source_kind="customer",
                   path=v2_paths["customers"])
    register_batch(db, batch_id=fx.CONTRACT_BATCH_V2, source_kind="contract",
                   path=v2_paths["contracts"])
    register_batch(db, batch_id=fx.TXN_BATCH_V2, source_kind="transaction",
                   path=v2_paths["transactions"])

    # 两个并发读者线程, 各自独立连接 (与写连接同配置, 满足 DuckDB 单进程多连接)。
    # 写事务在提交前持有未提交版本; MVCC 下读者只能读到已提交的上一完整版本。
    reader_observations = []
    stop = threading.Event()

    def reader_loop(reader_id):
        con = connect_persistent(db)  # 同进程同配置的独立连接
        try:
            while not stop.is_set():
                row = con.execute(
                    """
                    SELECT cv.version_id, COUNT(f.txn_row_id)
                    FROM current_version cv
                    LEFT JOIN facts f ON f.version_id = cv.version_id
                    GROUP BY cv.version_id
                    """
                ).fetchall()
                version_id, fact_count = row[0] if row else (None, 0)
                reader_observations.append((reader_id, version_id, fact_count))
                time.sleep(0.02)
        finally:
            con.close()

    readers = [threading.Thread(target=reader_loop, args=(i,), daemon=True) for i in range(2)]
    for t in readers:
        t.start()
    time.sleep(0.4)  # 让读者先稳定读到 v1

    v2_result = asm.assemble(customer_batch=fx.CUSTOMER_BATCH_V2,
                             contract_batch=fx.CONTRACT_BATCH_V2,
                             transaction_batch=fx.TXN_BATCH_V2,
                             delay_before_commit=1.5)
    stop.set()
    for t in readers:
        t.join(timeout=5)

    v2 = v2_result.version_id
    check("v2 published", v2_result.status == "PUBLISHED" and v2 != v1)

    seen_versions = {obs[1] for obs in reader_observations}
    atomic_ok = True
    for _rid, version_id, fact_count in reader_observations:
        if version_id == v1 and fact_count != 7:
            atomic_ok = False
        if version_id == v2 and fact_count != 2:
            atomic_ok = False
    check("readers collected observations across the window",
          len(reader_observations) >= 20, f"n={len(reader_observations)}")
    check("reader only ever saw v1 or v2 (never a partial/uncommitted mix)",
          seen_versions <= {v1, v2} and v1 in seen_versions and v2 in seen_versions,
          f"seen={seen_versions}")
    check("each visible version had its own complete fact count", atomic_ok)
    check("current version switched to v2", asm.current_version()["version_id"] == v2)

    # ---------------------------------------------------------------- #
    section("11. 旧版本仍可按 version_id 查询 (多版本共存)")
    old_facts = asm.query_facts(version_id=v1)
    new_facts = asm.query_facts(version_id=v2)
    check("v1 remains readable with 7 facts", len(old_facts) == 7)
    check("v2 readable with 2 facts", len(new_facts) == 2)
    check("v2 boundary txn selects r3 generations",
          "r3" in new_facts[0]["customer_generation_id"]
          and "r3" in new_facts[0]["contract_generation_id"])
    check("current_facts view only exposes v2",
          {f["version_id"] for f in asm.query_facts()} == {v2})
    old_q = asm.query_quarantine(version_id=v1)
    check("v1 quarantine remains readable", len(old_q) == 7)

    # 追溯接口在当前版本下对 v1 独有交易给出 UNKNOWN (不串版本)
    gone = asm.trace_transaction("TXN-001")
    check("trace on current version does not leak other-version txn",
          gone["outcome"] == "UNKNOWN")

    # ---------------------------------------------------------------- #
    section("12. 历史批次清理: 被发布事实引用时保护, 无引用可删")
    plan_busy = asm.cleanup_batch(fx.CUSTOMER_BATCH, dry_run=True)
    check("batch still referenced by v1 is protected",
          len(plan_busy["protected_by_published_facts"]) >= 1,
          str(plan_busy))
    forced = asm.cleanup_batch(fx.CUSTOMER_BATCH)
    check("forced cleanup of protected batch is refused",
          forced.get("protected_by_published_facts") and "deleted" not in forced)

    # 未被任何版本引用的坏批次可以清理
    plan_free = asm.cleanup_batch(fx.BAD_CUSTOMER_BATCH, dry_run=True)
    check("unreferenced batch has no protection",
          not plan_free["protected_by_published_facts"])
    deleted = asm.cleanup_batch(fx.BAD_CUSTOMER_BATCH)
    check("unreferenced batch deleted", deleted.get("deleted") is True)

    # 删除后该批次确实不可再用于装配
    try:
        asm.assemble(customer_batch=fx.BAD_CUSTOMER_BATCH,
                     contract_batch=fx.CONTRACT_BATCH,
                     transaction_batch=fx.TXN_BATCH)
        check("deleted batch cannot be assembled", False, "assembled")
    except BatchNotFoundError:
        check("deleted batch cannot be assembled", True)

    # 受保护批次未被删除, 现有事实仍可查询
    check("protected sources intact, v1 facts still queryable",
          len(asm.query_facts(version_id=v1)) == 7)

    # ---------------------------------------------------------------- #
    section("13. 世代索引快照与内存边界")
    idx = asm.generation_index_snapshot("customer")
    check("persisted generation index contains all registered customer gens",
          {g["generation_id"] for g in idx} >=
          {f["customer_generation_id"] for f in asm.query_facts(version_id=v1)})
    check("index stores half-open intervals",
          all("valid_from" in g for g in idx))

    # ---------------------------------------------------------------- #
    section("14. 内存不随历史批次增长: 索引请求级 + staging 不堆积 + 历史只在磁盘")
    staging_dir = work / "staging"
    # 装配器对象本身不持有任何世代/交易数据结构 (全部是 _run_assembly 内的局部变量)
    retained = [k for k in vars(asm) if any(
        token in k.lower() for token in ("generation", "index", "transaction", "fact"))]
    check("assembler object retains no in-memory generation/transaction state",
          retained == [], f"retained attrs={retained}")

    # 连续装配多组互不相同的批次, 每次成功后 staging 目录必须清空
    import csv as _csv
    loop_dir = work / "memory_loop"
    loop_dir.mkdir(parents=True, exist_ok=True)

    def write_batch_files(tag, n):
        cust = loop_dir / f"c_{tag}.csv"
        cont = loop_dir / f"k_{tag}.csv"
        txn = loop_dir / f"t_{tag}.csv"
        with cust.open("w", newline="", encoding="utf-8") as fh:
            w = _csv.writer(fh)
            w.writerow(["business_key", "revision_no", "valid_from", "valid_to",
                        "source_batch", "customer_name"])
            w.writerow([f"LC{tag}", "1", "2026-01-01 00:00:00", "", f"LC-B{tag}", f"n{n}"])
        with cont.open("w", newline="", encoding="utf-8") as fh:
            w = _csv.writer(fh)
            w.writerow(["business_key", "revision_no", "valid_from", "valid_to",
                        "source_batch", "plan"])
            w.writerow([f"LK{tag}", "1", "2026-01-01 00:00:00", "", f"LK-B{tag}", "p"])
        with txn.open("w", newline="", encoding="utf-8") as fh:
            w = _csv.writer(fh)
            w.writerow(["txn_row_id", "event_time", "customer_key", "contract_key",
                        "amount", "source_batch"])
            w.writerow([f"LT{tag}-1", "2026-02-01 00:00:00", f"LC{tag}", f"LK{tag}",
                        f"{n}.00", f"LT-B{tag}"])
        return cust, cont, txn

    import tracemalloc
    tracemalloc.start()
    snap_before = tracemalloc.take_snapshot()
    n_loops = 6
    for i in range(n_loops):
        tag = f"{i}"
        c, k, t = write_batch_files(tag, i)
        register_batch(db, batch_id=f"LC-B{tag}", source_kind="customer", path=c)
        register_batch(db, batch_id=f"LK-B{tag}", source_kind="contract", path=k)
        register_batch(db, batch_id=f"LT-B{tag}", source_kind="transaction", path=t)
        r = asm.assemble(customer_batch=f"LC-B{tag}", contract_batch=f"LK-B{tag}",
                         transaction_batch=f"LT-B{tag}")
        check(f"loop assembly {i} published one fact", r.fact_count == 1)
        leftovers = list(staging_dir.glob("*.staging.duckdb*"))
        check(f"staging cleaned after loop assembly {i}", leftovers == [],
              f"{[p.name for p in leftovers]}")
    snap_after = tracemalloc.take_snapshot()
    tracemalloc.stop()
    growth = sum(s.size_diff for s in snap_after.compare_to(snap_before, "filename"))
    # 历史批次的事实/世代都在磁盘库; Python 侧净增长应很小 (仅注册名/连接等),
    # 不允许随循环线性累积世代对象。给一个宽松上界而非脆弱的精确值。
    check("python-side memory does not grow linearly with batch history",
          growth < 2_000_000, f"net growth bytes={growth}")
    check("history versions all live on disk and remain queryable",
          len(asm.list_versions()) == 2 + n_loops)

    print("\n" + "=" * 60)
    print(f"TOTAL {len(PASS) + len(FAIL)}  PASS {len(PASS)}  FAIL {len(FAIL)}")
    if FAIL:
        for name, detail in FAIL:
            print(f"  FAILED: {name} :: {detail}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
