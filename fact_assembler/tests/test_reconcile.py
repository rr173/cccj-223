"""回单勾稽台账 (reconcile) 测试: 拆组/改接/拒绝原子性/增量范围/幂等。

运行:
    cd fact_assembler
    python3 -I tests/test_reconcile.py

覆盖题目全部要求:
  1. 初始化: 只读消费 allocate 账簿, 原账簿字节不变; 重复初始化拒绝
  2. 题目例子: A=40/B=60, X->A(40), Y->A,B(60); Y 改指 B 后必须拆成两个
     相符组 (不是总数不变的旧组); 之后把拆出的部分接回原组
  3. 单边组: 只有回单/只有明细, 金额为零也不得报 MATCHED
  4. 同一明细被多张回单覆盖时只计一次; 回单可直接查关联
  5. 批次校验: 同批重复改、同回单重复关联、不存在的明细、撤销未知回单、
     格式错误 (金额非整数/ID 非字符串/未知 op/非法 JSON) 全部整批拒绝
  6. 批次中途发现非法关联: 同一批里合法的回单也不落库 (原子)
  7. 批号: 从 1 连续; 跳号/回退拒绝; 同字节重送幂等 (返回 REUSED, 状态不变);
     同批号不同内容拒绝
  8. 撤销回单: 关联一并删除, 明细回到自己的单边组
  9. 每组金额/差额/状态与独立全图重算一致 (每批前后核对)
 10. 增量范围: 大量无关小组时, 批次的 scope 只含受影响旧组与新节点;
     无关组的成员行与汇总行完全未变; recon_recompute_log 给出原因与计数
 11. 失败批次不改变成员/汇总/批次号; 同批号可再次提交合法内容
 12. CLI: 初始化/批次/错误退出码
"""
from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import bootstrap  # noqa: F401,E402

from factasm import (  # noqa: E402
    BatchSequenceError,
    ChangesValidationError,
    ReconcileLedgerError,
    StateExistsError,
    initialize_state,
    reconcile_batch,
    verify_state,
)
from tests import reconcile_fixtures as rfx  # noqa: E402
from tests.reconcile_independent import recompute_all  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
        print(f"  [PASS] {name}")
    else:
        FAIL.append((name, detail))
        print(f"  [FAIL] {name} :: {detail}")


def section(title):
    print(f"\n=== {title} ===")


# --------------------------------------------------------------------------- #
# 只通过公共视图读取台账 (与财务的查询方式一致)
# --------------------------------------------------------------------------- #
def q(state, sql, params=None):
    import duckdb

    con = duckdb.connect(str(state), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def logs(state, seq):
    return q(state, "SELECT old_group_id, new_group_id, reason, old_member_count, "
                    "new_member_count, retained_members, removed_members, "
                    "inserted_members, scanned_links, visited_nodes "
                    "FROM recon_recompute_log WHERE seq = ? ORDER BY old_group_id, "
                    "new_group_id", [seq])


def group_of_receipt(state, rid):
    rows = q(state, "SELECT group_id FROM recon_member_groups "
                    "WHERE member_kind='RECEIPT' AND receipt_id = ?", [rid])
    return rows[0][0] if rows else None


def group_of_detail(state, txn, cc):
    rows = q(state, "SELECT group_id FROM recon_member_groups "
                    "WHERE member_kind='DETAIL' AND txn_row_id = ? AND cost_center = ?",
             [txn, cc])
    return rows[0][0] if rows else None


def summary_of(state, gid):
    return q(state, "SELECT status, receipt_total, detail_total, difference, "
                    "receipt_count, detail_count FROM recon_group_summary "
                    "WHERE group_id = ?", [gid])


def members_of(state, gid):
    return q(state, "SELECT member_kind, COALESCE(receipt_id, txn_row_id), "
                    "cost_center, amount_units, direct_peer_count "
                    "FROM recon_group_members WHERE group_id = ? "
                    "ORDER BY member_kind, 2, 3", [gid])


def detail_key_group_map(state):
    """明细键 -> group_id; 同时返回回单 -> group_id。"""
    d = {(t, c): g for g, t, c in q(
        state, "SELECT group_id, txn_row_id, cost_center FROM recon_member_groups "
               "WHERE member_kind='DETAIL'")}
    r = {rid: g for g, rid in q(
        state, "SELECT group_id, receipt_id FROM recon_member_groups "
               "WHERE member_kind='RECEIPT'")}
    return d, r


def apply(state, seq, lines, work, raw=None):
    path = pathlib.Path(work) / f"changes-{seq}.jsonl"
    if raw is None:
        raw = "".join(json.dumps(l, ensure_ascii=False) + "\n" for l in lines).encode()
        path.write_bytes(raw)
    else:
        path.write_bytes(raw)
    return reconcile_batch(state, seq=seq, changes_path=path,
                           changes_bytes=raw)


def compare_with_oracle(state, note):
    """独立全图重算: 成员分组/状态/两侧合计/差额必须与台账完全一致。"""
    oracle = recompute_all(str(state))
    d_map, r_map = detail_key_group_map(state)
    ok = True
    detail = []
    # 台账分组 -> 预言机分组 (用组内任一节点对齐)
    for (txn, cc), gid in d_map.items():
        key = f"DETAIL|{json.dumps([txn, cc], ensure_ascii=False)}"
        txn_oracle = oracle["groups"][oracle["node_to_group"][key]]
        row = summary_of(state, gid)
        if not row:
            ok = False
            detail.append(f"no summary for detail {(txn, cc)}")
            continue
        status, rt, dt_, diff, rc, dc = row[0]
        if (status, rt, dt_, diff) != (txn_oracle["status"],
                                       txn_oracle["receipt_total"],
                                       txn_oracle["detail_total"],
                                       txn_oracle["difference"]):
            ok = False
            detail.append(f"detail {(txn, cc)} group {gid}: ledger={row} oracle={txn_oracle}")
    # 回单也能在预言机里找到同组状态
    for rid, gid in r_map.items():
        key = f"RECEIPT|{json.dumps(rid, ensure_ascii=False)}"
        og = oracle["groups"][oracle["node_to_group"][key]]
        row = summary_of(state, gid)
        if row[0][0] != og["status"] or row[0][3] != og["difference"]:
            ok = False
            detail.append(f"receipt {rid}: ledger={row} oracle={og}")
    check(f"matches independent full-graph recompute ({note})", ok,
          "; ".join(detail[:4]))
    return oracle


# --------------------------------------------------------------------------- #
# 夹具: 一个只有 A=40 / B=60 的账簿
# --------------------------------------------------------------------------- #
def main() -> int:
    work = ROOT / "run_reconcile"
    shutil.rmtree(work, ignore_errors=True)
    fx = rfx.rebuild(work / "example")
    state = fx["state"]
    A, B = fx["detail_keys"]["A"], fx["detail_keys"]["B"]

    # ---------------------------------------------------------------- #
    section("1. 初始化: 原账簿只读, 已有勾稽库拒绝重新初始化")
    import hashlib
    ledger_sha = hashlib.sha256(pathlib.Path(fx["ledger"]).read_bytes()).hexdigest()
    check("init copies 2 allocation details", fx["init"].detail_count == 2)
    check("init records version/batch/rules digest",
          fx["init"].version_id == fx["version_id"]
          and fx["init"].transaction_batch_id == rfx.TXN_BATCH
          and len(fx["init"].rules_sha256) == 64)
    try:
        initialize_state(fx["ledger"], state)
        check("reinitialize existing state refused", False, "no exception")
    except StateExistsError as exc:
        check("reinitialize existing state refused", exc.code == "RECONCILE_STATE_EXISTS")
    check("ledger bytes unchanged by init+refusal",
          hashlib.sha256(pathlib.Path(fx["ledger"]).read_bytes()).hexdigest()
          == ledger_sha)
    strict = work / "strict"
    strict.mkdir()
    bad_ledger = strict / "not-a-ledger.duckdb"
    import duckdb
    con = duckdb.connect(str(bad_ledger))
    con.execute("CREATE TABLE allocation(txn_row_id VARCHAR, cost_center VARCHAR, "
                "amount_units HUGEINT)")
    con.close()
    try:
        initialize_state(bad_ledger, strict / "state.duckdb")
        check("ledger without ledger_meta refused", False, "no exception")
    except ReconcileLedgerError as exc:
        check("ledger without ledger_meta refused",
              exc.code == "RECONCILE_LEDGER_INVALID")
    check("failed init leaves no state file", not (strict / "state.duckdb").exists())
    check("initial member layout: two only-detail groups, state self-consistent",
          q(state, "SELECT COUNT(*) FROM recon_group_summary") == [(2,)]
          and q(state, "SELECT COUNT(*) FROM recon_group_summary "
                       "WHERE status='ONLY_DETAIL'") == [(2,)]
          and verify_state(state)["empty_groups"] == 0)
    compare_with_oracle(state, "after init")

    # ---------------------------------------------------------------- #
    section("2. 题目例子: 拆组 (Y 从 A、B 改为只指 B)")
    r1 = apply(state, 1, [
        {"op": "upsert", "receipt_id": "X", "amount_units": 40_000_000,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}]},
        {"op": "upsert", "receipt_id": "Y", "amount_units": 60_000_000,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"},
                   {"txn_row_id": "B", "cost_center": "CC_EXAMPLE"}]},
    ], work)
    check("batch 1 applied", r1.applied and r1.receipt_upserts == 2)
    check("batch 1: one matched group of 4 members",
          r1.group_count == 1
          and q(state, "SELECT status, receipt_total, detail_total, difference "
                       "FROM recon_group_summary") == [("MATCHED", 100_000_000,
                                                        100_000_000, 0)])
    gid_before = group_of_receipt(state, "X")
    check("X and Y and A and B all in the same group",
          {group_of_receipt(state, "X"), group_of_receipt(state, "Y"),
           group_of_detail(state, *A), group_of_detail(state, *B)} == {gid_before})
    check("receipt direct links inspectable: Y -> A,B; X -> A",
          q(state, "SELECT receipt_id, txn_row_id FROM recon_receipt_links "
                   "ORDER BY receipt_id, txn_row_id")
          == [("X", "A"), ("Y", "A"), ("Y", "B")])
    check("direct_peer_count reflects direct links",
          q(state, "SELECT receipt_id, direct_peer_count FROM recon_group_members "
                   "WHERE member_kind='RECEIPT' ORDER BY receipt_id")
          == [("X", 1), ("Y", 2)])
    compare_with_oracle(state, "after batch 1")

    r2 = apply(state, 2, [
        {"op": "upsert", "receipt_id": "Y", "amount_units": 60_000_000,
         "links": [{"txn_row_id": "B", "cost_center": "CC_EXAMPLE"}]},
    ], work)
    groups_x = group_of_receipt(state, "X")
    groups_y = group_of_receipt(state, "Y")
    check("split produces TWO distinct correct groups, not one merged leftover",
          groups_x != groups_y
          and summary_of(state, groups_x) == [("MATCHED", 40_000_000, 40_000_000,
                                              0, 1, 1)]
          and summary_of(state, groups_y) == [("MATCHED", 60_000_000, 60_000_000,
                                              0, 1, 1)],
          f"X={groups_x} {summary_of(state, groups_x)} Y={groups_y} "
          f"{summary_of(state, groups_y)}")
    check("split: X+A in one group, Y+B in the other",
          {group_of_detail(state, *A), group_of_detail(state, *B)} == {groups_x, groups_y}
          and group_of_detail(state, *A) == groups_x
          and group_of_detail(state, *B) == groups_y)
    check("batch 2 recompute log shows the old group split into two",
          len(logs(state, 2)) >= 2
          and {row[1] for row in logs(state, 2) if row[0] is not None}
          == {groups_x, groups_y}
          and all(row[4] == 2 for row in logs(state, 2)
                  if row[0] is not None),
          str(logs(state, 2)))
    check("split batch touches only 2 groups overall",
          r2.group_count == 2 and r2.affected_old_group_count == 1
          and r2.unaffected_group_count == 0)
    compare_with_oracle(state, "after split")

    # 改接别组: 新回单 Z 指向 B, 应并入原本与 X+A 无关的 Y+B 组
    r3 = apply(state, 3, [
        {"op": "upsert", "receipt_id": "Z", "amount_units": 60_000_000,
         "links": [{"txn_row_id": "B", "cost_center": "CC_EXAMPLE"}]},
    ], work)
    gx, gy = group_of_receipt(state, "X"), group_of_receipt(state, "Y")
    gz = group_of_receipt(state, "Z")
    check("new receipt joins existing Y+B group without touching X+A",
          gz == gy and gx != gy
          and summary_of(state, gx) == [("MATCHED", 40_000_000, 40_000_000, 0, 1, 1)]
          and summary_of(state, gy) == [("DIFFERENCE", 120_000_000, 60_000_000,
                                         60_000_000, 2, 1)])
    check("batch 3 scope is only the Y group + new Z",
          r3.affected_old_group_count == 1 and r3.unaffected_group_count == 1)
    # 把拆出的部分接回原组: Z 改指 A, 与 X 同组; Y 保持只指 B
    r4 = apply(state, 4, [
        {"op": "upsert", "receipt_id": "Z", "amount_units": 40_000_000,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}]},
    ], work)
    g_a = group_of_receipt(state, "X")
    check("re-route Z to A bridges back to X's group",
          group_of_receipt(state, "Z") == g_a
          and summary_of(state, g_a) == [("DIFFERENCE", 80_000_000, 40_000_000,
                                          40_000_000, 2, 1)]
          and summary_of(state, group_of_receipt(state, "Y"))
          == [("MATCHED", 60_000_000, 60_000_000, 0, 1, 1)])
    check("batch 4 affected old groups = Z's old group and A's group",
          r4.affected_old_group_count == 2, str(r4.as_dict()))
    compare_with_oracle(state, "after re-route")

    # ---------------------------------------------------------------- #
    section("3. 单边组: 只有一侧时金额为零也不得报相符")
    r5 = apply(state, 5, [
        {"op": "upsert", "receipt_id": "EMPTY_R", "amount_units": 0, "links": []},
    ], work)
    check("zero-amount receipt with no links is ONLY_RECEIPT",
          summary_of(state, group_of_receipt(state, "EMPTY_R"))
          == [("ONLY_RECEIPT", 0, 0, 0, 1, 0)])
    # 只指一条金额为零的明细会需要零额明细; 用 X 改指 A 后把 X 金额改为 0,
    # 组里仍有明细 -> MATCHED 是允许的 (两边都在); 这里验证纯单边:
    check("zero-total group with only one side never reports MATCHED",
          q(state, "SELECT COUNT(*) FROM recon_group_summary WHERE "
                   "status='MATCHED' AND (receipt_count=0 OR detail_count=0)")
          == [(0,)])
    compare_with_oracle(state, "after single-sided receipt")

    # ---------------------------------------------------------------- #
    section("4. 同一明细被多张回单覆盖: 明细金额只计一次")
    r6 = apply(state, 6, [
        {"op": "upsert", "receipt_id": "P1", "amount_units": 15_000_000,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}]},
        {"op": "upsert", "receipt_id": "P2", "amount_units": 25_000_000,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}]},
    ], work)
    # P1/P2 与 A 及 X/Z 同组; A 侧仍只算 40
    row = summary_of(state, group_of_receipt(state, "P1"))
    # 同组已有 X/Z 各 40, 再加 P1=15、P2=25 -> 回单合计 120; 明细 A 只计一次 40
    check("shared detail counted once (receipt 120 vs detail 40, diff 80)",
          row == [("DIFFERENCE", 120_000_000, 40_000_000, 80_000_000, 4, 1)],
          str(row))
    check("each receipt lists its own direct link separately",
          q(state, "SELECT receipt_id, detail_amount_units FROM recon_receipt_links "
                   "WHERE txn_row_id='A' ORDER BY receipt_id")
          == [("P1", 40_000_000), ("P2", 40_000_000), ("X", 40_000_000),
              ("Z", 40_000_000)],
          str(q(state, "SELECT receipt_id FROM recon_receipt_links")))
    compare_with_oracle(state, "after shared detail")

    # ---------------------------------------------------------------- #
    section("5. 批次校验: 格式/重复/引用错误整批拒绝")
    before_members = q(state, "SELECT node_id, group_id FROM recon_member "
                              "ORDER BY node_id")
    before_groups = q(state, "SELECT * FROM recon_group_summary ORDER BY group_id")
    before_log = q(state, "SELECT COUNT(*) FROM recon_batch_report")

    def expect_reject(name, lines, raw=None, error=ChangesValidationError):
        try:
            apply(state, 7, lines, work, raw=raw)
            check(name, False, "batch accepted but must be rejected")
        except error as exc:
            check(name, getattr(exc, "code", None) == "RECONCILE_BATCH_REJECTED"
                  or error is BatchSequenceError, str(exc))
        except Exception as exc:  # noqa: BLE001
            check(name, False, f"{type(exc).__name__}: {exc}")

    expect_reject("duplicate modification of same receipt rejected", [
        {"op": "upsert", "receipt_id": "DUP", "amount_units": 1, "links": []},
        {"op": "delete", "receipt_id": "DUP"},
    ])
    expect_reject("duplicate link inside one receipt rejected", [
        {"op": "upsert", "receipt_id": "DUP", "amount_units": 1,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"},
                   {"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}]},
    ])
    expect_reject("link to non-existent detail rejected", [
        {"op": "upsert", "receipt_id": "DUP", "amount_units": 1,
         "links": [{"txn_row_id": "NOPE", "cost_center": "CC_EXAMPLE"}]},
    ])
    expect_reject("link to existing txn but wrong cost center rejected", [
        {"op": "upsert", "receipt_id": "DUP", "amount_units": 1,
         "links": [{"txn_row_id": "A", "cost_center": "CC_OTHER"}]},
    ])
    expect_reject("delete unknown receipt rejected", [
        {"op": "delete", "receipt_id": "NEVER-SEEN"},
    ])
    expect_reject("empty receipt_id rejected", [
        {"op": "upsert", "receipt_id": "", "amount_units": 1, "links": []},
    ])
    expect_reject("non-integer amount rejected", [
        {"op": "upsert", "receipt_id": "DUP", "amount_units": 1.5, "links": []},
    ])
    expect_reject("string amount rejected", [
        {"op": "upsert", "receipt_id": "DUP", "amount_units": "100", "links": []},
    ])
    expect_reject("bool amount rejected", [
        {"op": "upsert", "receipt_id": "DUP", "amount_units": True, "links": []},
    ])
    expect_reject("unknown op rejected", [
        {"op": "insert", "receipt_id": "DUP", "amount_units": 1, "links": []},
    ])
    expect_reject("delete with extra keys rejected", [
        {"op": "delete", "receipt_id": "X", "amount_units": 1},
    ])
    expect_reject("upsert unknown keys rejected", [
        {"op": "upsert", "receipt_id": "DUP", "amount_units": 1, "links": [],
         "note": "x"},
    ])
    expect_reject("malformed JSON line rejected", [], raw=b'{"op":"upsert",\n')
    expect_reject("non-UTF8 bytes rejected", [], raw=b'\xff\xfe\n')
    expect_reject("JSON array line rejected", [], raw=b'[1,2]\n')

    check("all rejections left members/groups/batch count untouched",
          q(state, "SELECT node_id, group_id FROM recon_member ORDER BY node_id")
          == before_members
          and q(state, "SELECT * FROM recon_group_summary ORDER BY group_id")
          == before_groups
          and q(state, "SELECT COUNT(*) FROM recon_batch_report") == before_log)

    # ---------------------------------------------------------------- #
    section("6. 批次中途发现非法关联: 合法行也不落库")
    before = q(state, "SELECT receipt_id, amount_units FROM recon_receipt "
                      "ORDER BY receipt_id")
    try:
        apply(state, 7, [
            {"op": "upsert", "receipt_id": "GOOD", "amount_units": 1,
             "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}]},
            {"op": "upsert", "receipt_id": "BAD", "amount_units": 1,
             "links": [{"txn_row_id": "MISSING", "cost_center": "CC_EXAMPLE"}]},
        ], work)
        check("mid-batch illegal link rejects whole batch", False, "accepted")
    except ChangesValidationError as exc:
        check("mid-batch illegal link rejects whole batch",
              exc.details["missing_detail_keys"]
              == [{"txn_row_id": "MISSING", "cost_center": "CC_EXAMPLE"}], str(exc))
    check("legal peer receipt from the rejected batch was not written",
          q(state, "SELECT receipt_id, amount_units FROM recon_receipt "
                   "ORDER BY receipt_id") == before
          and q(state, "SELECT COUNT(*) FROM recon_batch WHERE seq=7") == [(0,)])
    # 同一批号随后提交合法内容可以成功 (拒绝未占用批号)
    r7 = apply(state, 7, [
        {"op": "upsert", "receipt_id": "GOOD", "amount_units": 40_000_000,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}]},
    ], work)
    check("same seq accepted after a rejected attempt", r7.applied)
    compare_with_oracle(state, "after recovered batch 7")

    # ---------------------------------------------------------------- #
    section("7. 批号连续/幂等重送/内容冲突")
    changes8 = [{"op": "upsert", "receipt_id": "R8", "amount_units": 5,
                 "links": []}]
    try:
        apply(state, 9, changes8, work)
        check("cannot skip seq", False, "accepted seq 9")
    except BatchSequenceError as exc:
        check("cannot skip seq", exc.code == "RECONCILE_BATCH_SEQUENCE"
              and exc.details["expected"] == 8, str(exc.details))
    try:
        apply(state, 6, changes8, work)
        check("cannot replay an already applied seq", False, "accepted seq 6")
    except BatchSequenceError as exc:
        check("cannot replay an already applied seq",
              "different file content" in exc.message, str(exc))
    r8 = apply(state, 8, changes8, work)
    check("seq 8 applies after the rejections", r8.applied)
    snapshot = (q(state, "SELECT * FROM recon_group_summary ORDER BY group_id"),
                q(state, "SELECT * FROM recon_batch ORDER BY seq"))
    r8b = apply(state, 8, changes8, work)   # 同字节重送
    check("identical bytes resend is idempotent (REUSED, no reapply)",
          r8b.reused and not r8b.applied
          and r8b.status_counts == r8.status_counts
          and snapshot == (q(state, "SELECT * FROM recon_group_summary "
                                   "ORDER BY group_id"),
                          q(state, "SELECT * FROM recon_batch ORDER BY seq")))
    try:
        apply(state, 8, [{"op": "upsert", "receipt_id": "R8",
                          "amount_units": 6, "links": []}], work)
        check("same seq with different bytes rejected", False, "accepted")
    except BatchSequenceError as exc:
        check("same seq with different bytes rejected",
              exc.code == "RECONCILE_BATCH_SEQUENCE", str(exc))

    # ---------------------------------------------------------------- #
    section("8. 撤销回单: 关联一并删除, 明细回到单边组")
    gid_y = group_of_receipt(state, "Y")
    r10 = apply(state, 9, [{"op": "delete", "receipt_id": "Y"}], work)
    check("deleted receipt no longer present anywhere",
          q(state, "SELECT COUNT(*) FROM recon_receipt WHERE receipt_id='Y'") == [(0,)]
          and q(state, "SELECT COUNT(*) FROM recon_member_groups "
                       "WHERE receipt_id='Y'") == [(0,)]
          and q(state, "SELECT COUNT(*) FROM recon_receipt_links "
                       "WHERE receipt_id='Y'") == [(0,)])
    check("detail B becomes a single-sided ONLY_DETAIL group of its own",
          summary_of(state, group_of_detail(state, *B))
          == [("ONLY_DETAIL", 0, 60_000_000, -60_000_000, 0, 1)])
    check("delete batch recompute log references the old group",
          any(row[0] == gid_y for row in logs(state, 9)), str(logs(state, 9)))
    compare_with_oracle(state, "after delete")

    # ---------------------------------------------------------------- #
    section("9. 大量无关小组: 只触及受影响组, 其余成员/汇总逐行不变")
    many = rfx.rebuild(work / "many")
    mstate = many["state"]
    # 初始化后已有 2 个明细组; 再建 300 个与账簿明细无关的回单组
    seed_seq = 1
    for i in range(0, 300, 100):
        lines = [{"op": "upsert", "receipt_id": f"NOISE-{j:03d}",
                  "amount_units": 1, "links": []} for j in range(i, i + 100)]
        apply(mstate, seed_seq, lines, work / "many")
        seed_seq += 1
    unrelated_groups = q(mstate, "SELECT group_id FROM recon_group_summary "
                                  "WHERE receipt_count=1 AND detail_count=0")
    check("300 noise groups created", len(unrelated_groups) == 300,
          str(len(unrelated_groups)))
    untouched_before = q(
        mstate, "SELECT g.group_id, g.status, g.receipt_total, g.detail_total, "
                "g.difference, g.receipt_count, g.detail_count, m.node_id "
                "FROM recon_group_summary g JOIN recon_member m USING (group_id) "
                "WHERE g.group_id IN (SELECT group_id FROM recon_group "
                "WHERE receipt_count=1 AND detail_count=0) "
                "ORDER BY g.group_id, m.node_id")
    scope_before = q(mstate, "SELECT COUNT(*) FROM recon_batch_scope_node")

    # 只改 A 组: X -> A(40)
    rn = apply(mstate, seed_seq, [
        {"op": "upsert", "receipt_id": "X", "amount_units": 40_000_000,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}]},
    ], work / "many")
    scope_rows = q(mstate, "SELECT node_id FROM recon_batch_scope_node "
                            "WHERE seq = ? ORDER BY node_id", [rn.seq])
    touched_members = q(
        mstate, "SELECT m.node_id FROM recon_member m WHERE m.node_id IN "
                "(SELECT node_id FROM recon_batch_scope_node WHERE seq=?) "
                "ORDER BY m.node_id", [rn.seq])
    check("scope contains only the touched detail + new receipt (2 nodes)",
          len(scope_rows) == 2, str(scope_rows))
    check("unrelated groups' members and summaries are byte-identical",
          q(mstate, "SELECT g.group_id, g.status, g.receipt_total, g.detail_total, "
                    "g.difference, g.receipt_count, g.detail_count, m.node_id "
                    "FROM recon_group_summary g JOIN recon_member m USING (group_id) "
                    "WHERE g.group_id IN (SELECT group_id FROM recon_group "
                    "WHERE receipt_count=1 AND detail_count=0) "
                    "ORDER BY g.group_id, m.node_id") == untouched_before)
    # 只有 A 的旧组受影响 (X 是新回单, 没有旧组); 300 个噪音组 + B 组不受影响
    check("recompute log counts only the one affected old group",
          rn.affected_old_group_count == 1 and rn.unaffected_group_count == 301,
          str(rn.as_dict()))
    check("affected old group is recorded with its reason and outcome",
          logs(mstate, rn.seq)
          and logs(mstate, rn.seq)[0][2] == "NEWLY_LINKED_DETAIL"
          and logs(mstate, rn.seq)[0][7] == 1,   # inserted: new receipt only
          str(logs(mstate, rn.seq)))
    check("scanned link rows for this batch is tiny (<= 4), not whole graph",
          rn.scanned_link_endpoints <= 4, str(rn.scanned_link_endpoints))
    check("scope never contains an unrelated receipt node",
          q(mstate, "SELECT COUNT(*) FROM recon_batch_scope_node s "
                    "JOIN recon_node n ON n.node_id=s.node_id "
                    "WHERE s.seq=? AND n.node_key LIKE 'NOISE-%'", [rn.seq])
          == [(0,)])
    compare_with_oracle(mstate, "after targeted batch among 300 noise groups")

    # 失败批次在大量无关组下也保持全库不变
    snapshot_before = (q(mstate, "SELECT * FROM recon_group_summary ORDER BY group_id"),
                       q(mstate, "SELECT node_id, group_id FROM recon_member "
                                 "ORDER BY node_id"))
    try:
        apply(mstate, rn.seq + 1, [
            {"op": "upsert", "receipt_id": "NOISE-000", "amount_units": 9,
             "links": []},
            {"op": "upsert", "receipt_id": "BAD", "amount_units": 1,
             "links": [{"txn_row_id": "GHOST", "cost_center": "CC_EXAMPLE"}]},
        ], work / "many")
        check("rejected batch leaves 300-group state untouched", False, "accepted")
    except ChangesValidationError:
        check("rejected batch leaves 300-group state untouched",
              (q(mstate, "SELECT * FROM recon_group_summary ORDER BY group_id"),
               q(mstate, "SELECT node_id, group_id FROM recon_member "
                         "ORDER BY node_id")) == snapshot_before)

    # ---------------------------------------------------------------- #
    section("10. 从一条记录查到它所在组和整组成员 (财务查询方式)")
    target = q(state, "SELECT group_id FROM recon_member_groups "
                      "WHERE member_kind='RECEIPT' AND receipt_id='P1'")[0][0]
    reached = q(state, """
        WITH me AS (SELECT group_id FROM recon_member_groups
                    WHERE member_kind='RECEIPT' AND receipt_id='P1')
        SELECT member_kind, COALESCE(receipt_id, ''),
               COALESCE(txn_row_id, ''), COALESCE(cost_center, ''), amount_units
        FROM recon_group_members
        WHERE group_id = (SELECT group_id FROM me)
        ORDER BY member_kind, 2, 3, 4""")
    members = q(state, "SELECT member_kind, COALESCE(receipt_id,''), "
                       "COALESCE(txn_row_id,''), COALESCE(cost_center,''), amount_units "
                       "FROM recon_group_members WHERE group_id = ? "
                       "ORDER BY member_kind, 2, 3, 4", [target])
    check("receipt -> group -> full member list works", reached == members
          and len(members) >= 3, str(members))
    check("member rows decode both sides (receipt id / detail key / amount)",
          any(m[0] == "RECEIPT" and m[1] and m[4] is not None for m in members)
          and any(m[0] == "DETAIL" and m[2] and m[3] and m[4] is not None
                  for m in members))
    totals = q(state, "SELECT SUM(CASE WHEN member_kind='RECEIPT' THEN amount_units "
                      "ELSE 0 END), SUM(CASE WHEN member_kind='DETAIL' "
                      "THEN amount_units ELSE 0 END) FROM recon_group_members "
                      "WHERE group_id = ?", [target])[0]
    summary = summary_of(state, target)[0]
    check("member totals agree with group summary",
          (totals[0], totals[1]) == (summary[1], summary[2]), f"{totals} {summary}")

    # ---------------------------------------------------------------- #
    section("11. CLI: 初始化 / 批次 / 错误退出码")
    cli = rfx.build_example_ledger(work / "cli", ledger_only=True)
    proc = subprocess.run(
        [sys.executable, "-I", "cli.py", "reconcile", "--state", str(cli["state"]),
         "--ledger", str(cli["ledger"])],
        capture_output=True, text=True, cwd=ROOT)
    check("cli init exit 0", proc.returncode == 0, proc.stderr[-400:])
    init_report = json.loads(proc.stdout) if proc.returncode == 0 else {}
    check("cli init reports version/detail count",
          init_report.get("status") == "INITIALIZED"
          and init_report.get("detail_count") == 2, proc.stdout[:400])
    proc = subprocess.run(
        [sys.executable, "-I", "cli.py", "reconcile", "--state", str(cli["state"]),
         "--ledger", str(cli["ledger"])],
        capture_output=True, text=True, cwd=ROOT)
    err = json.loads(proc.stdout) if proc.stdout.strip() else {}
    check("cli re-init exit 6 + RECONCILE_STATE_EXISTS",
          proc.returncode == 6 and err.get("error") == "RECONCILE_STATE_EXISTS",
          f"rc={proc.returncode} {proc.stdout[:200]}")
    changes = work / "cli" / "c1.jsonl"
    rfx.write_changes(changes, [
        {"op": "upsert", "receipt_id": "X", "amount_units": 40_000_000,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}]},
        {"op": "upsert", "receipt_id": "Y", "amount_units": 60_000_000,
         "links": [{"txn_row_id": "A", "cost_center": "CC_EXAMPLE"},
                   {"txn_row_id": "B", "cost_center": "CC_EXAMPLE"}]},
    ])
    proc = subprocess.run(
        [sys.executable, "-I", "cli.py", "reconcile", "--state", str(cli["state"]),
         "--seq", "1", "--changes", str(changes)],
        capture_output=True, text=True, cwd=ROOT)
    report = json.loads(proc.stdout) if proc.returncode == 0 else {}
    check("cli batch 1 exit 0 + APPLIED + matched group",
          proc.returncode == 0 and report.get("status") == "APPLIED"
          and report.get("status_counts") == {"MATCHED": 1},
          f"rc={proc.returncode} {proc.stdout[:400]}")
    bad = work / "cli" / "bad.jsonl"
    bad.write_text('{"op":"delete","receipt_id":"NOPE"}\n', encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, "-I", "cli.py", "reconcile", "--state", str(cli["state"]),
         "--seq", "2", "--changes", str(bad)],
        capture_output=True, text=True, cwd=ROOT)
    err = json.loads(proc.stdout) if proc.stdout.strip() else {}
    check("cli rejected batch exit 6 + RECONCILE_BATCH_REJECTED",
          proc.returncode == 6 and err.get("error") == "RECONCILE_BATCH_REJECTED",
          f"rc={proc.returncode} {proc.stdout[:200]}")

    # ---------------------------------------------------------------- #
    section("12. 题例直接可查的最终断言 (独立连接, 不经过 factasm)")
    con = duckdb.connect(str(state), read_only=True)
    try:
        two_groups = con.execute(
            "SELECT COUNT(*) FROM recon_group_summary WHERE status='MATCHED'").fetchone()
        member_index = con.execute(
            "SELECT group_id FROM recon_member_groups WHERE member_kind='DETAIL' "
            "AND txn_row_id='B'").fetchone()
        group_view = con.execute(
            "SELECT member_kind, receipt_id, txn_row_id, amount_units "
            "FROM recon_group_members WHERE group_id = ? "
            "ORDER BY member_kind", [member_index[0]]).fetchall()
    finally:
        con.close()
    check("matching groups visible to plain DuckDB",
          two_groups is not None and group_view, str(group_view))

    print(f"\n{'=' * 60}\nTOTAL: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        for name, detail in FAIL:
            print(f"  FAILED: {name} :: {detail}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
