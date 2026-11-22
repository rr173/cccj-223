"""可复跑演示: 造数据 -> 初始化 -> 两批回单 -> 直接查询拆组结果。

运行:
    cd fact_assembler
    python3 -I tests/reconcile_demo.py [输出目录]

输出目录默认 ``run_reconcile_demo`` (每次运行前清空, 不写入仓库)。
脚本最后会打印财务可以直接复用的 SQL; 全程不依赖网络或已有库文件。
"""
from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import bootstrap  # noqa: F401,E402

from factasm import reconcile_batch, verify_state  # noqa: E402
from tests import reconcile_fixtures as rfx  # noqa: E402

SQL_MEMBER_LOOKUP = """
-- 1) 从一条回单查它当前所在的组和整组成员
WITH me AS (
    SELECT group_id FROM recon_member_groups
    WHERE member_kind = 'RECEIPT' AND receipt_id = 'Y'
)
SELECT g.group_id, g.status, g.receipt_total, g.detail_total, g.difference,
       m.member_kind, COALESCE(m.receipt_id, ''), m.txn_row_id, m.cost_center,
       m.amount_units
FROM recon_group_members m
JOIN recon_group_summary g USING (group_id)
WHERE m.group_id = (SELECT group_id FROM me)
ORDER BY m.member_kind, m.receipt_id, m.txn_row_id;

-- 2) 从一条入账明细查它当前所在的组和整组成员
WITH me AS (
    SELECT group_id FROM recon_member_groups
    WHERE member_kind = 'DETAIL' AND txn_row_id = 'B'
      AND cost_center = 'CC_EXAMPLE'
)
SELECT m.* FROM recon_group_members m
WHERE m.group_id = (SELECT group_id FROM me);

-- 3) 所有仍有差额/单边的组
SELECT * FROM recon_group_summary
WHERE status <> 'MATCHED'
ORDER BY status, group_id;
"""


def main() -> int:
    work = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "run_reconcile_demo"
    if not work.is_absolute():
        work = ROOT / work
    fx = rfx.rebuild(work)
    state = fx["state"]
    print(f"state     : {state}")
    print(f"ledger    : {fx['ledger']}")
    print(f"version   : {fx['version_id']}")

    def batch(seq, lines):
        raw = "".join(__import__("json").dumps(l) + "\n" for l in lines).encode()
        path = work / f"batch-{seq}.jsonl"
        path.write_bytes(raw)
        result = reconcile_batch(state, seq=seq, changes_path=path, changes_bytes=raw)
        print(f"batch {seq} : {result.as_dict()}")
        return result

    A = {"txn_row_id": "A", "cost_center": "CC_EXAMPLE"}
    B = {"txn_row_id": "B", "cost_center": "CC_EXAMPLE"}
    batch(1, [
        {"op": "upsert", "receipt_id": "X", "amount_units": 40_000_000,
         "links": [A]},
        {"op": "upsert", "receipt_id": "Y", "amount_units": 60_000_000,
         "links": [A, B]},
    ])
    batch(2, [
        {"op": "upsert", "receipt_id": "Y", "amount_units": 60_000_000,
         "links": [B]},
    ])
    print(f"verify    : {verify_state(state)}")

    import duckdb

    con = duckdb.connect(str(state), read_only=True)
    try:
        print("\n--- recon_group_summary ---")
        for row in con.execute(
                "SELECT * FROM recon_group_summary ORDER BY group_id").fetchall():
            print(row)
        print("\n--- recon_group_members ---")
        for row in con.execute(
                "SELECT group_id, member_kind, receipt_id, txn_row_id, cost_center, "
                "amount_units, direct_peer_count FROM recon_group_members "
                "ORDER BY group_id, member_kind, receipt_id, txn_row_id").fetchall():
            print(row)
        print("\n--- recon_receipt_links ---")
        for row in con.execute(
                "SELECT * FROM recon_receipt_links ORDER BY receipt_id, txn_row_id"
        ).fetchall():
            print(row)
        print("\n--- recon_recompute_log (batch 2) ---")
        for row in con.execute(
                "SELECT * FROM recon_recompute_log WHERE seq = 2 "
                "ORDER BY old_group_id, new_group_id").fetchall():
            print(row)
    finally:
        con.close()

    print("\n可直接查询的 SQL:")
    print(SQL_MEMBER_LOOKUP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
