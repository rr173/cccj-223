"""独立的全图重算预言机: 只读勾稽库, 用最朴素的方法重算全部组。

不 import 任何 factasm 代码, 也不复用台账里的分组结果:
  * 从 ``recon_ledger_detail`` 取固定入账侧 (明细键、金额);
  * 从 ``recon_receipt`` / ``recon_link`` 取现存回单与关联;
  * 对全图做并查集, 得到每个节点的组, 逐组求两侧合计与差额;
  * 返回 {(kind, key): group_key} 与 {group_key: {...}}。

group_key 用组内最小成员键的规范串表示, 只用于比对「谁与谁同组」, 与台账
分配的 group_id 无关。测试用它核对每个批次前后的成员与差额。
"""
from __future__ import annotations

import json


def recompute_all(state_path: str) -> dict:
    import sys

    # 允许从任意 cwd 调用: vendor 路径由调用方参数环境提供
    import duckdb

    con = duckdb.connect(str(state_path), read_only=True)
    try:
        details = {
            (txn, cc): int(units)
            for txn, cc, units in con.execute(
                "SELECT txn_row_id, cost_center, amount_units "
                "FROM recon_ledger_detail").fetchall()
        }
        receipts = {
            rid: int(units)
            for rid, units in con.execute(
                "SELECT receipt_id, amount_units FROM recon_receipt").fetchall()
        }
        links = con.execute(
            "SELECT rr.receipt_id, dn.txn_row_id, dn.cost_center "
            "FROM recon_link l "
            "JOIN recon_receipt rr ON rr.node_id = l.receipt_node_id "
            "JOIN recon_node dn ON dn.node_id = l.detail_node_id").fetchall()
    finally:
        con.close()

    nodes = [("RECEIPT", rid) for rid in receipts] + \
            [("DETAIL", k) for k in details]
    parent = {n: n for n in nodes}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            # 稳定合并: 按规范串大小, 保证可复跑结果一致
            if repr(ra) <= repr(rb):
                parent[rb] = ra
            else:
                parent[ra] = rb

    for rid, txn, cc in links:
        if ("RECEIPT", rid) in parent and ("DETAIL", (txn, cc)) in parent:
            union(("RECEIPT", rid), ("DETAIL", (txn, cc)))

    groups: dict = {}
    for node in nodes:
        groups.setdefault(find(node), []).append(node)

    result_groups: dict[str, dict] = {}
    node_to_group: dict[tuple, str] = {}
    for members in groups.values():
        gid = min(repr(m) for m in members)
        receipt_total = sum(receipts[m[1]] for m in members if m[0] == "RECEIPT")
        detail_total = sum(details[m[1]] for m in members if m[0] == "DETAIL")
        n_receipts = sum(1 for m in members if m[0] == "RECEIPT")
        n_details = sum(1 for m in members if m[0] == "DETAIL")
        difference = receipt_total - detail_total
        if n_receipts == 0:
            status = "ONLY_DETAIL"
        elif n_details == 0:
            status = "ONLY_RECEIPT"
        else:
            status = "MATCHED" if difference == 0 else "DIFFERENCE"
        result_groups[gid] = {
            "receipt_total": receipt_total,
            "detail_total": detail_total,
            "difference": difference,
            "receipt_count": n_receipts,
            "detail_count": n_details,
            "status": status,
            "receipts": sorted(m[1] for m in members if m[0] == "RECEIPT"),
            "details": sorted(m[1] for m in members if m[0] == "DETAIL"),
        }
        for m in members:
            node_to_group[m] = gid

    payload = {
        "node_to_group": {f"{kind}|{json.dumps(key, ensure_ascii=False)}": gid
                          for (kind, key), gid in node_to_group.items()},
        "groups": result_groups,
    }
    return payload


if __name__ == "__main__":
    import sys

    print(json.dumps(recompute_all(sys.argv[1]), indent=2, ensure_ascii=False))
