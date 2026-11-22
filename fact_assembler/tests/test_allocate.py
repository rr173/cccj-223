"""月结分摊 (allocate) 测试: 规则校验、合并份额、最大余数、交付保证。

运行: python3 -I tests/test_allocate.py

覆盖:
  1. 规则静态校验: 重复节点/边/合同入口、未声明节点、非法权重、
     出边基点之和、任意环 (含未被交易使用的部分)、节点 ID 字符集
  2. 两条分支汇入同一中心: 按合并份额入账, 每中心恰一行
  3. 最大余数法: 负额、零额、同余数按 ASCII 字典序、余数从大到小
  4. 跨版本原行 ID 重用: 账簿只含所选版本的行
  5. 反复分开再汇合的规则: 复杂度不随路径条数增长 (不枚举路径)
  6. 拒绝出账: 缺入口 / 循环规则 / 版本不存在 / 目标已存在, 且不留成品
  7. 合法空版本: 交付只有规则与版本标识的空账簿
  8. 账簿自足: 脱离事实库用纯 DuckDB 打开并逐笔对账
  9. 事实库只读: 分摊前后内容指纹不变
 10. CLI: 成功/失败出口码与错误 JSON
"""
from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import bootstrap  # noqa: F401,E402

from factasm import (  # noqa: E402
    FactAssembler,
    InvalidRulesError,
    LedgerExistsError,
    MissingRootEntryError,
    VersionNotFoundError,
    allocate,
    load_rules_file,
)
from factasm.allocation import AllocationRules, allocate_units  # noqa: E402
from factasm.db import connect_persistent  # noqa: E402
from factasm.sources import register_batch  # noqa: E402
from tests import fixtures_builder as fx  # noqa: E402
from fractions import Fraction  # noqa: E402

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


# --------------------------------------------------------------------------- #
# 夹具
# --------------------------------------------------------------------------- #
DIAMOND_RULES = {
    # POOL 6000/4000 分到 A/B; A 再 5000/5000; B 全额进 CC_WEST
    # => CC_EAST = 3/10, CC_WEST = 7/10 (两条分支汇入 CC_WEST)
    "nodes": ["POOL", "A", "B", "CC_EAST", "CC_WEST"],
    "edges": [
        {"from": "POOL", "to": "A", "bps": 6000},
        {"from": "POOL", "to": "B", "bps": 4000},
        {"from": "A", "to": "CC_EAST", "bps": 5000},
        {"from": "A", "to": "CC_WEST", "bps": 5000},
        {"from": "B", "to": "CC_WEST", "bps": 10000},
    ],
    "roots": {
        "contract:K-B1:K:r1": "POOL",
        "contract:K-B1:K:r2": "POOL",
    },
}

HALF_RULES = {
    # 50/50 两个中心, 用于同余数 (ASCII 字典序) 与负额/零额验证
    "nodes": ["POOL", "CC_A", "CC_B"],
    "edges": [
        {"from": "POOL", "to": "CC_A", "bps": 5000},
        {"from": "POOL", "to": "CC_B", "bps": 5000},
    ],
    "roots": {
        "contract:K-B1:K:r1": "POOL",
        "contract:K-B1:K:r2": "POOL",
    },
}

QUARTER_RULES = {
    # 25/25/50 三个中心, 验证余数从大到小
    "nodes": ["POOL", "CC_A", "CC_B", "CC_C"],
    "edges": [
        {"from": "POOL", "to": "CC_A", "bps": 2500},
        {"from": "POOL", "to": "CC_B", "bps": 2500},
        {"from": "POOL", "to": "CC_C", "bps": 5000},
    ],
    "roots": {
        "contract:K-B1:K:r1": "POOL",
        "contract:K-B1:K:r2": "POOL",
    },
}


def write_rules(path: pathlib.Path, rules) -> pathlib.Path:
    path.write_text(
        rules if isinstance(rules, str) else json.dumps(rules, ensure_ascii=False),
        encoding="utf-8",
    )
    return path


def write_txn_csv(path: pathlib.Path, batch_id: str, rows: list[dict]) -> pathlib.Path:
    """自定义交易批次 (列与 fixtures_builder 一致)。"""
    import csv

    header = ["txn_row_id", "event_time", "customer_key", "contract_key",
              "amount", "source_batch"]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        for r in rows:
            row = dict(r)
            row.setdefault("source_batch", batch_id)
            writer.writerow(row)
    return path


def assemble(db, cust, cont, txn):
    asm = FactAssembler(db)
    return asm.assemble(customer_batch=cust, contract_batch=cont,
                        transaction_batch=txn)


def ledger_query(path, sql, params=None):
    """用全新只读连接直接查询账簿 (不经过 factasm)。"""
    import duckdb

    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def db_fingerprint(db) -> dict:
    """事实库内容指纹: 版本指针 + 各表行数 + 文件字节哈希。"""
    import hashlib

    con = connect_persistent(db, read_only=True)
    try:
        current = con.execute(
            "SELECT version_id FROM current_version").fetchone()
        facts = con.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        quarantine = con.execute("SELECT COUNT(*) FROM quarantine").fetchone()[0]
        versions = con.execute("SELECT COUNT(*) FROM fact_versions").fetchone()[0]
    finally:
        con.close()
    digest = hashlib.sha256(pathlib.Path(db).read_bytes()).hexdigest()
    return {"current": current[0] if current else None, "facts": facts,
            "quarantine": quarantine, "versions": versions, "sha256": digest}


def expect_invalid(name, rules_text, fragment=""):
    try:
        AllocationRules.parse(rules_text)
        check(name, False, "rules accepted but must be invalid")
    except InvalidRulesError as exc:
        ok = fragment in str(exc) if fragment else True
        check(name, ok, f"{exc.code}: {exc.message}")


def layered_rules(depth: int, roots: dict) -> dict:
    """程序生成反复分开再汇合的规则: 每层 2 节点, 层间全连接 5000/5000,
    最终只有 SINK_A/SINK_B 两个入账中心。ENTRY 到任一中心的路径数为 2^(depth+1)。
    """
    nodes = ["ENTRY"]
    edges = []
    prev = ["ENTRY"]
    for i in range(depth):
        layer = [f"L{i}_0", f"L{i}_1"]
        nodes.extend(layer)
        for p in prev:
            for c in layer:
                edges.append({"from": p, "to": c, "bps": 5000})
        prev = layer
    nodes.extend(["SINK_A", "SINK_B"])
    for p in prev:
        edges.append({"from": p, "to": "SINK_A", "bps": 5000})
        edges.append({"from": p, "to": "SINK_B", "bps": 5000})
    return {"nodes": nodes, "edges": edges, "roots": roots}


def main() -> int:
    work = ROOT / "run_allocate"
    shutil.rmtree(work, ignore_errors=True)
    incoming = work / "incoming"
    incoming.mkdir(parents=True)
    db = work / "warehouse.duckdb"

    # ---------------------------------------------------------------- #
    section("1. 规则静态校验 (与版本是否使用无关)")
    rules = AllocationRules.parse(json.dumps(DIAMOND_RULES))
    check("diamond shares merged exactly",
          rules.shares_for_entry("POOL") == {"CC_EAST": Fraction(3, 10),
                                             "CC_WEST": Fraction(7, 10)},
          str(rules.shares_for_entry("POOL")))
    check("shares memoized per entry (no re-expansion)",
          rules.shares_for_entry("POOL") is rules.shares_for_entry("POOL"))
    check("cost centers are sinks only",
          rules.cost_centers == ["CC_EAST", "CC_WEST"], str(rules.cost_centers))

    direct = AllocationRules.parse(json.dumps({
        "nodes": ["CC_DIRECT"],
        "edges": [],
        "roots": {"g1": "CC_DIRECT"},
    }))
    check("sink node allowed as direct entry",
          direct.shares_for_entry("CC_DIRECT") == {"CC_DIRECT": Fraction(1)})

    expect_invalid("duplicate node rejected", json.dumps({
        "nodes": ["A", "A"], "edges": [], "roots": {}}), "duplicate node")
    expect_invalid("duplicate edge rejected", json.dumps({
        "nodes": ["A", "B"],
        "edges": [{"from": "A", "to": "B", "bps": 10000},
                  {"from": "A", "to": "B", "bps": 10000}],
        "roots": {}}), "duplicate edge")
    expect_invalid("duplicate contract root rejected",
                   '{"nodes":["A"],"edges":[],"roots":{"g1":"A","g1":"A"}}',
                   "duplicate key")
    expect_invalid("undeclared node in edge rejected", json.dumps({
        "nodes": ["A"], "edges": [{"from": "A", "to": "B", "bps": 10000}],
        "roots": {}}), "undeclared node")
    expect_invalid("undeclared node in root rejected", json.dumps({
        "nodes": ["A"], "edges": [], "roots": {"g1": "B"}}), "undeclared node")
    for bad_bps in ("0", "-5", "10001", "1.5", '"5000"', "true"):
        expect_invalid(f"illegal bps {bad_bps} rejected", json.dumps({
            "nodes": ["A", "B"],
            "edges": [{"from": "A", "to": "B", "bps": json.loads(bad_bps)}],
            "roots": {}}))
    expect_invalid("out bps sum != 10000 rejected", json.dumps({
        "nodes": ["A", "B", "C"],
        "edges": [{"from": "A", "to": "B", "bps": 3000},
                  {"from": "A", "to": "C", "bps": 6000}],
        "roots": {}}), "must be exactly 10000")
    expect_invalid("reachable cycle rejected", json.dumps({
        "nodes": ["A", "B"],
        "edges": [{"from": "A", "to": "B", "bps": 10000},
                  {"from": "B", "to": "A", "bps": 10000}],
        "roots": {"g1": "A"}}), "cycle")
    expect_invalid("unused cycle still rejected", json.dumps({
        "nodes": ["A", "B", "C"],
        "edges": [{"from": "B", "to": "C", "bps": 10000},
                  {"from": "C", "to": "B", "bps": 10000}],
        "roots": {"g1": "A"}}), "cycle")
    expect_invalid("self loop rejected", json.dumps({
        "nodes": ["A"], "edges": [{"from": "A", "to": "A", "bps": 10000}],
        "roots": {}}), "cycle")
    for bad_node in ('""', '"cc 1"', '"cc.1"', '"中心"', '"cc/1"', "5"):
        expect_invalid(f"bad node id {bad_node} rejected", json.dumps({
            "nodes": [json.loads(bad_node)], "edges": [], "roots": {}}))
    expect_invalid("unknown top-level key rejected", json.dumps({
        "nodes": [], "edges": [], "roots": {}, "extra": 1}), "unknown")
    expect_invalid("missing edges key rejected",
                   json.dumps({"nodes": [], "roots": {}}), "missing")
    expect_invalid("non-JSON rules rejected", "{not json", "JSON")

    # ---------------------------------------------------------------- #
    section("2. 准备事实库: 两个版本, 跨版本重用原行 ID TXN-001")
    paths = fx.build_fixtures(incoming)
    register_batch(db, batch_id=fx.CUSTOMER_BATCH, source_kind="customer",
                   path=paths["customers"])
    register_batch(db, batch_id=fx.CONTRACT_BATCH, source_kind="contract",
                   path=paths["contracts"])
    register_batch(db, batch_id=fx.TXN_BATCH, source_kind="transaction",
                   path=paths["transactions"])
    v1 = assemble(db, fx.CUSTOMER_BATCH, fx.CONTRACT_BATCH, fx.TXN_BATCH).version_id

    # 第二版: 同一客户/合同批次 + 新交易批次, 重用 TXN-001 身份但金额不同
    txn_v2 = write_txn_csv(incoming / "transactions_v2.csv", "T-B2", [
        dict(txn_row_id="TXN-001", event_time="2026-01-15 09:00:00",
             customer_key="C", contract_key="K", amount="999.000001"),
        dict(txn_row_id="TXN-NEG", event_time="2026-02-10 10:00:00",
             customer_key="C", contract_key="K", amount="-7.000003"),
        dict(txn_row_id="TXN-ZERO", event_time="2026-02-11 10:00:00",
             customer_key="C", contract_key="K", amount="0.000000"),
        dict(txn_row_id="TXN-ODD", event_time="2026-02-12 10:00:00",
             customer_key="C", contract_key="K", amount="1.000001"),
        dict(txn_row_id="TXN-ODD3", event_time="2026-02-13 10:00:00",
             customer_key="C", contract_key="K", amount="1.000003"),
        dict(txn_row_id="TXN-TINY", event_time="2026-02-14 10:00:00",
             customer_key="C", contract_key="K", amount="0.000001"),
    ])
    register_batch(db, batch_id="T-B2", source_kind="transaction", path=txn_v2)
    v2 = assemble(db, fx.CUSTOMER_BATCH, fx.CONTRACT_BATCH, "T-B2").version_id
    check("two versions published", v1 != v2 and v1 and v2, f"{v1} / {v2}")

    # ---------------------------------------------------------------- #
    section("3. 两条分支汇入同一中心: 按合并份额入账")
    rules_file = write_rules(work / "diamond.json", DIAMOND_RULES)
    out1 = work / "ledger-v1.duckdb"
    res = allocate(db, version_id=v1, rules=load_rules_file(rules_file),
                   out_path=out1)
    check("v1 ledger delivered", res.fact_count == 7 and res.allocation_count == 14,
          f"facts={res.fact_count} alloc={res.allocation_count}")

    rows = ledger_query(out1,
                        "SELECT cost_center, amount_units, share_numerator, "
                        "share_denominator FROM allocation WHERE txn_row_id = 'TXN-001' "
                        "ORDER BY cost_center")
    check("TXN-001 merged shares 3/10 + 7/10",
          rows == [("CC_EAST", 3000000, "3", "10"),
                   ("CC_WEST", 7000000, "7", "10")], str(rows))
    # 全部 7 笔逐笔对账: 明细和 = 控制数
    bad = ledger_query(out1, "SELECT txn_row_id, unallocated_units FROM reconciliation "
                             "WHERE unallocated_units <> 0")
    check("v1 reconciliation clean for all 7 txns", bad == [], str(bad))
    # 每个可达中心恰有一行
    dup = ledger_query(out1, "SELECT txn_row_id, cost_center, COUNT(*) c FROM allocation "
                             "GROUP BY 1, 2 HAVING c <> 1")
    check("exactly one row per (txn, center)", dup == [], str(dup))
    meta = ledger_query(out1, "SELECT version_id, transaction_batch_id, fact_count, "
                              "allocation_count, rules_sha256, rules_json FROM ledger_meta")
    check("meta carries version/batch/rules+digest",
          meta[0][0] == v1 and meta[0][1] == "T-B1" and meta[0][2] == 7
          and meta[0][3] == 14 and len(meta[0][4]) == 64
          and json.loads(meta[0][5])["roots"] == DIAMOND_RULES["roots"],
          str(meta))

    # ---------------------------------------------------------------- #
    section("4. 负额 / 零额 / 同余数 / 余数排序")
    out2 = work / "ledger-v2-half.duckdb"
    allocate(db, version_id=v2, rules=AllocationRules.parse(json.dumps(HALF_RULES)),
             out_path=out2)
    # 同余数: 1.000001 -> 1000001 单位, 两半各 500000.5, 余数相同,
    # 按中心 ID ASCII 字典序 CC_A 先得 1 单位
    odd = ledger_query(out2, "SELECT cost_center, amount_units FROM allocation "
                             "WHERE txn_row_id = 'TXN-ODD' ORDER BY cost_center")
    check("equal remainders -> ASCII order (CC_A gets the odd unit)",
          odd == [("CC_A", 500001), ("CC_B", 500000)], str(odd))
    neg = ledger_query(out2, "SELECT cost_center, amount_units FROM allocation "
                             "WHERE txn_row_id = 'TXN-NEG' ORDER BY cost_center")
    check("negative amount keeps sign after abs allocation",
          neg == [("CC_A", -3500002), ("CC_B", -3500001)], str(neg))
    zero = ledger_query(out2, "SELECT cost_center, amount_units FROM allocation "
                              "WHERE txn_row_id = 'TXN-ZERO' ORDER BY cost_center")
    check("zero amount still books one zero row per center",
          zero == [("CC_A", 0), ("CC_B", 0)], str(zero))
    tiny = ledger_query(out2, "SELECT cost_center, amount_units FROM allocation "
                              "WHERE txn_row_id = 'TXN-TINY' ORDER BY cost_center")
    check("tiny amount books zero row for losing center",
          tiny == [("CC_A", 1), ("CC_B", 0)] or tiny == [("CC_A", 0), ("CC_B", 1)],
          str(tiny))
    check("v2 reconciliation clean (negative/zero included)",
          ledger_query(out2, "SELECT COUNT(*) FROM reconciliation "
                             "WHERE unallocated_units <> 0") == [(0,)])

    out3 = work / "ledger-v2-quarter.duckdb"
    allocate(db, version_id=v2,
             rules=AllocationRules.parse(json.dumps(QUARTER_RULES)),
             out_path=out3)
    odd3 = ledger_query(out3, "SELECT cost_center, amount_units FROM allocation "
                              "WHERE txn_row_id = 'TXN-ODD3' ORDER BY cost_center")
    # 1000003 单位: 1/4 -> 250000.75 (x2), 1/2 -> 500001.5; 余 2 单位给 .75/.75
    check("remainders allocated largest-first",
          odd3 == [("CC_A", 250001), ("CC_B", 250001), ("CC_C", 500001)], str(odd3))

    # ---------------------------------------------------------------- #
    section("5. 跨版本原行 ID 重用: 账簿只含所选版本")
    c1 = ledger_query(out1, "SELECT amount_units FROM control WHERE txn_row_id = 'TXN-001'")
    out4 = work / "ledger-v2-diamond.duckdb"
    allocate(db, version_id=v2, rules=load_rules_file(rules_file), out_path=out4)
    c2 = ledger_query(out4, "SELECT amount_units FROM control WHERE txn_row_id = 'TXN-001'")
    check("same txn_row_id scoped per version",
          c1 == [(10000000,)] and c2 == [(999000001,)], f"v1={c1} v2={c2}")
    check("v2 ledger has only v2 facts",
          ledger_query(out4, "SELECT COUNT(*) FROM control") == [(6,)]
          and ledger_query(out4, "SELECT COUNT(DISTINCT txn_row_id) FROM control") == [(6,)])

    # ---------------------------------------------------------------- #
    section("6. 反复分开再汇合: 复杂度不随路径条数增长")
    depth = 50  # ENTRY 到任一中心的路径数 = 2^51, 枚举路径不可行
    big_rules = layered_rules(depth, {"contract:K-B1:K:r1": "ENTRY",
                                      "contract:K-B1:K:r2": "ENTRY"})
    t0 = time.monotonic()
    parsed = AllocationRules.parse(json.dumps(big_rules))
    parse_s = time.monotonic() - t0
    shares = parsed.shares_for_entry("ENTRY")
    check("layered rule shares exact 1/2 + 1/2",
          shares == {"SINK_A": Fraction(1, 2), "SINK_B": Fraction(1, 2)}, str(shares))
    check("only two booking centers in layered rule",
          parsed.cost_centers == ["SINK_A", "SINK_B"], str(parsed.cost_centers))
    t0 = time.monotonic()
    out5 = work / "ledger-v1-layered.duckdb"
    res5 = allocate(db, version_id=v1, rules=parsed, out_path=out5)
    alloc_s = time.monotonic() - t0
    check("2^51-path rule parsed+allocated fast",
          parse_s + alloc_s < 10.0, f"parse={parse_s:.2f}s alloc={alloc_s:.2f}s")
    check("layered ledger rows = 2 per txn",
          res5.allocation_count == 14, str(res5.allocation_count))
    check("layered reconciliation clean",
          ledger_query(out5, "SELECT COUNT(*) FROM reconciliation "
                             "WHERE unallocated_units <> 0") == [(0,)])

    # ---------------------------------------------------------------- #
    section("7. 拒绝出账: 缺入口 / 循环 / 版本不存在 / 目标已存在")
    stray = sorted(p.name for p in work.glob("*.tmp-*"))
    check("no temp residue before rejection tests", stray == [], str(stray))

    no_entry = dict(DIAMOND_RULES)
    no_entry["roots"] = {"contract:K-B1:K:r1": "POOL"}  # 缺 r2 入口
    out_missing = work / "should-not-exist-1.duckdb"
    try:
        allocate(db, version_id=v1,
                 rules=AllocationRules.parse(json.dumps(no_entry)),
                 out_path=out_missing)
        check("missing root entry rejected", False, "no exception")
    except MissingRootEntryError as exc:
        check("missing root entry rejected",
              exc.details["missing_roots"] == ["contract:K-B1:K:r2"], str(exc.details))
    check("missing entry leaves no target file", not out_missing.exists())

    cyclic = work / "cyclic.json"
    write_rules(cyclic, {
        "nodes": ["P", "Q", "CC"],
        "edges": [{"from": "P", "to": "Q", "bps": 10000},
                  {"from": "Q", "to": "P", "bps": 10000}],
        "roots": {"contract:K-B1:K:r1": "P", "contract:K-B1:K:r2": "P"},
    })
    out_cyclic = work / "should-not-exist-2.duckdb"
    try:
        allocate(db, version_id=v1, rules=load_rules_file(cyclic), out_path=out_cyclic)
        check("cyclic rules rejected", False, "no exception")
    except InvalidRulesError as exc:
        check("cyclic rules rejected", "cycle" in str(exc), str(exc))
    check("cyclic rules leave no target file", not out_cyclic.exists())

    out_nover = work / "should-not-exist-3.duckdb"
    try:
        allocate(db, version_id="ver-does-not-exist",
                 rules=load_rules_file(rules_file), out_path=out_nover)
        check("unknown version rejected", False, "no exception")
    except VersionNotFoundError:
        check("unknown version rejected", True)
    check("unknown version leaves no target file", not out_nover.exists())

    try:
        allocate(db, version_id=v1, rules=load_rules_file(rules_file), out_path=out1)
        check("existing target refused", False, "no exception")
    except LedgerExistsError:
        check("existing target refused", True)
    check("existing target untouched",
          ledger_query(out1, "SELECT fact_count FROM ledger_meta") == [(7,)])
    stray = sorted(p.name for p in work.glob("*.tmp-*"))
    check("no temp residue after rejections", stray == [], str(stray))

    # ---------------------------------------------------------------- #
    section("8. 合法空版本: 只有规则与版本标识的空账簿")
    empty_txn = incoming / "transactions_empty.csv"
    empty_txn.write_text(
        "txn_row_id,event_time,customer_key,contract_key,amount,source_batch\n",
        encoding="utf-8")
    register_batch(db, batch_id="T-EMPTY", source_kind="transaction", path=empty_txn)
    v_empty = assemble(db, fx.CUSTOMER_BATCH, fx.CONTRACT_BATCH, "T-EMPTY").version_id
    out_empty = work / "ledger-empty.duckdb"
    res_empty = allocate(db, version_id=v_empty, rules=load_rules_file(rules_file),
                         out_path=out_empty)
    empty_meta = ledger_query(out_empty, "SELECT version_id, transaction_batch_id, "
                                         "fact_count, allocation_count, rules_sha256 "
                                         "FROM ledger_meta")
    check("empty version delivers empty ledger with identity",
          res_empty.fact_count == 0
          and empty_meta[0][0] == v_empty and empty_meta[0][1] == "T-EMPTY"
          and empty_meta[0][2] == 0 and empty_meta[0][3] == 0
          and empty_meta[0][4] == res.rules_sha256,
          str(empty_meta))
    check("empty ledger has no control/allocation rows",
          ledger_query(out_empty, "SELECT COUNT(*) FROM control") == [(0,)]
          and ledger_query(out_empty, "SELECT COUNT(*) FROM allocation") == [(0,)])

    # ---------------------------------------------------------------- #
    section("9. 账簿自足: 脱离事实库用纯 DuckDB 逐笔对账")
    checker = work / "standalone_checker.py"
    checker.write_text(
        "import json, sys\n"
        "sys.path.insert(0, sys.argv[1])  # vendor/duckdb_pkg\n"
        "import duckdb\n"
        "con = duckdb.connect(sys.argv[2], read_only=True)\n"
        "meta = con.execute('SELECT version_id, fact_count FROM ledger_meta').fetchone()\n"
        "bad = con.execute('SELECT COUNT(*) FROM reconciliation WHERE unallocated_units <> 0').fetchone()[0]\n"
        "sql = ('SELECT COUNT(*) FROM (SELECT c.txn_row_id FROM control c '\n"
        "       'JOIN allocation a ON a.txn_row_id = c.txn_row_id '\n"
        "       'GROUP BY c.txn_row_id, c.amount_units '\n"
        "       'HAVING SUM(a.amount_units) <> c.amount_units)')\n"
        "sums = con.execute(sql).fetchone()[0]\n"
        "print(json.dumps({'version': meta[0], 'facts': meta[1],\n"
        "                  'recon_bad': bad, 'sum_bad': sums}))\n",
        encoding="utf-8")
    # 把事实库暂时改名, 证明账簿独立可开
    hidden = db.with_suffix(".hidden")
    db.rename(hidden)
    try:
        proc = subprocess.run(
            [sys.executable, "-I", str(checker), str(ROOT / "vendor" / "duckdb_pkg"),
             str(out1)],
            capture_output=True, text=True, cwd=work)
        check("ledger opens without fact db", proc.returncode == 0,
              proc.stderr[-500:])
        report = json.loads(proc.stdout) if proc.returncode == 0 else {}
        check("standalone reconciliation clean",
              report.get("recon_bad") == 0 and report.get("sum_bad") == 0
              and report.get("version") == v1 and report.get("facts") == 7,
              proc.stdout)
    finally:
        hidden.rename(db)

    # ---------------------------------------------------------------- #
    section("10. 事实库只读: 分摊前后内容指纹不变")
    fp10 = db_fingerprint(db)
    out10 = work / "ledger-readonly-check.duckdb"
    allocate(db, version_id=v2, rules=load_rules_file(rules_file), out_path=out10)
    try:
        allocate(db, version_id=v1,
                 rules=AllocationRules.parse(json.dumps(no_entry)),
                 out_path=work / "should-not-exist-4.duckdb")
    except MissingRootEntryError:
        pass
    fp11 = db_fingerprint(db)
    check("fact db unchanged by allocation (success + rejection)",
          fp10 == fp11, f"before={fp10} after={fp11}")

    # ---------------------------------------------------------------- #
    section("11. CLI: 出口码与错误 JSON")
    cli_out = work / "ledger-cli.duckdb"
    proc = subprocess.run(
        [sys.executable, "-I", "cli.py", "allocate", "--db", str(db),
         "--version", v1, "--rules", str(rules_file), "--out", str(cli_out)],
        capture_output=True, text=True, cwd=ROOT)
    check("cli allocate exit 0", proc.returncode == 0, proc.stderr[-500:])
    cli_report = json.loads(proc.stdout) if proc.returncode == 0 else {}
    check("cli report delivered",
          cli_report.get("status") == "DELIVERED"
          and cli_report.get("version_id") == v1
          and cli_report.get("fact_count") == 7,
          proc.stdout)

    proc = subprocess.run(
        [sys.executable, "-I", "cli.py", "allocate", "--db", str(db),
         "--version", v1, "--rules", str(cyclic), "--out", str(work / "x.duckdb")],
        capture_output=True, text=True, cwd=ROOT)
    cli_err = json.loads(proc.stdout) if proc.stdout.strip() else {}
    check("cli cyclic rules exit 5 + INVALID_ALLOCATION_RULES",
          proc.returncode == 5
          and cli_err.get("error") == "INVALID_ALLOCATION_RULES",
          f"rc={proc.returncode} out={proc.stdout[:300]}")
    check("cli failure leaves no target", not (work / "x.duckdb").exists())

    proc = subprocess.run(
        [sys.executable, "-I", "cli.py", "allocate", "--db", str(db),
         "--version", v1, "--rules", str(rules_file), "--out", str(cli_out)],
        capture_output=True, text=True, cwd=ROOT)
    cli_err = json.loads(proc.stdout) if proc.stdout.strip() else {}
    check("cli existing target exit 5 + LEDGER_EXISTS",
          proc.returncode == 5 and cli_err.get("error") == "LEDGER_EXISTS",
          f"rc={proc.returncode} out={proc.stdout[:300]}")

    # ---------------------------------------------------------------- #
    section("12. 分批读取: 小批大小结果一致")
    out6 = work / "ledger-v1-smallbatches.duckdb"
    allocate(db, version_id=v1, rules=load_rules_file(rules_file), out_path=out6,
             fact_batch_size=2)  # 7 笔事实分 4 批
    same = ledger_query(out6, "SELECT * FROM allocation ORDER BY 1, 2") == \
        ledger_query(out1, "SELECT * FROM allocation ORDER BY 1, 2")
    check("batched reading yields identical ledger", same)

    print(f"\n{'=' * 60}\nTOTAL: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        for name, detail in FAIL:
            print(f"  FAILED: {name} :: {detail}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
