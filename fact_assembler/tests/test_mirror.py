"""离线事实镜像端到端测试 (可复跑小数据)。

运行: python3 -I tests/test_mirror.py

场景主线:
  1. 总部发布 v1/v2/v3/v4 四个版本 (每版带隔离行);
  2. 站点以空镜像为起点, 用 EMPTY -> v1 的包首次装入旧版;
  3. 跳过 v2, 用 v1 -> v3 的包切到目标版 (含真实删除、百万分之一金额更正、
     区间末端 NULL -> 具体时间、整批未变行以及落在无关分区的未变行);
  4. 直接查询总部库 v3 的投影列核对镜像 (独立预言机, 不用差量实现生成预期);
  5. 构造「少一笔删除但沿用原目标指纹」的包, 确认落地后核对失败且版本/数据不变;
  6. 分区局部性: 用镜像内的 touch/catalog 证据断言实际读/写了哪些分区,
     未受影响分区既不被扫描也不被重写 (不是打印预期范围, 也不用耗时替代);
  7. 零行变更包 (v3 -> v4) 也能切版; 起点不符/无效包/重复投递旧包一律拒绝且不前进;
  8. 首次失败不留可冒充已同步的镜像。
"""
from __future__ import annotations

import json
import pathlib
import shutil
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import bootstrap  # noqa: F401,E402

import duckdb  # noqa: E402

from factasm import (  # noqa: E402
    EMPTY_BASE,
    FactAssembler,
    MirrorBaseMismatchError,
    MirrorInvalidBaseError,
    MirrorPackageInvalidError,
    MirrorStateInvalidError,
    MirrorTargetCheckError,
    MirrorVersionNotFoundError,
    apply_delta,
    canonical_row_line,
    export_delta,
    load_package,
    mirror_status,
    partition_of,
)
from factasm.mirror import empty_content_fingerprint  # noqa: E402
from factasm.db import connect_persistent  # noqa: E402
from factasm.sources import register_batch  # noqa: E402
from tests import mirror_fixtures as mfx  # noqa: E402
from tests.mirror_independent import (  # noqa: E402
    PROJECTION_COLUMNS,
    oracle_fingerprint_of_rows,
    oracle_partition,
    oracle_projection_from_warehouse,
    oracle_row_line,
)

PASS: list[str] = []
FAIL: list[tuple[str, str]] = []
EMPTY_FINGERPRINT = empty_content_fingerprint()


def check(name, cond, detail="") -> None:
    if cond:
        PASS.append(name)
        print(f"  [PASS] {name}")
    else:
        FAIL.append((name, detail))
        print(f"  [FAIL] {name} :: {detail}")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #
def build_warehouse(work: pathlib.Path) -> tuple[pathlib.Path, FactAssembler, list[str]]:
    incoming = work / "incoming"
    db = work / "hq.duckdb"
    paths = mfx.build_mirror_fixtures(incoming)
    asm = FactAssembler(db)
    # 维度批次四版共享, 只登记一次
    register_batch(db, batch_id=mfx.C_STABLE, source_kind="customer",
                   path=paths[f"customers_{mfx.C_STABLE}"])
    register_batch(db, batch_id=mfx.K_STABLE, source_kind="contract",
                   path=paths[f"contracts_{mfx.K_STABLE}"])
    version_ids: list[str] = []
    for c_batch, k_batch, t_batch in mfx.VERSION_BATCHES:
        register_batch(db, batch_id=t_batch, source_kind="transaction",
                       path=paths[f"transactions_{t_batch}"])
        result = asm.assemble(customer_batch=c_batch, contract_batch=k_batch,
                              transaction_batch=t_batch)
        version_ids.append(result.version_id)
    return db, asm, version_ids


def catalog_snapshot(mirror_path: pathlib.Path) -> dict[int, tuple[int, str]]:
    con = duckdb.connect(str(mirror_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT partition_id, row_count, partition_digest FROM mirror_partition_catalog "
            "ORDER BY partition_id"
        ).fetchall()
    finally:
        con.close()
    return {pid: (row_count, digest) for pid, row_count, digest in rows}


def mirror_rows(mirror_path: pathlib.Path) -> dict[str, dict]:
    con = duckdb.connect(str(mirror_path), read_only=True)
    try:
        cols = ", ".join(PROJECTION_COLUMNS)
        cur = con.execute(f"SELECT {cols} FROM mirror_fact_projection ORDER BY txn_row_id")
        return {r[0]: dict(zip(PROJECTION_COLUMNS, r)) for r in cur.fetchall()}
    finally:
        con.close()


def touch_rows(mirror_path: pathlib.Path) -> list[tuple]:
    con = duckdb.connect(str(mirror_path), read_only=True)
    try:
        return con.execute(
            "SELECT apply_id, partition_id, rows_read, rows_inserted, rows_updated, rows_deleted "
            "FROM mirror_apply_partition_touch ORDER BY partition_id"
        ).fetchall()
    finally:
        con.close()


def tamper_drop_one_delete(package_path: pathlib.Path, dst: pathlib.Path,
                           drop_key: str) -> dict:
    """构造少一笔删除的包: 删掉 delete 变更并同步分区声明使结构仍然有效,
    但原样保留目标版本与导出端算出的目标指纹 (绝不从剩余操作反推预期)。
    这样包能通过落地前核对, 却必然在落地后内容核对中暴露漏删。"""
    pkg = json.loads(package_path.read_text(encoding="utf-8"))
    before = len(pkg["changes"])
    removed = next(c for c in pkg["changes"]
                   if c["op"] == "delete" and c["txn_row_id"] == drop_key)
    pkg["changes"] = [c for c in pkg["changes"] if c is not removed]
    # 仅修正声明使结构校验通过 (声明本来就必须服从按键推导); 指纹一律不动
    derived = sorted({partition_of(c["txn_row_id"]) for c in pkg["changes"]})
    pkg["declared_touched_partitions"] = derived
    assert len(pkg["changes"]) == before - 1
    dst.write_text(json.dumps(pkg, ensure_ascii=False, indent=2), encoding="utf-8")
    return pkg


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #
def main() -> int:
    work = ROOT / "run_mirror"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    pkg_dir = work / "packages"
    pkg_dir.mkdir()
    mirror_path = work / "site_mirror.duckdb"

    # ---------------------------------------------------------------- #
    section("1. 总部发布四个版本, 每版事实与隔离分离")
    db, asm, versions = build_warehouse(work)
    v1, v2, v3, v4 = versions
    check("four versions published", len(versions) == 4, str(versions))
    hq = connect_persistent(db, read_only=True)
    try:
        q_counts = {
            vid: hq.execute("SELECT COUNT(*) FROM quarantine WHERE version_id = ?", [vid]).fetchone()[0]
            for vid in versions
        }
        f_counts = {
            vid: hq.execute("SELECT COUNT(*) FROM facts WHERE version_id = ?", [vid]).fetchone()[0]
            for vid in versions
        }
    finally:
        hq.close()
    check("every version has quarantined rows", all(n >= 1 for n in q_counts.values()), str(q_counts))
    # v1=6 事实 (7 交易-1 隔离), v2=8 (9-1), v3/v4=5 (6-1)
    check("fact counts v1/v2/v3/v4",
          [f_counts[v] for v in versions] == [6, 8, 5, 5], str(f_counts))
    # 独立预言机直接查总部库三个版本的投影
    hq = connect_persistent(db, read_only=True)
    try:
        rows_v1 = oracle_projection_from_warehouse(hq, v1)
        rows_v3 = oracle_projection_from_warehouse(hq, v3)
        rows_v4 = oracle_projection_from_warehouse(hq, v4)
        fp_v1, digests_v1, counts_v1 = oracle_fingerprint_of_rows(rows_v1)
        fp_v3, digests_v3, counts_v3 = oracle_fingerprint_of_rows(rows_v3)
        fp_v4, _, _ = oracle_fingerprint_of_rows(rows_v4)
    finally:
        hq.close()

    # ---------------------------------------------------------------- #
    section("2. 导出参数校验: 版本必须存在且起点 != 目标, 不从当前版本补数")
    try:
        export_delta(db, base_version="ver-nope", target_version=v1,
                     out_path=pkg_dir / "x.json")
        check("unknown base version rejected", False, "no error")
    except MirrorVersionNotFoundError:
        check("unknown base version rejected", True)
    try:
        export_delta(db, base_version=v3, target_version=v3,
                     out_path=pkg_dir / "y.json")
        check("same base/target rejected", False, "no error")
    except MirrorInvalidBaseError:
        check("same base/target rejected", True)
    try:
        export_delta(db, base_version=EMPTY_BASE, target_version=EMPTY_BASE,
                     out_path=pkg_dir / "z.json")
        check("EMPTY target rejected", False, "no error")
    except MirrorInvalidBaseError:
        check("EMPTY target rejected", True)

    # ---------------------------------------------------------------- #
    section("3. 首次收数: EMPTY -> v1, 以空镜像为起点走同一套流程")
    p_empty_v1 = pkg_dir / "delta_empty_v1.json"
    export1 = export_delta(db, base_version=EMPTY_BASE, target_version=v1,
                           out_path=p_empty_v1)
    check("bootstrap package upserts every v1 fact, no deletes",
          export1.upsert_count == f_counts[v1] and export1.delete_count == 0,
          export1.as_dict())
    check("bootstrap package base fingerprint is the empty fingerprint",
          export1.base_fingerprint == EMPTY_FINGERPRINT)
    check("mirror file does not exist before first apply", not mirror_path.exists())
    r1 = apply_delta(mirror_path, p_empty_v1)
    check("mirror created on first apply", mirror_path.exists())
    check("bootstrap flag set", r1.bootstrap is True)
    check("mirror current version is v1", mirror_status(mirror_path)["current_version"] == v1)
    mirror_v1 = mirror_rows(mirror_path)
    check("mirror row count equals v1 facts", len(mirror_v1) == f_counts[v1],
          f"{len(mirror_v1)} vs {f_counts[v1]}")
    check("mirror v1 content equals warehouse v1 projection (oracle)",
          mirror_v1 == rows_v1)
    check("mirror fingerprint matches oracle v1 fingerprint",
          mirror_status(mirror_path)["content_fingerprint"] == fp_v1)
    check("open interval ends stay SQL NULL in mirror",
          all(mirror_v1[k]["customer_valid_to"] is None or
              mirror_v1[k]["contract_valid_to"] is None for k in mirror_v1) is True
          and any(mirror_v1[k]["contract_valid_to"] is None for k in mirror_v1))
    sample_amount = next(iter(mirror_v1.values()))["amount"]
    check("amount keeps DECIMAL(28,6) scale", str(sample_amount) == f"{sample_amount}"
          and sample_amount.as_tuple().exponent == -6, str(sample_amount))

    # 首次失败不能留下可冒充已同步的镜像
    p_v1_v2 = pkg_dir / "delta_v1_v2.json"
    export_delta(db, base_version=v1, target_version=v2, out_path=p_v1_v2)
    bogus_mirror = work / "bootstrap_fail.duckdb"
    try:
        apply_delta(bogus_mirror, p_v1_v2)  # 非 EMPTY 包不能建镜像
        check("non-empty package cannot bootstrap", False, "no error")
    except MirrorBaseMismatchError:
        check("non-empty package cannot bootstrap", True)
    check("failed bootstrap leaves no mirror file", not bogus_mirror.exists())
    check("no temp files left after failed bootstrap",
          not list(work.glob("bootstrap_fail.duckdb.tmp-*")))

    # ---------------------------------------------------------------- #
    section("4. 跨版本差量 v1 -> v3 (跳过 v2): 删除/金额/区间/未变/复用 全覆盖")
    p_v1_v3 = pkg_dir / "delta_v1_v3.json"
    export3 = export_delta(db, base_version=v1, target_version=v3, out_path=p_v1_v3)
    pkg3 = load_package(p_v1_v3)
    changes = pkg3["_normalized_changes"]
    by_key = {c["txn_row_id"]: c for c in changes}
    check("two real deletes emitted with explicit keys",
          sorted(c["txn_row_id"] for c in changes if c["op"] == "delete")
          == ["TXN-DEL-01", "TXN-DEL-02"])
    check("cross-middle-version delete (MID) is not in v1->v3 package",
          "TXN-MID-01" not in by_key, "MID only exists in skipped v2")
    check("amount change of 1e-6 is one upsert",
          by_key["TXN-AMT-01"]["op"] == "upsert"
          and str(by_key["TXN-AMT-01"]["row"]["amount"]) == "100.000001")
    check("new key NEW-01 is an upsert", by_key["TXN-NEW-01"]["op"] == "upsert")
    check("unchanged keepers have no change body",
          "TXN-KEEP-01" not in by_key and "TXN-KEEP-02" not in by_key
          and "TXN-REUSE-01" not in by_key)
    # 行 ID 复用但投影未变: 直接对比总部库 v1/v3 该行
    check("reused row id projection is identical across v1/v3",
          rows_v1["TXN-REUSE-01"] == rows_v3["TXN-REUSE-01"])
    # 区间末端 NULL -> 具体时间的更正落在 NEW-01 (v2 挂开放 K2O, v3 改挂封口 K2C)
    new_row = rows_v3["TXN-NEW-01"]
    check("interval end corrected NULL -> concrete timestamp",
          new_row["contract_valid_to"] is not None
          and new_row["contract_valid_to"].year == 2026
          and (new_row["contract_valid_to"].month, new_row["contract_valid_to"].day) == (6, 1)
          and new_row["contract_key"] == "K2C",
          str(new_row["contract_valid_to"]))
    # 同一行在 v2 时末端还是 NULL
    hq2 = connect_persistent(db, read_only=True)
    try:
        v2_rows = oracle_projection_from_warehouse(hq2, v2)
    finally:
        hq2.close()
    check("same key had an open (NULL) interval end in v2",
          v2_rows["TXN-NEW-01"]["contract_valid_to"] is None)
    # 分区声明必须等于从键推导 (删除也计入)
    derived = sorted({partition_of(c["txn_row_id"]) for c in changes})
    check("declared partitions equal key-derived partitions",
          pkg3["declared_touched_partitions"] == derived,
          f"{pkg3['declared_touched_partitions']} vs {derived}")
    check("package fingerprints equal independent oracle fingerprints",
          export3.base_fingerprint == fp_v1 and export3.target_fingerprint == fp_v3)
    # 隔离行绝不进包
    check("quarantine ids never appear in package",
          all(not c["txn_row_id"].startswith("TXN-Q") for c in changes))

    # ---------------------------------------------------------------- #
    section("5. 应用 v1 -> v3 并直接用总部库 v3 投影核对镜像")
    catalog_before = catalog_snapshot(mirror_path)
    r3 = apply_delta(mirror_path, p_v1_v3)
    catalog_after = catalog_snapshot(mirror_path)
    check("mirror advanced to v3", mirror_status(mirror_path)["current_version"] == v3)
    mirror_v3 = mirror_rows(mirror_path)
    check("mirror v3 content equals warehouse v3 projection (oracle, no diff impl)",
          mirror_v3 == rows_v3)
    check("mirror v3 fingerprint equals oracle v3 fingerprint",
          mirror_status(mirror_path)["content_fingerprint"] == fp_v3)
    check("deleted keys are gone from mirror",
          "TXN-DEL-01" not in mirror_v3 and "TXN-DEL-02" not in mirror_v3)
    check("1e-6 amount correction physically present",
          str(mirror_v3["TXN-AMT-01"]["amount"]) == "100.000001")
    check("NULL interval ends still NULL where open",
          mirror_v3["TXN-KEEP-02"]["contract_valid_to"] is None)
    check("quarantine rows never materialized in mirror",
          all(not k.startswith("TXN-Q") for k in mirror_v3))

    # ---------------------------------------------------------------- #
    section("6. 分区局部性证据: 实际读/写只发生在受影响分区")
    changed_partitions = set(derived)
    unaffected = [pid for pid in range(256) if pid not in changed_partitions]
    touched = touch_rows(mirror_path)
    latest_apply = mirror_status(mirror_path)["last_apply"]["apply_id"]
    touched_latest = [t for t in touched if t[0] == latest_apply]
    touched_pids = {t[1] for t in touched_latest}
    check("touch evidence covers exactly the key-derived partitions",
          touched_pids == changed_partitions,
          f"touched={sorted(touched_pids)} derived={sorted(changed_partitions)}")
    # 未受影响分区: 摘要目录逐行不变 (行数与摘要都没被重写)
    rewritten_catalog = [pid for pid in unaffected
                         if catalog_before[pid] != catalog_after[pid]]
    check("unaffected partition catalog rows are byte-for-byte unchanged",
          not rewritten_catalog, f"rewritten={rewritten_catalog[:5]}")
    # 受影响分区的读/写计数与操作语义一致
    stats = {t[1]: {"read": t[2], "ins": t[3], "upd": t[4], "del": t[5]}
             for t in touched_latest}
    total_del = sum(s["del"] for s in stats.values())
    total_ins = sum(s["ins"] for s in stats.values())
    total_upd = sum(s["upd"] for s in stats.values())
    check("delete counted in affected partition stats", total_del == 2, str(total_del))
    # 期望值直接由包内操作 + v1 镜像现有键推导 (不硬编码分区号/计数):
    # 用应用前的 v1 内容 (catalog_before 取自同一时刻) 判断每笔 upsert 是新增还是替换
    replay_v1 = work / "expect_v1.duckdb"
    apply_delta(replay_v1, p_empty_v1)
    v1_key_set = set(mirror_rows(replay_v1))
    expected_ins = sum(1 for c in changes if c["op"] == "upsert"
                       and c["txn_row_id"] not in v1_key_set)
    expected_upd = sum(1 for c in changes if c["op"] == "upsert"
                       and c["txn_row_id"] in v1_key_set)
    check("insert/update counts match package ops against v1 keys",
          total_ins == expected_ins and total_upd == expected_upd,
          f"got ins={total_ins} upd={total_upd}, want ins={expected_ins} upd={expected_upd}")
    # 每个受影响分区都按区内现有键做了定位读取 (新增分区无现存行, read=0 是正确的)
    insert_only_pids = {pid for pid, s in stats.items() if s["ins"] == 1}
    check("insert-only partition had no pre-existing rows to read",
          all(stats[pid]["read"] == 0 for pid in insert_only_pids), str(stats))
    check("every partition with existing keys was actually re-read",
          all(s["read"] >= 1 for pid, s in stats.items() if pid not in insert_only_pids),
          str(stats))
    # 定位读取只命中受影响键: 每区 rows_read 不超过该区变更键数
    changes_by_pid: dict[int, int] = {}
    for c in changes:
        changes_by_pid[c["partition_id"]] = changes_by_pid.get(c["partition_id"], 0) + 1
    check("no full partition scans: rows_read bounded by changed keys",
          all(stats[pid]["read"] <= changes_by_pid[pid] for pid in stats), str(stats))
    check("catalog row counts match oracle partition counts after apply",
          all(catalog_after[pid][0] == counts_v3[pid] for pid in range(256)),
          "partition row counts diverge")
    check("catalog digests match oracle v3 partition digests",
          all(catalog_after[pid][1] == digests_v3[pid] for pid in range(256)))
    check("no touch rows exist for unaffected partitions",
          all(t[1] in changed_partitions for t in touched_latest))
    # 明确验证「落在完全无关分区的未变行」: KEEP-02 的分区不在受影响集合里,
    # 且该分区摘要目录行应用前后逐字节不变
    keep02_pid = partition_of("TXN-KEEP-02")
    check("unchanged keeper sits in a fully unrelated partition",
          keep02_pid not in changed_partitions
          and catalog_before[keep02_pid] == catalog_after[keep02_pid]
          and catalog_after[keep02_pid][0] == 1,
          f"pid={keep02_pid} changed={sorted(changed_partitions)}")

    # ---------------------------------------------------------------- #
    section("7. 少一笔删除但沿用原目标指纹 -> 落地后核对失败, 版本/数据不变")
    tampered = pkg_dir / "delta_v1_v3_missing_delete.json"
    tamper_drop_one_delete(p_v1_v3, tampered, "TXN-DEL-01")
    # 在 v1 镜像副本上重放: 先建一个停在 v1 的镜像
    v1_mirror = work / "site_v1_replay.duckdb"
    apply_delta(v1_mirror, p_empty_v1)
    fp_before = mirror_status(v1_mirror)["content_fingerprint"]
    rows_before = mirror_rows(v1_mirror)
    cat_before = catalog_snapshot(v1_mirror)
    try:
        apply_delta(v1_mirror, tampered)
        check("tampered package rejected", False, "no error")
    except MirrorTargetCheckError as exc:
        check("tampered package rejected by target check", True, exc.code)
    status = mirror_status(v1_mirror)
    check("version did not advance after failed target check",
          status["current_version"] == v1)
    check("fingerprint unchanged after failed target check",
          status["content_fingerprint"] == fp_before)
    check("mirror rows unchanged after failed target check",
          mirror_rows(v1_mirror) == rows_before)
    check("catalog unchanged after failed target check",
          catalog_snapshot(v1_mirror) == cat_before)
    # 漏删的键确实仍在 (证明不是悄悄放过)
    check("would-be-deleted key still present in untouched mirror",
          "TXN-DEL-01" in mirror_rows(v1_mirror))

    # ---------------------------------------------------------------- #
    section("8. 起点不符: 重复投递旧包 / 指纹不符 / 无效包, 镜像不前进")
    # 对已在 v3 的镜像重复投递 v1->v3 旧包
    fp_v3_now = mirror_status(mirror_path)["content_fingerprint"]
    try:
        apply_delta(mirror_path, p_v1_v3)
        check("stale package re-delivery rejected", False, "no error")
    except MirrorBaseMismatchError as exc:
        check("stale package re-delivery rejected", True, exc.code)
    check("mirror still at v3 after stale delivery",
          mirror_status(mirror_path)["current_version"] == v3
          and mirror_status(mirror_path)["content_fingerprint"] == fp_v3_now)
    # 版本 ID 对但内容指纹被改坏
    bad_fp = pkg_dir / "delta_bad_base_fp.json"
    pkg_bad = json.loads(p_v1_v3.read_text(encoding="utf-8"))
    pkg_bad["base_content_fingerprint"] = "0" * 64
    bad_fp.write_text(json.dumps(pkg_bad), encoding="utf-8")
    try:
        load_package(bad_fp)
        check("internally inconsistent fingerprint package invalid", False, "accepted")
    except MirrorPackageInvalidError:
        check("internally inconsistent fingerprint package invalid", True)
    # 无效包: 坏 JSON / 未知键 / 缺键删除 / 声明分区与键不符
    invalid_dir = pkg_dir / "invalid"
    invalid_dir.mkdir(exist_ok=True)
    (invalid_dir / "broken.json").write_text("{not json", encoding="utf-8")
    good = json.loads(p_v1_v3.read_text(encoding="utf-8"))
    unknown = dict(good); unknown["extra"] = 1
    (invalid_dir / "unknown_key.json").write_text(json.dumps(unknown), encoding="utf-8")
    no_key = dict(good); no_key["changes"] = [{"op": "delete", "partition_id": 1}]
    (invalid_dir / "missing_key.json").write_text(json.dumps(no_key), encoding="utf-8")
    wrong_pid = json.loads(p_v1_v3.read_text(encoding="utf-8"))
    wrong_pid["declared_touched_partitions"] = [0]
    (invalid_dir / "wrong_partition.json").write_text(json.dumps(wrong_pid), encoding="utf-8")
    for name in ("broken.json", "unknown_key.json", "missing_key.json", "wrong_partition.json"):
        try:
            apply_delta(mirror_path, invalid_dir / name)
            check(f"invalid package {name} rejected", False, "accepted")
        except (MirrorPackageInvalidError, MirrorBaseMismatchError) as exc:
            check(f"invalid package {name} rejected ({exc.code})", True)
    check("mirror still at v3 after all invalid packages",
          mirror_status(mirror_path)["current_version"] == v3)
    # 对非镜像库应用 -> 状态无效
    other_db = work / "not_a_mirror.duckdb"
    shutil.copy(db, other_db)
    try:
        apply_delta(other_db, p_v1_v3)
        check("non-mirror database rejected", False, "accepted")
    except MirrorStateInvalidError:
        check("non-mirror database rejected", True)

    # 删除必须命中起点镜像里明确存在的键, 否则是无效包且镜像不前进
    ghost = pkg_dir / "delta_ghost_delete.json"
    ghost_pkg = json.loads(p_v1_v3.read_text(encoding="utf-8"))
    ghost_pkg["changes"].append({"op": "delete", "txn_row_id": "TXN-GHOST-99",
                                 "partition_id": partition_of("TXN-GHOST-99")})
    ghost_pid_set = set(ghost_pkg["declared_touched_partitions"])
    ghost_pid_set.add(partition_of("TXN-GHOST-99"))
    ghost_pkg["declared_touched_partitions"] = sorted(ghost_pid_set)
    ghost.write_text(json.dumps(ghost_pkg), encoding="utf-8")
    try:
        apply_delta(v1_mirror, ghost)
        check("delete of a key absent from base rejected", False, "accepted")
    except MirrorPackageInvalidError as exc:
        check("delete of a key absent from base rejected", True, exc.code)
    check("mirror unchanged after ghost-delete rejection",
          mirror_status(v1_mirror)["current_version"] == v1
          and mirror_rows(v1_mirror) == rows_before)

    # EMPTY 起点的包里若混入删除, 不能建镜像, 目标路径无文件
    empty_with_delete = pkg_dir / "empty_with_delete.json"
    ewd = json.loads(p_empty_v1.read_text(encoding="utf-8"))
    ewd["changes"].append({"op": "delete", "txn_row_id": "TXN-GHOST-99",
                           "partition_id": partition_of("TXN-GHOST-99")})
    ewd_pids = set(ewd["declared_touched_partitions"])
    ewd_pids.add(partition_of("TXN-GHOST-99"))
    ewd["declared_touched_partitions"] = sorted(ewd_pids)
    empty_with_delete.write_text(json.dumps(ewd), encoding="utf-8")
    doomed = work / "doomed_bootstrap.duckdb"
    try:
        apply_delta(doomed, empty_with_delete)
        check("EMPTY package containing a delete cannot bootstrap", False, "accepted")
    except MirrorPackageInvalidError:
        check("EMPTY package containing a delete cannot bootstrap", True)
    check("failed bootstrap created no mirror or temp file",
            not doomed.exists() and not list(work.glob("doomed_bootstrap.duckdb.tmp-*")))

    # ---------------------------------------------------------------- #
    section("9. 零行变更包 v3 -> v4: 投影完全相同也能切版")
    p_v3_v4 = pkg_dir / "delta_v3_v4.json"
    export4 = export_delta(db, base_version=v3, target_version=v4, out_path=p_v3_v4)
    check("no-op package has zero row changes",
          export4.change_count == 0 and export4.touched_partitions == [])
    check("v3/v4 projections identical but versions differ", fp_v3 != fp_v4 or v3 != v4)
    check("v3/v4 content fingerprints equal (same logical content)", fp_v3 == fp_v4)
    r4 = apply_delta(mirror_path, p_v3_v4)
    check("mirror switched to v4 with no data changes",
          mirror_status(mirror_path)["current_version"] == v4
          and r4.upsert_count == 0 and r4.delete_count == 0
          and r4.touched_partitions == [])
    check("mirror content still equals warehouse v3/v4 projection",
          mirror_rows(mirror_path) == rows_v4 == rows_v3)

    # ---------------------------------------------------------------- #
    section("10. 指纹定义: 行序/物理布局无关, 逻辑相同则指纹相同")
    # 重新走一遍 EMPTY->v1 建第二个镜像, 与首个 v1 镜像指纹必须一致
    mirror_again = work / "site_mirror_again.duckdb"
    apply_delta(mirror_again, p_empty_v1)
    check("independently built mirrors share the v1 fingerprint",
          mirror_status(mirror_again)["content_fingerprint"] == fp_v1)
    # 预言机与镜像模块对同一行的规范化结果一致 (跨实现口径)
    from factasm.mirror import canonical_row_line
    sample = next(iter(rows_v3.values()))
    check("canonical row line matches independent oracle",
          canonical_row_line(sample) == oracle_row_line(sample))
    check("partition function matches independent oracle across all keys",
          all(partition_of(k) == oracle_partition(k) for k in rows_v3))

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        for name, detail in FAIL:
            print(f"  FAIL {name}: {detail}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
