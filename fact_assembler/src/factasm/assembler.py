"""装配门面: 批次检查 -> 世代索引 -> 时态关联 -> 交叉核对 -> 原子发布。

发布语义:
  * 已确认的批次组合重复装配 -> 幂等返回既有版本, 不增加事实;
  * 同名批次换内容重送 -> 在登记阶段即以内容指纹冲突拒绝;
  * 任何校验/核对失败 -> 只写 rejected_assembly_audit, 不动 current_version;
  * 发布在单个 DuckDB 事务内完成, 并发读者要么看到上一版要么看到新版。
"""
from __future__ import annotations

import dataclasses
import datetime as _dt
import json
import os
import pathlib
import tempfile
from decimal import Decimal
from typing import Any

from . import reference as reference_mod
from .db import connect_persistent, connect_staging
from .engine import DuckDBEngine
from .errors import AssemblyRejected, ReferenceCheckError
from .sources import (
    CONTRACT,
    CUSTOMER,
    TRANSACTION,
    load_source_rows,
    require_batches,
)
from .util import Reason, canonical_json, content_fingerprint, short_hash, ts_to_str
from .validation import (
    Generation,
    GenerationIndex,
    parse_dimension_rows,
    parse_transaction_rows,
)


@dataclasses.dataclass
class AssemblyResult:
    request_id: str
    version_id: str | None
    status: str                      # PUBLISHED / REJECTED / REUSED
    fact_count: int
    quarantine_count: int
    customer_batch: str
    contract_batch: str
    transaction_batch: str
    reused: bool = False
    facts: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    quarantine: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    evidence: list[dict[str, Any]] = dataclasses.field(default_factory=list)
    rejections: list[dict[str, Any]] = dataclasses.field(default_factory=list)


class FactAssembler:
    """离线事实装配器的统一接口。"""

    def __init__(self, db_path: str | os.PathLike[str], *, staging_dir: str | os.PathLike[str] | None = None):
        self.db_path = pathlib.Path(db_path)
        self.staging_dir = pathlib.Path(staging_dir) if staging_dir else self.db_path.parent / "staging"
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ #
    # 装配与发布
    # ------------------------------------------------------------------ #
    def assemble(
        self,
        *,
        customer_batch: str,
        contract_batch: str,
        transaction_batch: str,
        crash_before_commit: bool = False,
        delay_before_commit: float = 0.0,
    ) -> AssemblyResult:
        """按显式批次组合执行一次装配。

        crash_before_commit=True 用于故障注入: 在持久库事务提交点之前
        ``os._exit`` 强退, 验证读者不会读到半成品。
        delay_before_commit>0 时在事务内、提交前睡眠指定秒数, 供并发读者
        观测「未提交版本不可见」。
        """
        con = connect_persistent(self.db_path)
        try:
            registered = require_batches(
                con,
                customer_batch=customer_batch,
                contract_batch=contract_batch,
                transaction_batch=transaction_batch,
            )
            hashes = {(r["source_kind"], r["batch_id"]): r["content_hash"] for r in registered}
            combo_fp = short_hash(
                canonical_json({
                    CUSTOMER: hashes[(CUSTOMER, customer_batch)],
                    CONTRACT: hashes[(CONTRACT, contract_batch)],
                    TRANSACTION: hashes[(TRANSACTION, transaction_batch)],
                })
            )
            canonical_request_id = f"req-{combo_fp}"

            # 幂等: 同一批次组合 (按内容指纹) 已发布过 -> 原样返回, 不增加事实
            existing = con.execute(
                "SELECT request_id, status FROM assembly_requests WHERE request_id = ?",
                [canonical_request_id],
            ).fetchone()
            if existing is not None and existing[1] == "PUBLISHED":
                return self._load_existing_result(
                    con, canonical_request_id,
                    customer_batch, contract_batch, transaction_batch,
                )

            staging_path, staging = connect_staging(self.staging_dir, f"{canonical_request_id}.staging.duckdb")
            try:
                return self._run_assembly(
                    con=con,
                    staging=staging,
                    staging_path=staging_path,
                    request_id=canonical_request_id,
                    customer_batch=customer_batch,
                    contract_batch=contract_batch,
                    transaction_batch=transaction_batch,
                    hashes=hashes,
                    crash_before_commit=crash_before_commit,
                    delay_before_commit=delay_before_commit,
                )
            finally:
                try:
                    staging.close()
                except Exception:
                    pass
        finally:
            con.close()

    def _run_assembly(
        self,
        *,
        con,
        staging,
        staging_path: pathlib.Path,
        request_id: str,
        customer_batch: str,
        contract_batch: str,
        transaction_batch: str,
        hashes: dict[tuple[str, str], str],
        crash_before_commit: bool,
        delay_before_commit: float = 0.0,
    ) -> AssemblyResult:
        # 1) 从登记的原始文件重新取数 (不按文件名猜批次, 批次来自显式请求)
        cust_rows = self._load_registered_rows(con, CUSTOMER, customer_batch)
        cont_rows = self._load_registered_rows(con, CONTRACT, contract_batch)
        txn_rows = self._load_registered_rows(con, TRANSACTION, transaction_batch)

        rejections: list[dict[str, Any]] = []

        # 2) 维度批次级校验 + 世代索引 (来源自身冲突直接拒批)
        try:
            cust_grouped = parse_dimension_rows(
                cust_rows, kind=CUSTOMER, batch_id=customer_batch,
                batch_content_hash=hashes[(CUSTOMER, customer_batch)],
            )
            cont_grouped = parse_dimension_rows(
                cont_rows, kind=CONTRACT, batch_id=contract_batch,
                batch_content_hash=hashes[(CONTRACT, contract_batch)],
            )
        except Exception as exc:  # ValidationError
            return self._reject(
                con, request_id, customer_batch, contract_batch, transaction_batch,
                rejections=[{"stage": "dimension_validation", "error": str(exc),
                             "details": getattr(exc, "details", None)}],
                staging_path=staging_path,
            )

        customer_index = GenerationIndex(cust_grouped)
        contract_index = GenerationIndex(cont_grouped)

        # 3) 交易解析 + 行级隔离 (缺时间/坏金额/重复原行/跨批次不一致)
        clean_txns, parse_quarantine = parse_transaction_rows(
            txn_rows,
            batch_id=transaction_batch,
            customer_batch_id=customer_batch,
            contract_batch_id=contract_batch,
        )

        # 4) 两条独立路径做时态关联
        ref_facts, ref_quarantine, evidence = reference_mod.correlate(
            clean_txns, customer_index, contract_index,
        )
        engine = DuckDBEngine(staging)
        engine.load(
            customer_gens=[g for gs in cust_grouped.values() for g in gs],
            contract_gens=[g for gs in cont_grouped.values() for g in gs],
            transactions=clean_txns,
        )
        sql_rows = engine.run_join()
        sql_counts = engine.match_counts()

        # 5) 交叉核对: SQL 命中数必须与参考实现候选数一致
        check_error = self._cross_check(sql_rows, sql_counts, ref_facts, ref_quarantine, clean_txns)
        if check_error is not None:
            rejections.append({"stage": "reference_cross_check", "error": check_error})
            return self._reject(
                con, request_id, customer_batch, contract_batch, transaction_batch,
                rejections=rejections, staging_path=staging_path,
            )

        all_quarantine = list(parse_quarantine)
        for q in ref_quarantine:
            q["transaction_batch_id"] = transaction_batch
            all_quarantine.append(q)
        # 去重并分配稳定代理 id: 同一源行同一原因同一证据只保留一条;
        # 不同维度但原因码相同 (如客户/合同两侧都重叠) 因证据不同而都保留。
        all_quarantine = self._dedupe_and_id_quarantine(all_quarantine, request_id)

        # 6) 每条可交付交易必须有唯一世代解释 (reference 已保证; 防御性断言)
        for f in ref_facts:
            if not f["customer_generation_id"] or not f["contract_generation_id"]:
                raise ReferenceCheckError(f"fact for {f['txn_row_id']} missing unique generation")

        source_summary = reference_mod.build_source_summary(
            customer_batch=customer_batch,
            contract_batch=contract_batch,
            transaction_batch=transaction_batch,
            batch_hashes=hashes,
        )
        for f in ref_facts:
            f["source_summary"] = source_summary
            f["request_id"] = request_id
            f["transaction_batch_id"] = transaction_batch

        # 7) 单事务原子发布
        version_id = f"ver-{short_hash(request_id + '|' + _dt.datetime.utcnow().isoformat())}"
        published_at = _dt.datetime.utcnow()
        self._publish(
            con=con,
            request_id=request_id,
            version_id=version_id,
            published_at=published_at,
            customer_batch=customer_batch,
            contract_batch=contract_batch,
            transaction_batch=transaction_batch,
            facts=ref_facts,
            quarantine=all_quarantine,
            customer_index=customer_index,
            contract_index=contract_index,
            txn_rows=txn_rows,
            hashes=hashes,
            crash_before_commit=crash_before_commit,
            delay_before_commit=delay_before_commit,
        )
        self._remove_staging(staging_path)

        return AssemblyResult(
            request_id=request_id,
            version_id=version_id,
            status="PUBLISHED",
            fact_count=len(ref_facts),
            quarantine_count=len(all_quarantine),
            customer_batch=customer_batch,
            contract_batch=contract_batch,
            transaction_batch=transaction_batch,
            facts=[self._fact_public(f) for f in ref_facts],
            quarantine=all_quarantine,
            evidence=evidence,
        )

    # ------------------------------------------------------------------ #
    # 交叉核对
    # ------------------------------------------------------------------ #
    def _cross_check(self, sql_rows, sql_counts, ref_facts, ref_quarantine, clean_txns) -> str | None:
        """两条独立路径对账: 每笔交易每个维度的候选数与选中世代必须一致。"""
        ref_fact_by_id = {f["txn_row_id"]: f for f in ref_facts}
        # 参考实现每个维度的期望命中数: 0=无世代, 1=唯一, 2=重叠(多命中)
        ref_hits: dict[str, dict[str, int]] = {}
        for q in ref_quarantine:
            # 隔离行的问题维度会被原因码覆盖; 未出现在原因里的维度视为正常唯一命中
            bucket = ref_hits.setdefault(
                q["txn_row_id"], {"customer_hits": 1, "contract_hits": 1}
            )
            if "CUSTOMER" in q["reason_code"]:
                bucket["customer_hits"] = (
                    2 if q["reason_code"] == Reason.OVERLAPPING_GENERATION else 0
                )
            if "CONTRACT" in q["reason_code"]:
                bucket["contract_hits"] = (
                    2 if q["reason_code"] == Reason.OVERLAPPING_GENERATION else 0
                )

        sql_by_id = {r["txn_row_id"]: r for r in sql_rows}
        if set(sql_by_id) != {t.txn_row_id for t in clean_txns}:
            return "SQL engine lost or invented transaction rows"

        dims = (("customer_hits", "customer_generation_id", "customer"),
                ("contract_hits", "contract_generation_id", "contract"))
        for txn in clean_txns:
            row = sql_by_id[txn.txn_row_id]
            sql_n = sql_counts[txn.txn_row_id]
            if txn.txn_row_id in ref_fact_by_id:
                # 参考实现判定唯一 -> SQL 两侧必须恰好各命中 1 个, 且世代身份一致
                if sql_n != {"customer_hits": 1, "contract_hits": 1}:
                    return f"hit-count mismatch for fact {txn.txn_row_id}: sql={sql_n}"
                fact = ref_fact_by_id[txn.txn_row_id]
                if row["customer_generation_id"] != fact["customer_generation_id"]:
                    return f"customer generation mismatch for {txn.txn_row_id}"
                if row["contract_generation_id"] != fact["contract_generation_id"]:
                    return f"contract generation mismatch for {txn.txn_row_id}"
                if _norm_dec(row["amount"]) != _norm_dec(fact["amount"]):
                    return f"amount mismatch for {txn.txn_row_id}"
                continue

            # 隔离行: 逐维度对账。隔离原因只出现在问题维度, 另一维度期望恰好 1 命中。
            ref_n = ref_hits.get(txn.txn_row_id,
                                 {"customer_hits": 1, "contract_hits": 1})
            for hit_field, gen_field, label in dims:
                expected = ref_n[hit_field]
                actual = sql_n[hit_field]
                if expected == 0:
                    if actual != 0 or row[gen_field] is not None:
                        return (f"{label} should have no generation for "
                                f"{txn.txn_row_id}, sql_hits={actual}")
                elif expected == 2:
                    if actual < 2:
                        return (f"{label} overlap not reproduced by SQL for "
                                f"{txn.txn_row_id}, sql_hits={actual}")
                else:  # 该维度本身正常, 应唯一命中
                    if actual != 1 or row[gen_field] is None:
                        return (f"{label} should uniquely match for "
                                f"{txn.txn_row_id}, sql_hits={actual}")
        return None

    # ------------------------------------------------------------------ #
    # 发布事务
    # ------------------------------------------------------------------ #
    def _publish(
        self,
        *,
        con,
        request_id: str,
        version_id: str,
        published_at: _dt.datetime,
        customer_batch: str,
        contract_batch: str,
        transaction_batch: str,
        facts: list[dict[str, Any]],
        quarantine: list[dict[str, Any]],
        customer_index: GenerationIndex,
        contract_index: GenerationIndex,
        txn_rows: list[dict[str, Any]],
        hashes: dict[tuple[str, str], str],
        crash_before_commit: bool,
        delay_before_commit: float = 0.0,
    ) -> None:
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(
                "INSERT OR REPLACE INTO assembly_requests "
                "(request_id, customer_batch_id, contract_batch_id, transaction_batch_id, "
                "status, created_at, published_at, fact_count, quarantine_count, reject_summary) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [request_id, customer_batch, contract_batch, transaction_batch,
                 "PUBLISHED", published_at, published_at, len(facts), len(quarantine), None],
            )
            con.execute(
                "INSERT INTO fact_versions "
                "(version_id, request_id, customer_batch_id, contract_batch_id, "
                "transaction_batch_id, created_at, fact_count, quarantine_count) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [version_id, request_id, customer_batch, contract_batch,
                 transaction_batch, published_at, len(facts), len(quarantine)],
            )

            _safe_executemany(
                con,
                "INSERT INTO customer_generations "
                "SELECT * FROM (SELECT ? AS generation_id, ? AS batch_id, ? AS business_key, "
                "? AS revision_no, ? AS valid_from, ? AS valid_to, ? AS attributes, "
                "? AS source_row_no, ? AS content_hash) "
                "WHERE NOT EXISTS (SELECT 1 FROM customer_generations WHERE generation_id = ?)",
                [
                    (g.generation_id, g.batch_id, g.business_key, g.revision_no,
                     g.valid_from, g.valid_to, canonical_json(g.attributes),
                     g.source_row_no, g.content_hash, g.generation_id)
                    for k in customer_index.keys() for g in customer_index.all_for(k)
                ],
            )
            _safe_executemany(
                con,
                "INSERT INTO contract_generations "
                "SELECT * FROM (SELECT ? AS generation_id, ? AS batch_id, ? AS business_key, "
                "? AS revision_no, ? AS valid_from, ? AS valid_to, ? AS attributes, "
                "? AS source_row_no, ? AS content_hash) "
                "WHERE NOT EXISTS (SELECT 1 FROM contract_generations WHERE generation_id = ?)",
                [
                    (g.generation_id, g.batch_id, g.business_key, g.revision_no,
                     g.valid_from, g.valid_to, canonical_json(g.attributes),
                     g.source_row_no, g.content_hash, g.generation_id)
                     for k in contract_index.keys() for g in contract_index.all_for(k)
                ],
            )

            _safe_executemany(
                con,
                "INSERT INTO transaction_inputs "
                "SELECT * FROM (SELECT ? AS txn_row_id, ? AS batch_id, ? AS source_row_no, "
                "? AS event_time, ? AS customer_key, ? AS contract_key, ? AS amount, ? AS raw_payload) "
                "WHERE NOT EXISTS (SELECT 1 FROM transaction_inputs WHERE batch_id = ? AND txn_row_id = ?)",
                [
                    (
                        str(r.get("txn_row_id")) if r.get("txn_row_id") is not None else f"__row_{r['__source_row_no']}",
                        transaction_batch,
                        r["__source_row_no"],
                        _safe_ts(r.get("event_time")),
                        _optional(r.get("customer_key")),
                        _optional(r.get("contract_key")),
                        _safe_amount(r.get("amount")),
                        canonical_json({k: v for k, v in r.items() if not k.startswith("__")}),
                        transaction_batch,
                        str(r.get("txn_row_id")) if r.get("txn_row_id") is not None else f"__row_{r['__source_row_no']}",
                    )
                    for r in txn_rows
                ],
            )

            _safe_executemany(
                con,
                "INSERT INTO facts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        version_id, f["request_id"], f["txn_row_id"], f["transaction_batch_id"],
                        f["source_row_no"], f["event_time"], f["customer_key"], f["contract_key"],
                        _norm_dec(f["amount"]),
                        f["customer_generation_id"], f["contract_generation_id"],
                        f["customer_valid_from"], f["customer_valid_to"],
                        f["contract_valid_from"], f["contract_valid_to"],
                        f["match_evidence"], f["source_summary"],
                    )
                    for f in facts
                ],
            )
            _safe_executemany(
                con,
                "INSERT INTO quarantine "
                "(q_id, version_id, request_id, transaction_batch_id, txn_row_id, "
                "source_row_no, reason_code, reason_detail, rejected_candidates, raw_payload) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        q["q_id"], version_id, request_id, q["transaction_batch_id"],
                        q["txn_row_id"], q["source_row_no"], q["reason_code"],
                        q["reason_detail"], q["rejected_candidates"], q["raw_payload"],
                    )
                    for q in quarantine
                ],
            )

            # 当前版本指针在同一事务内切换 -> 读者要么只见旧版要么只见新版
            con.execute("DELETE FROM current_version")
            con.execute("INSERT INTO current_version (version_id, published_at) VALUES (?, ?)",
                        [version_id, published_at])

            if delay_before_commit > 0:
                # 事务仍未提交: 并发读者在此窗口内必须只能看到旧版本
                import time
                time.sleep(delay_before_commit)

            if crash_before_commit:
                # 故障注入: 事务未提交即进程消失, WAL 回滚保证旧版本完好
                con.close()
                os._exit(87)
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise

    def _reject(
        self,
        con,
        request_id: str,
        customer_batch: str,
        contract_batch: str,
        transaction_batch: str,
        *,
        rejections: list[dict[str, Any]],
        staging_path: pathlib.Path,
    ) -> AssemblyResult:
        now = _dt.datetime.utcnow()
        # 审计记录与 current_version/facts 物理隔离, 失败不改变旧版本。
        # REPLACE: 同一失败组合允许重试, 审计以最新一次为准。
        con.execute(
            "INSERT OR REPLACE INTO rejected_assembly_audit VALUES (?, ?, ?, ?, ?, ?)",
            [request_id, customer_batch, contract_batch, transaction_batch,
             now, canonical_json(rejections)],
        )
        con.execute(
            "INSERT OR REPLACE INTO assembly_requests "
            "(request_id, customer_batch_id, contract_batch_id, transaction_batch_id, "
            "status, created_at, published_at, fact_count, quarantine_count, reject_summary) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [request_id, customer_batch, contract_batch, transaction_batch,
             "REJECTED", now, None, 0, 0, canonical_json(rejections)],
        )
        self._remove_staging(staging_path)
        result = AssemblyResult(
            request_id=request_id,
            version_id=None,
            status="REJECTED",
            fact_count=0,
            quarantine_count=0,
            customer_batch=customer_batch,
            contract_batch=contract_batch,
            transaction_batch=transaction_batch,
            rejections=rejections,
        )
        raise AssemblyRejected(
            f"assembly {request_id} rejected: {canonical_json(rejections)}",
            request_id=request_id,
            rejections=rejections,
        )

    # ------------------------------------------------------------------ #
    # 查询接口
    # ------------------------------------------------------------------ #
    def current_version(self) -> dict[str, Any] | None:
        con = connect_persistent(self.db_path, read_only=True)
        try:
            row = con.execute(
                "SELECT cv.version_id, cv.published_at, fv.request_id, "
                "fv.customer_batch_id, fv.contract_batch_id, fv.transaction_batch_id, "
                "fv.fact_count, fv.quarantine_count "
                "FROM current_version cv JOIN fact_versions fv ON fv.version_id = cv.version_id"
            ).fetchone()
            if row is None:
                return None
            cols = [d[0] for d in con.description]
            return dict(zip(cols, row))
        finally:
            con.close()

    def query_facts(self, *, version_id: str | None = None, where: str | None = None,
                    params: list[Any] | None = None) -> list[dict[str, Any]]:
        """查询事实; 默认查当前已发布版本, 也可指定历史版本。"""
        con = connect_persistent(self.db_path, read_only=True)
        try:
            table, version_filter = self._scope_table("facts", version_id)
            clauses: list[str] = []
            params: list[Any] = []
            if version_filter:
                clauses.append(version_filter)
                params.append(version_id)
            if where:
                clauses.append(f"({where})")
            sql = f"SELECT * FROM {table}"
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY source_row_no"
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            con.close()

    def query_quarantine(self, *, version_id: str | None = None,
                         reason_code: str | None = None) -> list[dict[str, Any]]:
        """隔离清单查询, 可按原因码过滤; 每条带原行身份与排除候选证据。"""
        con = connect_persistent(self.db_path, read_only=True)
        try:
            table, version_filter = self._scope_table("quarantine", version_id)
            clauses: list[str] = []
            params: list[Any] = []
            if version_filter:
                clauses.append(version_filter)
                params.append(version_id)
            if reason_code:
                clauses.append("reason_code = ?")
                params.append(reason_code)
            sql = f"SELECT * FROM {table}"
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY source_row_no, reason_code"
            cur = con.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            con.close()

    def trace_transaction(self, txn_row_id: str) -> dict[str, Any]:
        """按交易原行身份追溯: 事实/隔离 + 世代身份 + 匹配区间 + 证据 + 来源摘要。"""
        con = connect_persistent(self.db_path, read_only=True)
        try:
            fact = con.execute(
                "SELECT * FROM current_facts WHERE txn_row_id = ?", [txn_row_id]
            ).fetchone()
            if fact is not None:
                cols = [d[0] for d in con.description]
                out = dict(zip(cols, fact))
                out["match_evidence"] = json.loads(out["match_evidence"])
                out["source_summary"] = json.loads(out["source_summary"])
                out["outcome"] = "FACT"
                return out
            qrows = con.execute(
                "SELECT * FROM current_quarantine WHERE txn_row_id = ? "
                "ORDER BY source_row_no, reason_code",
                [txn_row_id],
            ).fetchall()
            if qrows:
                cols = [d[0] for d in con.description]
                reasons = [dict(zip(cols, r)) for r in qrows]
                for r in reasons:
                    if r["rejected_candidates"]:
                        r["rejected_candidates"] = json.loads(r["rejected_candidates"])
                    if r["raw_payload"]:
                        r["raw_payload"] = json.loads(r["raw_payload"])
                return {"txn_row_id": txn_row_id, "outcome": "QUARANTINED", "reasons": reasons}
            return {"txn_row_id": txn_row_id, "outcome": "UNKNOWN",
                    "detail": "not present in current version (check batch/version)"}
        finally:
            con.close()

    def generation_index_snapshot(self, dimension: str) -> list[dict[str, Any]]:
        """查看已注册并发布引用的世代索引 (客户/合同)。"""
        table = "customer_generations" if dimension == CUSTOMER else "contract_generations"
        key = "customer_key" if dimension == CUSTOMER else "contract_key"
        con = connect_persistent(self.db_path, read_only=True)
        try:
            cur = con.execute(
                f"SELECT generation_id, batch_id, {key} AS business_key, revision_no, "
                f"valid_from, valid_to, attributes, source_row_no FROM {table} "
                f"ORDER BY business_key, valid_from"
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            con.close()

    def list_versions(self) -> list[dict[str, Any]]:
        con = connect_persistent(self.db_path, read_only=True)
        try:
            cur = con.execute("SELECT * FROM fact_versions ORDER BY created_at")
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            con.close()

    def list_rejections(self) -> list[dict[str, Any]]:
        con = connect_persistent(self.db_path, read_only=True)
        try:
            cur = con.execute("SELECT * FROM rejected_assembly_audit ORDER BY rejected_at")
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            con.close()

    # ------------------------------------------------------------------ #
    # 清理 (内存不随历史增长; 历史按批次清理但保护在引用来源)
    # ------------------------------------------------------------------ #
    def cleanup_batch(self, batch_id: str, *, dry_run: bool = False) -> dict[str, Any]:
        """尝试清理一个历史批次的全部登记/世代/交易行。

        若该批次仍被任一已发布事实版本引用, 拒绝删除并返回引用清单;
        dry_run=True 只报告将删除/被保护的内容。
        """
        con = connect_persistent(self.db_path)
        try:
            refs = self._find_references(con, batch_id)
            protected = [r for r in refs if r["referenced_by_published_facts"]]
            deletable_kinds = []
            for kind in (CUSTOMER, CONTRACT, TRANSACTION):
                row = con.execute(
                    "SELECT row_count FROM batch_registry WHERE batch_id = ? AND source_kind = ?",
                    [batch_id, kind],
                ).fetchone()
                if row is not None:
                    deletable_kinds.append(kind)

            plan = {
                "batch_id": batch_id,
                "dry_run": dry_run,
                "protected_by_published_facts": protected,
                "would_delete_registry_kinds": [] if protected else deletable_kinds,
            }
            if dry_run or protected:
                return plan

            con.execute("BEGIN TRANSACTION")
            try:
                con.execute("DELETE FROM customer_generations WHERE batch_id = ?", [batch_id])
                con.execute("DELETE FROM contract_generations WHERE batch_id = ?", [batch_id])
                con.execute("DELETE FROM transaction_inputs WHERE batch_id = ?", [batch_id])
                con.execute("DELETE FROM batch_registry WHERE batch_id = ?", [batch_id])
                con.execute("COMMIT")
            except BaseException:
                con.execute("ROLLBACK")
                raise
            plan["deleted"] = True
            return plan
        finally:
            con.close()

    def cleanup_staging(self) -> list[str]:
        """清理没有对应已发布版本的遗留 staging 文件 (崩溃后的垃圾回收)。"""
        con = connect_persistent(self.db_path, read_only=True)
        try:
            live = {r[0] for r in con.execute("SELECT request_id FROM assembly_requests").fetchall()}
        finally:
            con.close()
        removed = []
        for p in self.staging_dir.glob("*.staging.duckdb*"):
            if not any(req in p.name for req in live):
                p.unlink(missing_ok=True)
                removed.append(str(p))
        return removed

    def _find_references(self, con, batch_id: str) -> list[dict[str, Any]]:
        """找出引用该批次的已发布版本; 事实或隔离清单引用都算「仍被引用」。"""
        out = []
        cur = con.execute(
            """
            SELECT fv.version_id, fv.request_id,
                   BOOL_OR(fv.customer_batch_id = ?) AS cust,
                   BOOL_OR(fv.contract_batch_id = ?) AS cont,
                   BOOL_OR(fv.transaction_batch_id = ?) AS txn,
                   (SELECT COUNT(*) FROM facts f WHERE f.version_id = fv.version_id) AS fact_rows,
                   (SELECT COUNT(*) FROM quarantine q WHERE q.version_id = fv.version_id) AS quarantine_rows
            FROM fact_versions fv
            WHERE fv.customer_batch_id = ? OR fv.contract_batch_id = ?
               OR fv.transaction_batch_id = ?
            GROUP BY fv.version_id, fv.request_id
            """,
            [batch_id] * 6,
        )
        for version_id, request_id, cust, cont, txn, fact_rows, q_rows in cur.fetchall():
            out.append({
                "version_id": version_id,
                "request_id": request_id,
                "uses_as": [k for k, v in
                            ((CUSTOMER, cust), (CONTRACT, cont), (TRANSACTION, txn)) if v],
                "fact_rows": fact_rows,
                "quarantine_rows": q_rows,
                "referenced_by_published_facts": (fact_rows + q_rows) > 0,
            })
        return out

    # ------------------------------------------------------------------ #
    # 内部辅助
    # ------------------------------------------------------------------ #
    def _scope_table(self, base: str, version_id: str | None) -> tuple[str, str | None]:
        """返回 (表名, 版本过滤 SQL 片段); None 表示读当前版本视图。"""
        if version_id is not None:
            return base, "version_id = ?"
        return f"current_{base}", None

    def _load_registered_rows(self, con, kind: str, batch_id: str) -> list[dict[str, Any]]:
        row = con.execute(
            "SELECT file_path FROM batch_registry WHERE batch_id = ? AND source_kind = ?",
            [batch_id, kind],
        ).fetchone()
        if row is None:  # pragma: no cover - require_batches 已拦截
            from .errors import BatchNotFoundError
            raise BatchNotFoundError(f"{kind} batch {batch_id!r} not registered")
        return load_source_rows(row[0])

    def _load_existing_result(
        self, con, request_id: str,
        customer_batch: str, contract_batch: str, transaction_batch: str,
    ) -> AssemblyResult:
        ver = con.execute(
            "SELECT version_id, fact_count, quarantine_count FROM fact_versions WHERE request_id = ?",
            [request_id],
        ).fetchone()
        version_id, fact_count, q_count = ver
        cur = con.execute("SELECT * FROM facts WHERE version_id = ? ORDER BY source_row_no", [version_id])
        cols = [d[0] for d in cur.description]
        facts = [self._fact_public(dict(zip(cols, r))) for r in cur.fetchall()]
        cur = con.execute(
            "SELECT * FROM quarantine WHERE version_id = ? ORDER BY source_row_no, reason_code",
            [version_id],
        )
        cols = [d[0] for d in cur.description]
        quarantine = []
        for r in cur.fetchall():
            q = dict(zip(cols, r))
            # 与首次发布口径一致: JSON 字段解析为对象
            if q.get("rejected_candidates"):
                q["rejected_candidates"] = json.loads(q["rejected_candidates"])
            if q.get("raw_payload"):
                q["raw_payload"] = json.loads(q["raw_payload"])
            quarantine.append(q)
        return AssemblyResult(
            request_id=request_id,
            version_id=version_id,
            status="REUSED",
            fact_count=fact_count,
            quarantine_count=q_count,
            customer_batch=customer_batch,
            contract_batch=contract_batch,
            transaction_batch=transaction_batch,
            reused=True,
            facts=facts,
            quarantine=quarantine,
        )

    def _fact_public(self, f: dict[str, Any]) -> dict[str, Any]:
        out = dict(f)
        out["amount"] = _norm_dec(out["amount"])
        out["match_evidence"] = json.loads(out["match_evidence"])
        out["source_summary"] = json.loads(out["source_summary"])
        return out

    def _remove_staging(self, staging_path: pathlib.Path) -> None:
        for suffix in ("", ".wal"):
            pathlib.Path(str(staging_path) + suffix).unlink(missing_ok=True)

    @staticmethod
    def _dedupe_and_id_quarantine(rows: list[dict[str, Any]], request_id: str) -> list[dict[str, Any]]:
        """去重并为每条隔离原因分配稳定代理主键 q_id。"""
        seen: set[tuple] = set()
        out: list[dict[str, Any]] = []
        for q in rows:
            key = (
                q.get("txn_row_id"),
                q.get("source_row_no"),
                q.get("reason_code"),
                q.get("reason_detail"),
                q.get("rejected_candidates"),
            )
            if key in seen:
                continue
            seen.add(key)
            q["q_id"] = "q-" + short_hash(canonical_json({"request": request_id, "key": key}))
            out.append(q)
        return out


# --------------------------------------------------------------------------- #
# 模块级小工具
# --------------------------------------------------------------------------- #
def _norm_dec(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _safe_executemany(con, sql: str, rows: list) -> None:
    """DuckDB 的 executemany 拒绝空参数集; 空列表视为无行可插, 直接跳过。"""
    if rows:
        con.executemany(sql, rows)


def _optional(value: Any) -> str | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return str(value).strip()


def _safe_ts(value: Any):
    from .util import parse_optional_timestamp
    try:
        return parse_optional_timestamp(value)
    except ValueError:
        return None


def _safe_amount(value: Any):
    from .util import parse_amount
    try:
        return parse_amount(value)
    except ValueError:
        return None
