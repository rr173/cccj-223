"""DuckDB 关系引擎: 在一次性 staging 库中完成时态连接。

职责边界:
  * DuckDB 负责集合扫描与半开区间连接等 *关系计算*;
  * 候选枚举与「为什么选中/排除」的逐代证据由 :mod:`factasm.reference`
    的纯 Python 路径独立产出, 这里不允许只用一条 SQL 丢掉证据;
  * 两条路径的结果由 assembler 逐行交叉核对, 不一致则拒绝发布。
"""
from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from typing import Any

from .validation import Generation, ParsedTransaction

_STAGING_DDL = """
CREATE TABLE stg_customer_generations (
    generation_id VARCHAR, batch_id VARCHAR, business_key VARCHAR,
    revision_no BIGINT, valid_from TIMESTAMP, valid_to TIMESTAMP,
    source_row_no BIGINT
);
CREATE TABLE stg_contract_generations (
    generation_id VARCHAR, batch_id VARCHAR, business_key VARCHAR,
    revision_no BIGINT, valid_from TIMESTAMP, valid_to TIMESTAMP,
    source_row_no BIGINT
);
CREATE TABLE stg_transactions (
    txn_row_id VARCHAR, source_row_no BIGINT, event_time TIMESTAMP,
    customer_key VARCHAR, contract_key VARCHAR, amount DECIMAL(28, 6)
);
"""

# 半开区间连接: valid_from <= event_time < COALESCE(valid_to, +inf)
_JOIN_SQL = """
WITH matched AS (
    SELECT
        t.txn_row_id,
        t.source_row_no,
        t.event_time,
        t.customer_key,
        t.contract_key,
        t.amount,
        cg.generation_id AS customer_generation_id,
        kg.generation_id AS contract_generation_id,
        cg.valid_from  AS customer_valid_from,
        cg.valid_to    AS customer_valid_to,
        kg.valid_from  AS contract_valid_from,
        kg.valid_to    AS contract_valid_to,
        cg.revision_no AS customer_revision_no,
        kg.revision_no AS contract_revision_no
    FROM stg_transactions t
    LEFT JOIN stg_customer_generations cg
      ON cg.business_key = t.customer_key
     AND cg.valid_from <= t.event_time
     AND t.event_time < COALESCE(cg.valid_to, TIMESTAMP '9999-12-31 23:59:59')
    LEFT JOIN stg_contract_generations kg
      ON kg.business_key = t.contract_key
     AND kg.valid_from <= t.event_time
     AND t.event_time < COALESCE(kg.valid_to, TIMESTAMP '9999-12-31 23:59:59')
)
SELECT * FROM matched ORDER BY source_row_no
"""


class DuckDBEngine:
    """封装 staging 库生命周期与关系计算, 数据不落主库。"""

    def __init__(self, con):
        self.con = con
        con.execute(_STAGING_DDL)

    def load(
        self,
        *,
        customer_gens: list[Generation],
        contract_gens: list[Generation],
        transactions: list[ParsedTransaction],
    ) -> None:
        self.con.executemany(
            "INSERT INTO stg_customer_generations VALUES (?, ?, ?, ?, ?, ?, ?)",
            [_gen_row(g) for g in customer_gens],
        )
        self.con.executemany(
            "INSERT INTO stg_contract_generations VALUES (?, ?, ?, ?, ?, ?, ?)",
            [_gen_row(g) for g in contract_gens],
        )
        self.con.executemany(
            "INSERT INTO stg_transactions VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    t.txn_row_id,
                    t.source_row_no,
                    t.event_time,
                    t.customer_key,
                    t.contract_key,
                    _to_decimal(t.amount),
                )
                for t in transactions
            ],
        )

    def run_join(self) -> list[dict[str, Any]]:
        """执行半开区间连接并返回每行的原始匹配结果 (含多匹配/未匹配)。"""
        cur = self.con.execute(_JOIN_SQL)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def match_counts(self) -> dict[str, dict[str, int]]:
        """按交易统计客户/合同侧命中世代数 (多命中即来源冲突信号)。"""
        result: dict[str, dict[str, int]] = {}
        cur = self.con.execute(
            """
            SELECT t.txn_row_id,
                   COUNT(cg.generation_id) AS cust_hits,
                   COUNT(kg.generation_id) AS cont_hits
            FROM stg_transactions t
            LEFT JOIN stg_customer_generations cg
              ON cg.business_key = t.customer_key
             AND cg.valid_from <= t.event_time
             AND t.event_time < COALESCE(cg.valid_to, TIMESTAMP '9999-12-31 23:59:59')
            LEFT JOIN stg_contract_generations kg
              ON kg.business_key = t.contract_key
             AND kg.valid_from <= t.event_time
             AND t.event_time < COALESCE(kg.valid_to, TIMESTAMP '9999-12-31 23:59:59')
            GROUP BY t.txn_row_id
            """
        )
        for txn_id, cust_hits, cont_hits in cur.fetchall():
            result[txn_id] = {"customer_hits": cust_hits, "contract_hits": cont_hits}
        return result


def _gen_row(g: Generation) -> tuple:
    return (
        g.generation_id,
        g.batch_id,
        g.business_key,
        g.revision_no,
        g.valid_from,
        g.valid_to,
        g.source_row_no,
    )


def _to_decimal(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value if isinstance(value, Decimal) else Decimal(str(value))
