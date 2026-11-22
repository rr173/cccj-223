"""DuckDB 连接管理与持久化 schema。

持久库 (main.duckdb) 中只保存 *已发布* 事实、已注册批次和审计/版本指针;
每次装配的中间数据写在独立的临时 staging 库中, 发布成功后才在单个事务里
并入持久库, 保证「失败批次不能改变旧版本」以及「读者只见完整版本」。

读侧通过视图 current_facts / current_quarantine 暴露 *当前已发布版本*,
装配写入发生在另一套物理表, 视图切换在事务提交点原子完成。
"""
from __future__ import annotations

import os
import pathlib

import duckdb

SCHEMA_VERSION = 1

_SCHEMA_SQL = """
-- -------------------------------------------------------------------------
-- 批次登记表: 同名批次内容指纹必须一致
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS batch_registry (
    batch_id        VARCHAR NOT NULL,
    source_kind     VARCHAR NOT NULL,            -- customer / contract / transaction
    file_path       VARCHAR NOT NULL,
    row_count       BIGINT  NOT NULL,
    content_hash    VARCHAR NOT NULL,
    registered_at   TIMESTAMP NOT NULL,
    PRIMARY KEY (batch_id, source_kind)
);

-- 已注册批次里的来源世代 (客户/合同), 发布事实通过外键引用
CREATE TABLE IF NOT EXISTS customer_generations (
    generation_id   VARCHAR NOT NULL PRIMARY KEY,
    batch_id        VARCHAR NOT NULL,
    customer_key    VARCHAR NOT NULL,
    revision_no     BIGINT  NOT NULL,
    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP,
    attributes      VARCHAR NOT NULL,            -- canonical JSON
    source_row_no   BIGINT  NOT NULL,
    content_hash    VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS contract_generations (
    generation_id   VARCHAR NOT NULL PRIMARY KEY,
    batch_id        VARCHAR NOT NULL,
    contract_key    VARCHAR NOT NULL,
    revision_no     BIGINT  NOT NULL,
    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP,
    attributes      VARCHAR NOT NULL,
    source_row_no   BIGINT  NOT NULL,
    content_hash    VARCHAR NOT NULL
);

-- 已注册交易原始行 (含被隔离的行, 保留可定位身份)
CREATE TABLE IF NOT EXISTS transaction_inputs (
    txn_row_id      VARCHAR NOT NULL,
    batch_id        VARCHAR NOT NULL,
    source_row_no   BIGINT  NOT NULL,
    event_time      TIMESTAMP,
    customer_key    VARCHAR,
    contract_key    VARCHAR,
    amount          DECIMAL(28, 6),
    raw_payload     VARCHAR NOT NULL,
    PRIMARY KEY (batch_id, txn_row_id)
);

-- -------------------------------------------------------------------------
-- 装配请求与版本
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS assembly_requests (
    request_id          VARCHAR NOT NULL PRIMARY KEY,
    customer_batch_id   VARCHAR NOT NULL,
    contract_batch_id   VARCHAR NOT NULL,
    transaction_batch_id VARCHAR NOT NULL,
    status              VARCHAR NOT NULL,        -- PUBLISHED / REJECTED
    created_at          TIMESTAMP NOT NULL,
    published_at        TIMESTAMP,
    fact_count          BIGINT NOT NULL DEFAULT 0,
    quarantine_count    BIGINT NOT NULL DEFAULT 0,
    reject_summary      VARCHAR
);

-- 每个 request 一行的不可变版本描述
CREATE TABLE IF NOT EXISTS fact_versions (
    version_id        VARCHAR NOT NULL PRIMARY KEY,
    request_id        VARCHAR NOT NULL,
    customer_batch_id VARCHAR NOT NULL,
    contract_batch_id VARCHAR NOT NULL,
    transaction_batch_id VARCHAR NOT NULL,
    created_at        TIMESTAMP NOT NULL,
    fact_count        BIGINT NOT NULL,
    quarantine_count  BIGINT NOT NULL
);

-- 当前版本指针: 全表至多一行
CREATE TABLE IF NOT EXISTS current_version (
    singleton   INTEGER NOT NULL PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
    version_id  VARCHAR NOT NULL,
    published_at TIMESTAMP NOT NULL
);

-- -------------------------------------------------------------------------
-- 已发布事实 (版本化, 只追加; 清理按版本/批次进行)
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS facts (
    version_id          VARCHAR NOT NULL,
    request_id          VARCHAR NOT NULL,
    txn_row_id          VARCHAR NOT NULL,
    transaction_batch_id VARCHAR NOT NULL,
    source_row_no       BIGINT NOT NULL,
    event_time          TIMESTAMP NOT NULL,
    customer_key        VARCHAR NOT NULL,
    contract_key        VARCHAR NOT NULL,
    amount              DECIMAL(28, 6) NOT NULL,
    customer_generation_id VARCHAR NOT NULL,
    contract_generation_id VARCHAR NOT NULL,
    customer_valid_from TIMESTAMP NOT NULL,
    customer_valid_to   TIMESTAMP,
    contract_valid_from TIMESTAMP NOT NULL,
    contract_valid_to   TIMESTAMP,
    match_evidence      VARCHAR NOT NULL,         -- JSON: 候选枚举与唯一选择证据
    source_summary      VARCHAR NOT NULL,         -- JSON: 三来源批次/行号摘要
    PRIMARY KEY (version_id, txn_row_id)
);

-- 已发布隔离清单 (版本化, 只追加)。
-- 用代理主键: 同一源行可能因不同维度产生相同原因码 (如客户/合同两侧都重叠),
-- 复合业务键无法区分, 故由装配器分配 q_id, 业务字段保留可定位身份。
CREATE TABLE IF NOT EXISTS quarantine (
    q_id                VARCHAR NOT NULL PRIMARY KEY,
    version_id          VARCHAR NOT NULL,
    request_id          VARCHAR NOT NULL,
    transaction_batch_id VARCHAR NOT NULL,
    txn_row_id          VARCHAR,
    source_row_no       BIGINT,
    reason_code         VARCHAR NOT NULL,
    reason_detail       VARCHAR NOT NULL,
    rejected_candidates VARCHAR,                  -- JSON: 被排除的候选及原因
    raw_payload         VARCHAR
);

-- 拒绝请求的审计记录 (不产生版本, 不改变 current_version)
CREATE TABLE IF NOT EXISTS rejected_assembly_audit (
    request_id      VARCHAR NOT NULL PRIMARY KEY,
    customer_batch_id VARCHAR NOT NULL,
    contract_batch_id VARCHAR NOT NULL,
    transaction_batch_id VARCHAR NOT NULL,
    rejected_at     TIMESTAMP NOT NULL,
    reject_summary  VARCHAR NOT NULL
);

-- -------------------------------------------------------------------------
-- 读侧视图: 始终指向「上一次完整发布版本」
-- -------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS current_facts AS
SELECT f.*
FROM facts f
JOIN current_version cv ON cv.version_id = f.version_id;

CREATE VIEW IF NOT EXISTS current_quarantine AS
SELECT q.*
FROM quarantine q
JOIN current_version cv ON cv.version_id = q.version_id;
"""


def connect_persistent(db_path: str | os.PathLike[str], *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """打开持久库并确保 schema 存在。

    read_only=True 时要求库文件已存在 (供并发读者使用), 不做 DDL。
    """
    path = pathlib.Path(db_path)
    if read_only:
        if not path.exists():
            raise FileNotFoundError(f"database not found: {path}")
        con = duckdb.connect(str(path), read_only=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(path))
        con.execute("PRAGMA enable_object_cache=false")
        _apply_pragmas(con)
        con.execute(_SCHEMA_SQL)
    return con


def connect_staging(dir_path: str | os.PathLike[str], name: str) -> tuple[pathlib.Path, duckdb.DuckDBPyConnection]:
    """创建一次性 staging 库 (装配中间结果), 调用方负责关闭与删除文件。"""
    directory = pathlib.Path(dir_path)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    if path.exists():
        path.unlink()
    con = duckdb.connect(str(path))
    _apply_pragmas(con)
    return path, con


def _apply_pragmas(con: duckdb.DuckDBPyConnection) -> None:
    # 内存只服务当前装配; 历史保留在持久库磁盘表中, 不随处理过程累积。
    con.execute("PRAGMA memory_limit='256MB'")
    con.execute("PRAGMA threads=2")


def destroy_database(db_path: str | os.PathLike[str]) -> None:
    """测试/运维辅助: 物理删除数据库及其临时旁路文件。"""
    path = pathlib.Path(db_path)
    for suffix in ("", ".wal", ".tmp"):
        candidate = pathlib.Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()
