"""离线事实镜像测试夹具: 三个已发布版本 + 一个无变化版本。

装配器一次装配只接受一个客户批次和一个合同批次, 而 generation_id 内嵌批次 id;
若在版本间更换维度批次, 该维度上所有世代的 generation_id 都会变, 就不可能存在
「投影逐字段不变」的行。为在同一套机制下同时覆盖「区间末端封口」与「整批未变
行」, 本夹具让四个版本共享同一对维度批次 (MC-S / MK-S), 版本差异全部来自交易
批次; 需要 NULL -> 具体时间 的更正时, 让同一交易键在新版改挂到一个预先定义好
的「已封口」合同世代 (合同键也在投影里, 整行只产生一笔 upsert)。

设计目标 (v1 -> v3 跨版本差量, 跳过 v2):
  * TXN-DEL-01 / TXN-DEL-02: v1 有、v3 无 -> 真实删除 (落在两个不同分区)。
  * TXN-AMT-01: 同一键金额仅差百万分之一 (100.000000 -> 100.000001), 其余投影
    不变 —— 专测 1e-6 差异也会被识别为整行替换。
  * TXN-NEW-01: v2 新增、v3 仍在; v2 挂开放合同 K2O (contract_valid_to=NULL),
    v3 改挂封口合同 K2C (contract_valid_to=2026-06-01) —— 区间末端 NULL 更正为
    具体时间, 整行只发一笔 upsert。
  * TXN-KEEP-01 / TXN-KEEP-02: v1/v3 投影逐字段相同的未变行 (挂稳定 K1/K3);
    KEEP-02 刻意取一个与全部变更行不同分区的行 ID (运行时用哈希验证)。
  * TXN-REUSE-01: 在 v1 与 v2 中复用同一行 ID, 但交易批次/源行号不同、投影相同,
    证明只按投影判断未变, 批号/请求 id/源行号不进差量。
  * TXN-MID-01: 只存在于被跳过的 v2, v3 删除 -> 跨中间发布的删除。
  * 每个交易批次各带一条隔离行 -> 证明隔离记录绝不进入镜像/差量包。
  * v4 用新交易批次重发与 v3 完全相同的事实 -> 零行变更包也能切版。

维度 (全部在共享批次 MC-S / MK-S 中预先定义):
  客户 C1: r1[2026-01-01,2026-03-01) r2[2026-03-01,2026-05-01) r3[2026-05-01,NULL)
  合同 K1 : 单一开放世代 (keeper 用, 三版身份恒定)
  合同 K3 : 单一开放世代 (keeper / 行 ID 复用用, 三版身份恒定)
  合同 K2O: r1[...,2026-03-01) r2[2026-03-01,NULL)   —— 开放区间末端 NULL
  合同 K2C: r1[...,2026-03-01) r2[2026-03-01,2026-06-01) —— 同一时态位置但已封口
"""
from __future__ import annotations

import csv
import pathlib

# 四个版本共享的维度批次
C_STABLE = "MC-S"
K_STABLE = "MK-S"
T_B1, T_B2, T_B3, T_B4 = "MT-B1", "MT-B2", "MT-B3", "MT-B4"

DIM_HEADER = ["business_key", "revision_no", "valid_from", "valid_to", "source_batch"]
TXN_HEADER = ["txn_row_id", "event_time", "customer_key", "contract_key",
              "amount", "source_batch"]


def _write_csv(path: pathlib.Path, header: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _customer_rows() -> list[dict]:
    return [
        dict(business_key="C1", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="2026-03-01 00:00:00", source_batch=C_STABLE, tier="standard"),
        dict(business_key="C1", revision_no="2", valid_from="2026-03-01 00:00:00",
             valid_to="2026-05-01 00:00:00", source_batch=C_STABLE, tier="gold"),
        dict(business_key="C1", revision_no="3", valid_from="2026-05-01 00:00:00",
             valid_to="", source_batch=C_STABLE, tier="platinum"),
    ]


def _contract_rows() -> list[dict]:
    return [
        dict(business_key="K1", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="", source_batch=K_STABLE, plan="basic"),
        dict(business_key="K3", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="", source_batch=K_STABLE, plan="steady"),
        # K2O: r2 开放 (valid_to NULL)
        dict(business_key="K2O", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="2026-03-01 00:00:00", source_batch=K_STABLE, plan="basic"),
        dict(business_key="K2O", revision_no="2", valid_from="2026-03-01 00:00:00",
             valid_to="", source_batch=K_STABLE, plan="premium"),
        # K2C: 同一时态位置, r2 末端封口为 2026-06-01
        dict(business_key="K2C", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="2026-03-01 00:00:00", source_batch=K_STABLE, plan="basic"),
        dict(business_key="K2C", revision_no="2", valid_from="2026-03-01 00:00:00",
             valid_to="2026-06-01 00:00:00", source_batch=K_STABLE, plan="premium"),
    ]


def _txn(batch: str, txn_id: str, event_time: str, customer: str, contract: str,
         amount: str) -> dict:
    return dict(txn_row_id=txn_id, event_time=event_time, customer_key=customer,
                contract_key=contract, amount=amount, source_batch=batch)


def _transaction_rows(batch: str) -> list[dict]:
    if batch == T_B1:
        return [
            # v3 中被删除 (两个不同分区)
            _txn(batch, "TXN-DEL-01", "2026-01-10 09:00:00", "C1", "K1", "12.000000"),
            _txn(batch, "TXN-DEL-02", "2026-01-12 09:00:00", "C1", "K1", "13.000000"),
            # 金额仅差百万分之一 (v3), 其余投影不变 (仍挂 K2O r2)
            _txn(batch, "TXN-AMT-01", "2026-03-10 09:00:00", "C1", "K2O", "100.000000"),
            # 未变 keeper: 挂稳定 K1/K3, 世代身份与全部投影三版恒定
            _txn(batch, "TXN-KEEP-01", "2026-01-20 09:00:00", "C1", "K1", "50.000000"),
            _txn(batch, "TXN-KEEP-02", "2026-01-21 09:00:00", "C1", "K3", "70.000000"),
            # 跨版本行 ID 复用: v1/v2 投影相同, 仅交易批次/源行号不同
            _txn(batch, "TXN-REUSE-01", "2026-01-22 09:00:00", "C1", "K3", "80.000000"),
            # 隔离行: 金额不可解析 (不产生事实)
            _txn(batch, "TXN-Q-01", "2026-01-23 09:00:00", "C1", "K3", "not-a-number"),
        ]
    if batch == T_B2:
        return [
            _txn(batch, "TXN-DEL-01", "2026-01-10 09:00:00", "C1", "K1", "12.000000"),
            _txn(batch, "TXN-DEL-02", "2026-01-12 09:00:00", "C1", "K1", "13.000000"),
            _txn(batch, "TXN-AMT-01", "2026-03-10 09:00:00", "C1", "K2O", "100.000000"),
            _txn(batch, "TXN-KEEP-01", "2026-01-20 09:00:00", "C1", "K1", "50.000000"),
            _txn(batch, "TXN-KEEP-02", "2026-01-21 09:00:00", "C1", "K3", "70.000000"),
            # 同 ID 复用: 交易批次与源行号不同, 投影与 v1 完全一致
            _txn(batch, "TXN-REUSE-01", "2026-01-22 09:00:00", "C1", "K3", "80.000000"),
            # 只在 v2 存在: v3 删除 (跨中间发布的删除)
            _txn(batch, "TXN-MID-01", "2026-04-15 09:00:00", "C1", "K2O", "33.000000"),
            # v2 新增: 挂开放合同 K2O -> contract_valid_to 为 NULL
            _txn(batch, "TXN-NEW-01", "2026-05-20 09:00:00", "C1", "K2O", "44.000000"),
            # 隔离行: 缺时间
            dict(txn_row_id="TXN-Q-02", event_time="", customer_key="C1",
                 contract_key="K3", amount="10.000000", source_batch=batch),
        ]
    if batch == T_B3:
        return [
            # TXN-DEL-01/02 在本版消失 -> 真实删除
            # 金额百万分之一更正 (合同/客户世代及其余投影不变)
            _txn(batch, "TXN-AMT-01", "2026-03-10 09:00:00", "C1", "K2O", "100.000001"),
            # keeper 投影与 v1 逐字段相同
            _txn(batch, "TXN-KEEP-01", "2026-01-20 09:00:00", "C1", "K1", "50.000000"),
            _txn(batch, "TXN-KEEP-02", "2026-01-21 09:00:00", "C1", "K3", "70.000000"),
            # REUSE-01 投影不变 (稳定 K3, 交易批次差异不进投影)
            _txn(batch, "TXN-REUSE-01", "2026-01-22 09:00:00", "C1", "K3", "80.000000"),
            # MID-01 在本版消失; NEW-01 改挂封口合同 K2C -> 末端 NULL -> 2026-06-01
            _txn(batch, "TXN-NEW-01", "2026-05-20 09:00:00", "C1", "K2C", "44.000000"),
            # 隔离行: 无客户世代
            _txn(batch, "TXN-Q-03", "2026-05-21 09:00:00", "NO-SUCH-C", "K3", "9.000000"),
        ]
    # T_B4: 新交易批次、与 v3 完全相同的事实内容 -> 发布 v4, 投影与 v3 逐行相同
    return [{**r, "source_batch": batch} for r in _transaction_rows(T_B3)]


# 版本 -> (客户批次, 合同批次, 交易批次); 维度批次四版共享
VERSION_BATCHES = [
    (C_STABLE, K_STABLE, T_B1),
    (C_STABLE, K_STABLE, T_B2),
    (C_STABLE, K_STABLE, T_B3),
    (C_STABLE, K_STABLE, T_B4),
]


def build_mirror_fixtures(root: str | pathlib.Path) -> dict[str, pathlib.Path]:
    """在独立空目录写出全部来源 CSV, 返回 {逻辑名: 路径}。"""
    root = pathlib.Path(root)
    root.mkdir(parents=True, exist_ok=True)
    paths: dict[str, pathlib.Path] = {}
    p = root / f"customers_{C_STABLE.lower()}.csv"
    _write_csv(p, DIM_HEADER + ["tier"], _customer_rows())
    paths[f"customers_{C_STABLE}"] = p
    p = root / f"contracts_{K_STABLE.lower()}.csv"
    _write_csv(p, DIM_HEADER + ["plan"], _contract_rows())
    paths[f"contracts_{K_STABLE}"] = p
    for batch in (T_B1, T_B2, T_B3, T_B4):
        p = root / f"transactions_{batch.lower()}.csv"
        _write_csv(p, TXN_HEADER, _transaction_rows(batch))
        paths[f"transactions_{batch}"] = p
    return paths
