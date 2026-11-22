"""回单勾稽 (reconcile) 的测试夹具与可复跑入口。

题目给出的核心例子 (金额单位与账簿一致, 即 1e-6 元):

    入账明细 A、B 分别是 40、60;
    回单 X 是 40 且只指向 A, 回单 Y 是 60 且同时指向 A、B。
    起初只有一个相符组 (X+A, Y+A+B 通过 A 连通)。
    把 Y 改为只指向 B 后, 必须得到两个相符组 (X+A 与 Y+B)。

本模块只负责「造数据」: 一个带 2 条事实的交易批次 -> allocate 账簿 ->
勾稽库初始化。命令行入口见 ``tests/run_reconcile_demo.py``。
"""
from __future__ import annotations

import csv
import json
import pathlib
import shutil
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import bootstrap  # noqa: F401,E402

from factasm import FactAssembler, initialize_state  # noqa: E402
from factasm.allocation import AllocationRules, allocate  # noqa: E402
from factasm.sources import register_batch  # noqa: E402
CUSTOMER_BATCH = "RC-C1"
CONTRACT_BATCH = "RC-K1"
TXN_BATCH = "RC-T1"


def _write_csv(path: pathlib.Path, header: list[str], rows: list[dict]) -> pathlib.Path:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def build_example_ledger(work: pathlib.Path, *, ledger_only: bool = False) -> dict:
    """生成 2 条入账明细 (A=40.00, B=60.00) 的账簿 (可选同时初始化勾稽库)。

    返回路径与关键 id, 不依赖网络、不依赖仓库里已有的库文件。
    交易批次/合同批次 id 由调用参数决定, 便于在同一事实库里并存多个版本。
    """
    work = pathlib.Path(work)
    incoming = work / "incoming"
    incoming.mkdir(parents=True, exist_ok=True)
    db = work / "warehouse.duckdb"
    ledger = work / "ledger.duckdb"
    state = work / "state.duckdb"

    customers = _write_csv(incoming / "customers.csv",
                           ["business_key", "revision_no", "valid_from", "valid_to",
                            "source_batch", "customer_name"], [
        dict(business_key="C", revision_no="1",
             valid_from="2026-01-01 00:00:00", valid_to="",
             source_batch=CUSTOMER_BATCH, customer_name="Example"),
    ])
    contracts = _write_csv(incoming / "contracts.csv",
                           ["business_key", "revision_no", "valid_from", "valid_to",
                            "source_batch", "plan"], [
        dict(business_key="K", revision_no="1",
             valid_from="2026-01-01 00:00:00", valid_to="",
             source_batch=CONTRACT_BATCH, plan="example"),
    ])
    transactions = _write_csv(incoming / "transactions.csv",
                              ["txn_row_id", "event_time", "customer_key",
                               "contract_key", "amount", "source_batch"], [
        dict(txn_row_id="A", event_time="2026-01-15 09:00:00", customer_key="C",
             contract_key="K", amount="40.000000", source_batch=TXN_BATCH),
        dict(txn_row_id="B", event_time="2026-01-16 09:00:00", customer_key="C",
             contract_key="K", amount="60.000000", source_batch=TXN_BATCH),
    ])

    register_batch(db, batch_id=CUSTOMER_BATCH, source_kind="customer", path=customers)
    register_batch(db, batch_id=CONTRACT_BATCH, source_kind="contract", path=contracts)
    register_batch(db, batch_id=TXN_BATCH, source_kind="transaction", path=transactions)
    version_id = FactAssembler(db).assemble(
        customer_batch=CUSTOMER_BATCH, contract_batch=CONTRACT_BATCH,
        transaction_batch=TXN_BATCH).version_id

    rules = AllocationRules.parse(json.dumps({
        "nodes": ["CC_EXAMPLE"],
        "edges": [],
        "roots": {"contract:RC-K1:K:r1": "CC_EXAMPLE"},
    }))
    allocate(db, version_id=version_id, rules=rules, out_path=ledger)
    out = {
        "db": db, "ledger": ledger, "state": state,
        "version_id": version_id,
        "detail_keys": {"A": ("A", "CC_EXAMPLE"), "B": ("B", "CC_EXAMPLE")},
    }
    if not ledger_only:
        out["init"] = initialize_state(ledger, state)
    return out


def write_changes(path: pathlib.Path, lines: list[dict]) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(l, ensure_ascii=False) + "\n" for l in lines),
                    encoding="utf-8")
    return path


def rebuild(work: pathlib.Path) -> dict:
    """清空并重建样例库 (运行时生成, 不入库)。"""
    work = pathlib.Path(work)
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    return build_example_ledger(work)
