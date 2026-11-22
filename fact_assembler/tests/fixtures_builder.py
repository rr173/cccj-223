"""手工夹具: 客户 C 在 1 月与 3 月修订, 合同 K 在 2 月切换。

时间轴 (左闭右开):
  客户 C: r1 [2026-01-01, 2026-03-01)  r2 [2026-03-01, +inf)
  合同 K: r1 [2026-01-01, 2026-02-01)  r2 [2026-02-01, +inf)
另含客户 X (无任何世代的悬空键) 与合同 M 重叠冲突夹具 (按需写入)。

所有文件写在调用方指定的独立空目录中, 装配器只通过显式批次 id 使用它们。
"""
from __future__ import annotations

import csv
import pathlib

CUSTOMER_BATCH = "C-B1"
CONTRACT_BATCH = "K-B1"
TXN_BATCH = "T-B1"
BAD_CUSTOMER_BATCH = "C-BAD-OVL"
BAD_CONTRACT_BATCH = "K-BAD-OVL"

# 正常批次的期望事实 (独立测试与参考实现共享的事实基准)
EXPECTED_FACTS = {
    # txn_row_id: (客户修订号, 合同修订号)
    "TXN-001": (1, 1),   # 1/15
    "TXN-002": (1, 1),   # 1/31 23:59:59, 合同切换前一秒仍是 r1
    "TXN-003": (1, 2),   # 恰在合同边界 2/1 00:00 -> 新合同, 旧客户
    "TXN-004": (1, 2),   # 2/14
    "TXN-005": (2, 2),   # 恰在客户边界 3/1 00:00 -> 新客户
    "TXN-006": (2, 2),   # 3/1 00:00:01
    "TXN-007": (2, 2),   # 4/1
}
EXPECTED_QUARANTINE_REASONS = {
    "TXN-101": "MISSING_EVENT_TIME",
    "TXN-102": "BAD_AMOUNT_TYPE",
    "TXN-103": "NO_CUSTOMER_GENERATION",   # 客户 X 无世代
    "TXN-104": "NO_CONTRACT_GENERATION",   # 合同 Z 无世代
    "TXN-001": "DUPLICATE_SOURCE_ROW",     # 第二行复用 TXN-001 身份 (源行 12)
    "TXN-106": "CROSS_BATCH_MISMATCH",     # 声明客户批次 C-OTHER
    "TXN-107": "CROSS_BATCH_MISMATCH",     # 交易行自带 source_batch=T-OTHER
}


def _write_csv(path: pathlib.Path, header: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_fixtures(root: str | pathlib.Path) -> dict[str, pathlib.Path]:
    root = pathlib.Path(root)
    root.mkdir(parents=True, exist_ok=True)

    customers = [
        # 1 月修订
        dict(business_key="C", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="2026-03-01 00:00:00", source_batch=CUSTOMER_BATCH,
             customer_name="Alpha-v1", tier="standard"),
        # 3 月修订, 边界相等不重叠
        dict(business_key="C", revision_no="2", valid_from="2026-03-01 00:00:00",
             valid_to="", source_batch=CUSTOMER_BATCH,
             customer_name="Alpha-v2", tier="gold"),
    ]
    contracts = [
        # 2 月切换
        dict(business_key="K", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="2026-02-01 00:00:00", source_batch=CONTRACT_BATCH,
             plan="basic", monthly_fee="100.00"),
        dict(business_key="K", revision_no="2", valid_from="2026-02-01 00:00:00",
             valid_to="", source_batch=CONTRACT_BATCH,
             plan="premium", monthly_fee="180.00"),
    ]
    transactions = [
        # --- 可交付事实 (覆盖边界) ---
        dict(txn_row_id="TXN-001", event_time="2026-01-15 09:00:00",
             customer_key="C", contract_key="K", amount="10.00", source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-002", event_time="2026-01-31 23:59:59",
             customer_key="C", contract_key="K", amount="20.00", source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-003", event_time="2026-02-01 00:00:00",  # 恰在合同边界
             customer_key="C", contract_key="K", amount="30.00", source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-004", event_time="2026-02-14 12:30:00",
             customer_key="C", contract_key="K", amount="40.00", source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-005", event_time="2026-03-01 00:00:00",  # 恰在客户边界
             customer_key="C", contract_key="K", amount="50.00", source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-006", event_time="2026-03-01 00:00:01",
             customer_key="C", contract_key="K", amount="60.00", source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-007", event_time="2026-04-01 08:00:00",
             customer_key="C", contract_key="K", amount="70.00", source_batch=TXN_BATCH),
        # --- 隔离行 ---
        dict(txn_row_id="TXN-101", event_time="",  # 缺失发生时间
             customer_key="C", contract_key="K", amount="10.00", source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-102", event_time="2026-02-10 00:00:00",
             customer_key="C", contract_key="K", amount="not-a-number",  # 金额类型错误
             source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-103", event_time="2026-02-10 00:00:00",
             customer_key="X", contract_key="K", amount="10.00",  # 无客户世代
             source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-104", event_time="2026-02-10 00:00:00",
             customer_key="C", contract_key="Z", amount="10.00",  # 无合同世代
             source_batch=TXN_BATCH),
        dict(txn_row_id="TXN-105", event_time="2026-01-20 00:00:00",
             customer_key="C", contract_key="K", amount="11.00", source_batch=TXN_BATCH,
             _dup_of="TXN-001"),  # 重复原行身份, 写出时覆盖 txn_row_id
        dict(txn_row_id="TXN-106", event_time="2026-02-10 00:00:00",
             customer_key="C", contract_key="K", amount="12.00", source_batch=TXN_BATCH,
             customer_batch_id="C-OTHER"),  # 跨来源批次不一致
        dict(txn_row_id="TXN-107", event_time="2026-02-10 00:00:00",
             customer_key="C", contract_key="K", amount="13.00",
             source_batch="T-OTHER"),  # 交易自身批次不一致
    ]
    # 处理重复身份: TXN-105 写成 TXN-001 的身份
    for row in transactions:
        row.pop("_dup_of", None)
    transactions[11]["txn_row_id"] = "TXN-001"

    # 重叠冲突夹具: 同键两个世代在 2 月真实重叠 (边界相等不算, 这里做真重叠)
    overlapping_customers = [
        dict(business_key="X", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="2026-02-15 00:00:00", source_batch=BAD_CUSTOMER_BATCH,
             customer_name="X-v1"),
        dict(business_key="X", revision_no="2", valid_from="2026-02-01 00:00:00",
             valid_to="", source_batch=BAD_CUSTOMER_BATCH, customer_name="X-v2"),
    ]
    overlapping_contracts = [
        dict(business_key="M", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="2026-02-15 00:00:00", source_batch=BAD_CONTRACT_BATCH, plan="a"),
        dict(business_key="M", revision_no="2", valid_from="2026-02-10 00:00:00",
             valid_to="", source_batch=BAD_CONTRACT_BATCH, plan="b"),
    ]
    dim_header = ["business_key", "revision_no", "valid_from", "valid_to", "source_batch"]
    paths = {}
    # 属性列并集
    _write_csv(root / "customers_b1.csv",
               dim_header + ["customer_name", "tier"], customers)
    paths["customers"] = root / "customers_b1.csv"
    _write_csv(root / "contracts_b1.csv",
               dim_header + ["plan", "monthly_fee"], contracts)
    paths["contracts"] = root / "contracts_b1.csv"
    _write_csv(root / "transactions_b1.csv",
               ["txn_row_id", "event_time", "customer_key", "contract_key",
                "amount", "source_batch", "customer_batch_id"],
               transactions)
    paths["transactions"] = root / "transactions_b1.csv"

    _write_csv(root / "customers_overlap.csv",
               dim_header + ["customer_name"], overlapping_customers)
    paths["overlap_customers"] = root / "customers_overlap.csv"
    _write_csv(root / "contracts_overlap.csv",
               dim_header + ["plan"], overlapping_contracts)
    paths["overlap_contracts"] = root / "contracts_overlap.csv"
    return paths


# --------------------------------------------------------------------------- #
# 第二代批次: 用于验证版本演进、旧版本仍可读、清理保护
# --------------------------------------------------------------------------- #
CUSTOMER_BATCH_V2 = "C-B2"
CONTRACT_BATCH_V2 = "K-B2"
TXN_BATCH_V2 = "T-B2"


def build_v2_fixtures(root):
    """客户加 r3 (5 月再修订), 合同加 r3 (5 月); 新交易落在 5 月之后。"""
    root = pathlib.Path(root)
    root.mkdir(parents=True, exist_ok=True)
    customers = [
        dict(business_key="C", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="2026-03-01 00:00:00", source_batch=CUSTOMER_BATCH_V2,
             customer_name="Alpha-v1", tier="standard"),
        dict(business_key="C", revision_no="2", valid_from="2026-03-01 00:00:00",
             valid_to="2026-05-01 00:00:00", source_batch=CUSTOMER_BATCH_V2,
             customer_name="Alpha-v2", tier="gold"),
        dict(business_key="C", revision_no="3", valid_from="2026-05-01 00:00:00",
             valid_to="", source_batch=CUSTOMER_BATCH_V2,
             customer_name="Alpha-v3", tier="platinum"),
    ]
    contracts = [
        dict(business_key="K", revision_no="1", valid_from="2026-01-01 00:00:00",
             valid_to="2026-02-01 00:00:00", source_batch=CONTRACT_BATCH_V2,
             plan="basic", monthly_fee="100.00"),
        dict(business_key="K", revision_no="2", valid_from="2026-02-01 00:00:00",
             valid_to="2026-05-01 00:00:00", source_batch=CONTRACT_BATCH_V2,
             plan="premium", monthly_fee="180.00"),
        dict(business_key="K", revision_no="3", valid_from="2026-05-01 00:00:00",
             valid_to="", source_batch=CONTRACT_BATCH_V2,
             plan="enterprise", monthly_fee="300.00"),
    ]
    transactions = [
        dict(txn_row_id="TXN-201", event_time="2026-05-15 09:00:00",
             customer_key="C", contract_key="K", amount="99.00", source_batch=TXN_BATCH_V2),
        dict(txn_row_id="TXN-202", event_time="2026-05-01 00:00:00",  # 恰在 v3 边界
             customer_key="C", contract_key="K", amount="100.00", source_batch=TXN_BATCH_V2),
    ]
    dim_header = ["business_key", "revision_no", "valid_from", "valid_to", "source_batch"]
    _write_csv(root / "customers_b2.csv", dim_header + ["customer_name", "tier"], customers)
    _write_csv(root / "contracts_b2.csv", dim_header + ["plan", "monthly_fee"], contracts)
    _write_csv(root / "transactions_b2.csv",
               ["txn_row_id", "event_time", "customer_key", "contract_key",
                "amount", "source_batch"], transactions)
    return {
        "customers": root / "customers_b2.csv",
        "contracts": root / "contracts_b2.csv",
        "transactions": root / "transactions_b2.csv",
    }


# --------------------------------------------------------------------------- #
# 同名批次重送: 同内容(应幂等) 与 换内容(应冲突)
# --------------------------------------------------------------------------- #
def copy_source(src_path, dst_dir):
    """字节级复制来源文件到新的独立目录, 模拟「同批次从不同路径重送」。"""
    dst_dir = pathlib.Path(dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / pathlib.Path(src_path).name
    dst.write_bytes(pathlib.Path(src_path).read_bytes())
    return dst


def write_mutated_copy(src_path, dst_dir, new_name, mutate):
    """读入来源, 经 mutate(rows) 改内容后写到新目录, 保留同名批次 id。"""
    dst_dir = pathlib.Path(dst_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)
    with pathlib.Path(src_path).open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames
        rows = list(reader)
    rows = mutate(rows)
    dst = dst_dir / new_name
    with dst.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return dst
