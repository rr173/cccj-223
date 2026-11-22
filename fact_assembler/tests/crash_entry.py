"""故障注入入口: 在发布事务提交前 os._exit 强退 (供并发/崩溃测试用)。

由 test_e2e 通过 subprocess 以 ``python3 -I`` 调起, 不依赖环境变量。
参数: db_path customer_batch contract_batch transaction_batch
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import bootstrap  # noqa: F401

from factasm import FactAssembler  # noqa: E402


def main() -> int:
    db, cb, kb, tb = sys.argv[1:5]
    asm = FactAssembler(db)
    asm.assemble(
        customer_batch=cb,
        contract_batch=kb,
        transaction_batch=tb,
        crash_before_commit=True,
    )
    # 正常情况下不会走到这里: 装配器在提交前已 os._exit(87)
    print("ERROR: crash injection did not fire", file=sys.stderr)
    return 99


if __name__ == "__main__":
    sys.exit(main())
