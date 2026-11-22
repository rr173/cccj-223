"""命令行接口: 批次登记/装配/查询/追溯/清理/月结分摊。

示例 (均以 `python3 -I cli.py ...` 运行, 路径显式传入):
  python3 -I cli.py register --db run/warehouse.duckdb --kind customer \
      --batch C_B1 --file data/customers_b1.csv
  python3 -I cli.py assemble --db run/warehouse.duckdb \
      --customer-batch C_B1 --contract-batch K_B1 --transaction-batch T_B1
  python3 -I cli.py trace --db run/warehouse.duckdb --txn TXN-007
  python3 -I cli.py allocate --db run/warehouse.duckdb --version ver-... \
      --rules rules.json --out run/ledger.duckdb
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

# -I 模式下不读 PYTHONPATH/用户站点; 显式把项目根加入, 再导入同目录 bootstrap。
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import bootstrap  # noqa: F401,E402  路径装配必须先于 factasm 导入

from factasm import (
    AllocationError,
    AssemblyRejected,
    BatchConflictError,
    BatchNotFoundError,
    FactAssembler,
    allocate,
    load_rules_file,
)
from factasm.sources import register_batch


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def cmd_register(args) -> int:
    info = register_batch(
        args.db, batch_id=args.batch, source_kind=args.kind, path=args.file,
    )
    _print(info)
    return 0


def cmd_assemble(args) -> int:
    asm = FactAssembler(args.db)
    try:
        result = asm.assemble(
            customer_batch=args.customer_batch,
            contract_batch=args.contract_batch,
            transaction_batch=args.transaction_batch,
            crash_before_commit=args.crash_before_commit,
            delay_before_commit=args.delay_before_commit,
        )
    except AssemblyRejected as exc:
        _print({
            "status": "REJECTED",
            "request_id": exc.request_id,
            "rejections": exc.rejections,
            "quarantine": exc.quarantine,
        })
        return 2
    _print({
        "request_id": result.request_id,
        "version_id": result.version_id,
        "status": result.status,
        "reused": result.reused,
        "fact_count": result.fact_count,
        "quarantine_count": result.quarantine_count,
        "batches": {
            "customer": result.customer_batch,
            "contract": result.contract_batch,
            "transaction": result.transaction_batch,
        },
    })
    return 0


def cmd_facts(args) -> int:
    asm = FactAssembler(args.db)
    rows = asm.query_facts(version_id=args.version, where=args.where)
    _print(rows)
    return 0


def cmd_quarantine(args) -> int:
    asm = FactAssembler(args.db)
    rows = asm.query_quarantine(version_id=args.version, reason_code=args.reason)
    _print(rows)
    return 0


def cmd_trace(args) -> int:
    asm = FactAssembler(args.db)
    _print(asm.trace_transaction(args.txn))
    return 0


def cmd_versions(args) -> int:
    asm = FactAssembler(args.db)
    _print({
        "current": asm.current_version(),
        "versions": asm.list_versions(),
        "rejections": asm.list_rejections(),
    })
    return 0


def cmd_cleanup(args) -> int:
    asm = FactAssembler(args.db)
    plan = asm.cleanup_batch(args.batch, dry_run=not args.force)
    _print(plan)
    return 0 if not plan.get("protected_by_published_facts") else 3


def cmd_check(args) -> int:
    from factasm.sources import inspect_batch
    _print(inspect_batch(args.db, args.batch, args.kind))
    return 0


def cmd_allocate(args) -> int:
    rules = load_rules_file(args.rules)
    result = allocate(
        args.db,
        version_id=args.version,
        rules=rules,
        out_path=args.out,
    )
    _print(result.as_dict())
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="offline fact assembler")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("register", help="register a source batch")
    r.add_argument("--db", required=True)
    r.add_argument("--kind", required=True, choices=["customer", "contract", "transaction"])
    r.add_argument("--batch", required=True)
    r.add_argument("--file", required=True)
    r.set_defaults(func=cmd_register)

    a = sub.add_parser("assemble", help="assemble and publish an explicit batch combination")
    a.add_argument("--db", required=True)
    a.add_argument("--customer-batch", required=True)
    a.add_argument("--contract-batch", required=True)
    a.add_argument("--transaction-batch", required=True)
    a.add_argument("--crash-before-commit", action="store_true",
                   help="fault injection: hard-exit before commit")
    a.add_argument("--delay-before-commit", type=float, default=0.0,
                   help="seconds to sleep inside the open transaction (concurrency tests)")
    a.set_defaults(func=cmd_assemble)

    f = sub.add_parser("facts", help="query published facts")
    f.add_argument("--db", required=True)
    f.add_argument("--version", default=None)
    f.add_argument("--where", default=None, help="optional SQL filter, e.g. amount > 100")
    f.set_defaults(func=cmd_facts)

    q = sub.add_parser("quarantine", help="query quarantine list")
    q.add_argument("--db", required=True)
    q.add_argument("--version", default=None)
    q.add_argument("--reason", default=None)
    q.set_defaults(func=cmd_quarantine)

    t = sub.add_parser("trace", help="trace one transaction by source row identity")
    t.add_argument("--db", required=True)
    t.add_argument("--txn", required=True)
    t.set_defaults(func=cmd_trace)

    v = sub.add_parser("versions", help="show current/historical versions and rejections")
    v.add_argument("--db", required=True)
    v.set_defaults(func=cmd_versions)

    c = sub.add_parser("cleanup", help="clean up a historical batch (protected if referenced)")
    c.add_argument("--db", required=True)
    c.add_argument("--batch", required=True)
    c.add_argument("--force", action="store_true", help="actually delete; default is dry-run")
    c.set_defaults(func=cmd_cleanup)

    k = sub.add_parser("check", help="inspect a registered batch")
    k.add_argument("--db", required=True)
    k.add_argument("--kind", required=True, choices=["customer", "contract", "transaction"])
    k.add_argument("--batch", required=True)
    k.set_defaults(func=cmd_check)

    al = sub.add_parser(
        "allocate",
        help="month-end allocation: deliver a standalone ledger for one published version",
    )
    al.add_argument("--db", required=True, help="fact database (opened read-only)")
    al.add_argument("--version", required=True, help="published version_id to allocate")
    al.add_argument("--rules", required=True, help="JSON allocation rules file")
    al.add_argument("--out", required=True, help="target ledger .duckdb (must not exist)")
    al.set_defaults(func=cmd_allocate)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (BatchConflictError, BatchNotFoundError) as exc:
        _print({"error": exc.code, "message": exc.message, "details": exc.details})
        return 4
    except AllocationError as exc:
        _print({"error": exc.code, "message": exc.message, "details": exc.details})
        return 5


if __name__ == "__main__":
    sys.exit(main())
