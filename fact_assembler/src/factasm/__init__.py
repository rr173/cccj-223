"""离线事实装配器 (offline fact assembler).

把客户/合同的世代 (generations) 与交易按有效时间做时态关联,
生成可查询事实表, 并为每条交易保留世代选择证据与可定位的隔离清单。

模块入口:
    FactAssembler  -- 装配/发布/查询/追溯/清理的统一门面
"""
from .errors import (
    FactAssemblerError,
    ValidationError,
    BatchConflictError,
    BatchNotFoundError,
    AssemblyRejected,
    ReferenceCheckError,
)
from .assembler import FactAssembler, AssemblyResult

__all__ = [
    "FactAssembler",
    "AssemblyResult",
    "FactAssemblerError",
    "ValidationError",
    "BatchConflictError",
    "BatchNotFoundError",
    "AssemblyRejected",
    "ReferenceCheckError",
]
