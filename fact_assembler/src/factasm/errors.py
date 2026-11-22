"""装配器的异常层级。

所有异常都携带稳定的 ``code``, 便于调用方按代码分支处理而不是匹配文案。
"""
from __future__ import annotations


class FactAssemblerError(Exception):
    """所有装配器异常的基类。"""

    code = "FACTASM_ERROR"

    def __init__(self, message: str, *, details: object | None = None):
        super().__init__(message)
        self.message = message
        self.details = details

    def __str__(self) -> str:  # pragma: no cover - 调试辅助
        if self.details is not None:
            return f"{self.code}: {self.message} | details={self.details!r}"
        return f"{self.code}: {self.message}"


class ValidationError(FactAssemblerError):
    """来源自身不合法 (时间区间重叠/逆序等批次级硬错误), 必须拒绝批次。"""

    code = "VALIDATION_ERROR"


class BatchConflictError(FactAssemblerError):
    """同名批次重送但内容指纹不同。"""

    code = "BATCH_CONTENT_CONFLICT"


class BatchNotFoundError(FactAssemblerError):
    """引用了未注册的批次。"""

    code = "BATCH_NOT_FOUND"


class AssemblyRejected(FactAssemblerError):
    """装配请求未通过校验或存在无法唯一解释的交易, 批次未发布。

    ``rejections`` 汇总批次级拒绝原因; ``quarantine`` 是行级隔离明细。
    """

    code = "ASSEMBLY_REJECTED"

    def __init__(
        self,
        message: str,
        *,
        request_id: str,
        rejections: list[dict] | None = None,
        quarantine: list[dict] | None = None,
    ):
        super().__init__(message, details={"request_id": request_id})
        self.request_id = request_id
        self.rejections = rejections or []
        self.quarantine = quarantine or []


class ReferenceCheckError(FactAssemblerError):
    """DuckDB 关系计算与独立参考实现结果不一致 (防御性自检失败)。"""

    code = "REFERENCE_CHECK_MISMATCH"
