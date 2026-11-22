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


# --------------------------------------------------------------------------- #
# 月结分摊 (allocate) 相关异常
# --------------------------------------------------------------------------- #
class AllocationError(FactAssemblerError):
    """分摊账簿无法交付的基类; 任何失败都不会留下可被当成成品的目标文件。"""

    code = "ALLOCATION_ERROR"


class InvalidRulesError(AllocationError):
    """分摊规则本身无效: 重复节点/边/合同入口、未声明节点、非法权重、
    出边基点之和不为 10000、或图中任意环 (含未被交易使用的部分)。"""

    code = "INVALID_ALLOCATION_RULES"


class VersionNotFoundError(AllocationError):
    """指定的已发布版本不存在。"""

    code = "VERSION_NOT_FOUND"


class FactDatabaseNotFoundError(AllocationError):
    """事实库文件本身不存在。"""

    code = "FACT_DB_NOT_FOUND"


class MissingRootEntryError(AllocationError):
    """所选版本中存在交易, 其合同世代在规则 roots 中没有入口。"""

    code = "MISSING_ROOT_ENTRY"


class LedgerExistsError(AllocationError):
    """目标账簿文件已存在; 为防误覆盖, 拒绝交付。"""

    code = "LEDGER_EXISTS"


# --------------------------------------------------------------------------- #
# 回单勾稽台账 (reconcile) 相关异常
# --------------------------------------------------------------------------- #
class ReconciliationError(FactAssemblerError):
    """勾稽台账无法初始化/应用批次的基类; 拒绝时台账不改变。"""

    code = "RECONCILIATION_ERROR"


class ReconcileLedgerError(ReconciliationError):
    """作为固定入账侧的分摊账簿不可用 (不存在、不是 allocate 交付的账簿等)。"""

    code = "RECONCILE_LEDGER_INVALID"


class StateExistsError(ReconciliationError):
    """勾稽库已存在; 已有勾稽库不接受重新初始化。"""

    code = "RECONCILE_STATE_EXISTS"


class StateNotFoundError(ReconciliationError):
    """勾稽库不存在或尚未初始化。"""

    code = "RECONCILE_STATE_NOT_FOUND"


class BatchSequenceError(ReconciliationError):
    """批号不连续、回退或内容冲突: 只接受从 1 连续递增且未应用过的批号。"""

    code = "RECONCILE_BATCH_SEQUENCE"


class ChangesValidationError(ReconciliationError):
    """变更文件格式错误、引用不存在的明细、重复修改/关联或撤销未知回单。"""

    code = "RECONCILE_BATCH_REJECTED"


# --------------------------------------------------------------------------- #
# 离线事实镜像 (mirror) 相关异常
# --------------------------------------------------------------------------- #
class MirrorError(FactAssemblerError):
    """差量包导出/应用失败的基类; 应用失败时镜像保持可查且版本不前进。"""

    code = "MIRROR_ERROR"


class MirrorVersionNotFoundError(MirrorError):
    """导出引用的起点/目标版本在事实库中不存在, 或不是已发布版本。"""

    code = "MIRROR_VERSION_NOT_FOUND"


class MirrorInvalidBaseError(MirrorError):
    """导出参数无效: 起点与目标相同、目标不是区别于起点的另一个版本等。"""

    code = "MIRROR_INVALID_BASE"


class MirrorPackageInvalidError(MirrorError):
    """差量包结构/内容无效 (JSON 损坏、未知字段、操作缺键、声明与按键推导不符等)。"""

    code = "MIRROR_PACKAGE_INVALID"


class MirrorBaseMismatchError(MirrorError):
    """落地前核对失败: 本地镜像的版本 ID 或内容指纹与包声明的起点不符
    (包括对已前进镜像重复投递旧包)。镜像原样保留。"""

    code = "MIRROR_BASE_MISMATCH"


class MirrorTargetCheckError(MirrorError):
    """落地后核对失败: 更新后的镜像内容指纹与导出端按目标事实算出的指纹不符。
    事务回滚, 镜像版本与数据均不变。"""

    code = "MIRROR_TARGET_CHECK_FAILED"


class MirrorStateInvalidError(MirrorError):
    """给定的 DuckDB 文件存在但不是本功能建立/维护的镜像。"""

    code = "MIRROR_STATE_INVALID"
