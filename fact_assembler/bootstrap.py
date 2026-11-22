"""运行引导: 在 Python 隔离模式 (-I) 下显式装配可信路径。

`-I` 会忽略 PYTHONPATH 且不把 cwd 放进 sys.path, 但脚本所在目录仍在
sys.path[0]。本模块把项目内的 vendor DuckDB 与 src 包显式插入, 避免依赖
环境变量, 也防止工作目录中被植入的同名模块 (如 json.py) 劫持导入。
"""
from __future__ import annotations

import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parent
_VENDOR = _ROOT / "vendor" / "duckdb_pkg"
_SRC = _ROOT / "src"

for p in (str(_VENDOR), str(_SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)
