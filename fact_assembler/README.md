# 离线事实装配器 (offline fact assembler)

把每天到达的**客户 / 合同 / 交易**三个来源，按各来源的有效时间与装载批次，
把交易关联到正确的客户/合同**世代 (generation)**，产出可查询的版本化事实表，
并为每条交易保留「为什么选中这个世代」的完整匹配证据与可定位的隔离清单。

仅依赖 Python 3.11 标准库 + DuckDB（已 vendor 在 `vendor/duckdb_pkg`，免安装）。

---

## 1. 核心语义

- **世代区间左闭右开 `[valid_from, valid_to)`**；边界相等不视为重叠，
  发生在边界点的交易归后一世代；`valid_to` 为空表示开放区间。
- 同一业务键同一时刻至多一个有效世代。来源自身出现真重叠、逆序区间、
  重复修订号、行声明批次与登记批次不一致等 → **拒绝整批**，不发布任何事实。
- 装配请求**显式指定**客户、合同、交易三个批次 id；绝不按文件名自动拼接。
- 行级问题进**隔离清单**（带原行身份、原因码、排除候选证据），不静默回退到最新世代：
  - `MISSING_EVENT_TIME` 交易发生时间缺失/不可解析
  - `BAD_AMOUNT_TYPE` 金额无法转为 DECIMAL
  - `DUPLICATE_SOURCE_ROW` 交易原行身份重复（首次保留，后续隔离）
  - `CROSS_BATCH_MISMATCH` 跨来源批次不一致
  - `NO_CUSTOMER_GENERATION` / `NO_CONTRACT_GENERATION` 无匹配世代
  - `OVERLAPPING_GENERATION` 命中多个世代（防御路径）
- **批次即契约**（内容指纹 sha256）：
  - 同名同内容重送 → 幂等复用；
  - 同名换内容重送 → `BATCH_CONTENT_CONFLICT` 拒绝。
- 已确认的批次组合重复装配 → 复用既有版本，**不增加事实**。
- 发布在**单个事务**内完成；失败批次只写审计表，不改变当前版本。
- 历史版本只追加、可按 `version_id` 回看；内存只持有当前装配的批次数据，
  世代/交易历史落在磁盘库；历史批次可清理，但仍被已发布事实/隔离引用的受保护。

## 2. 双路径核对（不只输出一条 SQL）

- `factasm/engine.py`：DuckDB 在一次性 staging 库中做半开区间连接（关系计算）。
- `factasm/reference.py`：**完全不使用 DuckDB** 的纯 Python 区间枚举，逐代记录
  候选、左闭右开比较结果、选中/排除原因，形成 `match_evidence`。
- `FactAssembler._cross_check` 对两条路径逐笔交易、逐维度对账候选数与选中的
  世代身份；不一致即 `REFERENCE_CHECK_MISMATCH`，拒绝发布。
- `tests/independent_checker.py` 是第三个独立预言机：自己解析原始 CSV、自己枚举
  区间，不 import 任何 `factasm` 代码，再与落库结果逐项核对。

## 3. 目录结构

```
fact_assembler/
├── bootstrap.py                # -I 模式下显式装配 vendor/src 路径, 防模块劫持
├── cli.py                      # 命令行: register/assemble/facts/quarantine/trace/...
├── requirements.txt
├── vendor/duckdb_pkg/          # 解压的官方 duckdb wheel
├── src/factasm/
│   ├── errors.py               # 带稳定 code 的异常层级
│   ├── util.py                 # 时间/金额解析、指纹、隔离原因码
│   ├── db.py                   # 持久库 schema、连接、staging 库
│   ├── sources.py              # CSV/JSONL 加载、批次登记/幂等/冲突
│   ├── validation.py           # 批次级校验、世代索引、交易解析与行级隔离
│   ├── reference.py            # 独立纯 Python 区间枚举 + 证据
│   ├── engine.py               # DuckDB staging 半开区间连接
│   └── assembler.py            # 门面: 编排/交叉核对/原子发布/查询/追溯/清理
└── tests/
    ├── fixtures_builder.py     # 手工夹具: C 1月/3月修订, K 2月切换 + 异常行
    ├── independent_checker.py  # 第三独立预言机
    ├── reference.py 无; reference 实现在 src
    ├── test_unit.py            # 边界/重叠/去重/校验防御路径 (14 项)
    ├── test_e2e.py             # 端到端 75 项
    └── crash_entry.py          # 提交前 os._exit 强退注入入口
```

## 4. 快速开始

所有命令以 `python3 -I` 运行（隔离模式：忽略 PYTHONPATH/用户站点，cwd 不入
`sys.path`，防止不可信数据目录里的同名模块劫持）。

```bash
cd fact_assembler

# 1) 显式登记三个来源批次
python3 -I cli.py register --db run/wh.duckdb --kind customer    --batch C-B1 --file <dir>/customers_b1.csv
python3 -I cli.py register --db run/wh.duckdb --kind contract    --batch K-B1 --file <dir>/contracts_b1.csv
python3 -I cli.py register --db run/wh.duckdb --kind transaction --batch T-B1 --file <dir>/transactions_b1.csv

# 2) 显式批次组合装配并发布
python3 -I cli.py assemble --db run/wh.duckdb \
    --customer-batch C-B1 --contract-batch K-B1 --transaction-batch T-B1

# 3) 查询事实 / 隔离 / 按交易追溯 / 版本
python3 -I cli.py facts       --db run/wh.duckdb
python3 -I cli.py quarantine  --db run/wh.duckdb --reason NO_CUSTOMER_GENERATION
python3 -I cli.py trace       --db run/wh.duckdb --txn TXN-005
python3 -I cli.py versions    --db run/wh.duckdb
python3 -I cli.py cleanup     --db run/wh.duckdb --batch C-B1          # 默认 dry-run
```

## 5. 追溯接口返回的证据（节选）

`trace --txn TXN-005`（发生在客户修订边界 2026-03-01 00:00:00）返回：

```json
"customer_generation_id": "customer:C-B1:C:r2",
"contract_generation_id": "contract:K-B1:K:r2",
"match_evidence": {
  "selection_rule": "exactly one half-open interval contains event_time",
  "customer": { "candidate_count": 1, "candidates_enumerated": [
    { "generation_id": "customer:C-B1:C:r2", "revision_no": 2,
      "valid_from": "2026-03-01 00:00:00", "valid_to": "infinity",
      "comparison": { "rule": "half_open [valid_from, valid_to)",
                      "ge_valid_from": true, "lt_valid_to": true },
      "selected": true, "exclude_reason": null } ] },
  "contract": { "...": "同样逐代枚举" }
},
"source_summary": { "customer_batch": "C-B1", "contract_batch": "K-B1",
                    "transaction_batch": "T-B1", "...批次内容指纹": "..." }
```

## 6. 测试与需求对照

```bash
python3 -I tests/test_unit.py    # 14 项防御路径
python3 -I tests/test_e2e.py     # 75 项端到端
```

| 题目要求 | 覆盖位置 |
| --- | --- |
| 交易恰在边界、边界前一秒 | e2e §3/§4、unit 半开边界 |
| 没有世代 | e2e §5（TXN-103/104） |
| 两个世代重叠（来源冲突拒批） | e2e §8、unit「真重叠拒批」 |
| 跨来源批次不一致 | e2e §5（TXN-106/107）、§8 rogue 批次 |
| 重复交易原行 | e2e §5、unit 重复身份多原因 |
| 缺时间 / 金额类型错误 | e2e §5（TXN-101/102） |
| 同名批次重送：幂等 + 换内容冲突 | e2e §6 |
| 组合重复装载不增加事实 | e2e §7 |
| 旧版本仍可读 | e2e §11 |
| 独立参考实现区间枚举核对 | `independent_checker.py` + e2e §3 |
| 发布前强退只见旧完整版本 | `crash_entry.py` + e2e §9 |
| 并发读者只见上一版或新版 | e2e §10（多连接 MVCC + 提交前延迟窗口） |
| 失败批次不改变旧版本 | e2e §8/§9 |
| 内存不随历史增长、staging 不堆积 | e2e §14 |
| 历史批次清理但保护在引用来源 | e2e §12 |

## 7. 安全说明

- 来源文件视为不可信数据，放在与代码分离的独立目录，路径由参数显式传入。
- 一律 `python3 -I` 运行；`bootstrap.py` 只把**项目内**可信的 vendor/src 目录
  插入 `sys.path`，工作目录与 `PYTHONPATH` 均不参与导入，已用植入 `json.py`
  的不可信目录验证不会被劫持。
- DuckDB 库文件与 staging 分目录；崩溃残留 staging 由 `cleanup_staging()` 回收。
