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
├── cli.py                      # 命令行: register/assemble/facts/quarantine/trace/allocate/...
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
│   ├── assembler.py            # 门面: 编排/交叉核对/原子发布/查询/追溯/清理
│   └── allocation.py           # 月结分摊: 规则校验/合并份额/最大余数/账簿交付
└── tests/
    ├── fixtures_builder.py     # 手工夹具: C 1月/3月修订, K 2月切换 + 异常行
    ├── independent_checker.py  # 第三独立预言机
    ├── test_unit.py            # 边界/重叠/去重/校验防御路径 (14 项)
    ├── test_e2e.py             # 端到端 75 项
    ├── test_allocate.py        # 月结分摊 68 项
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

# 4) 月结分摊: 对指定已发布版本交付独立账簿 (详见 §8)
python3 -I cli.py allocate    --db run/wh.duckdb --version <ver-...> \
    --rules rules.json --out run/ledger.duckdb
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
python3 -I tests/test_unit.py      # 14 项防御路径
python3 -I tests/test_e2e.py       # 75 项端到端
python3 -I tests/test_allocate.py  # 68 项月结分摊
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
| 分摊规则静态校验 (重复/未声明/权重/环) | allocate §1 |
| 分支汇入同一中心按合并份额入账 | allocate §3 |
| 负额/零额/同余数/余数排序 | allocate §4 |
| 跨版本原行 ID 重用按版本隔离 | allocate §5 |
| 反复分合规则不枚举路径 (2⁵¹ 路径秒级) | allocate §6 |
| 缺入口/循环/版本不存在/目标已存在拒绝出账 | allocate §7 |
| 空版本交付空账簿、账簿脱离事实库自足对账 | allocate §8/§9 |
| 分摊不改变事实库 (字节级指纹) | allocate §10 |

## 7. 安全说明

- 来源文件视为不可信数据，放在与代码分离的独立目录，路径由参数显式传入。
- 一律 `python3 -I` 运行；`bootstrap.py` 只把**项目内**可信的 vendor/src 目录
  插入 `sys.path`，工作目录与 `PYTHONPATH` 均不参与导入，已用植入 `json.py`
  的不可信目录验证不会被劫持。
- DuckDB 库文件与 staging 分目录；崩溃残留 staging 由 `cleanup_staging()` 回收。

## 8. 月结分摊账簿 (`allocate`)

月结时，财务要把已装配交易的金额分摊到最终入账的成本中心：合同指定的入口
往往是一个共用费用池，钱可经若干中转池汇入同一个成本中心。`allocate` 消费
**指定已发布版本**的 `facts`，产出一份**脱离事实库也能打开并逐笔对账**的
DuckDB 账簿。

```bash
python3 -I cli.py allocate --db run/wh.duckdb --version ver-336907ee575db15c \
    --rules rules.json --out run/ledger-2026-04.duckdb
```

### 8.1 规则文件 (只有一份)

```json
{
  "nodes": ["POOL_SALES", "HUB_EAST", "HUB_WEST", "CC_EAST", "CC_SHARED"],
  "edges": [
    {"from": "POOL_SALES", "to": "HUB_EAST", "bps": 6000},
    {"from": "POOL_SALES", "to": "HUB_WEST", "bps": 4000},
    {"from": "HUB_EAST", "to": "CC_EAST", "bps": 5000},
    {"from": "HUB_EAST", "to": "CC_SHARED", "bps": 5000},
    {"from": "HUB_WEST", "to": "CC_SHARED", "bps": 10000}
  ],
  "roots": {
    "contract:K-B1:K:r1": "POOL_SALES",
    "contract:K-B1:K:r2": "POOL_SALES"
  }
}
```

- `nodes` 声明全部节点 ID（ASCII 字母/数字/`_`/`-`，区分大小写）；
  **无出边的节点就是最终成本中心**，也允许直接作为入口；有出边的节点只中转。
- `edges` 每条边是正整数基点；**同一节点所有出边之和必须恰为 10000**。
- `roots` 把 `contract_generation_id` 映射到入口节点。
- 重复节点、重复边、重复合同入口（JSON 重复键）、未声明节点、非法权重、
  **任意环**都让规则无效——包括尚未被该版本交易使用的部分（规则整体校验）。

### 8.2 分摊语义

- 一个最终中心的份额 = 从入口到它的**所有路径的权重乘积之和**。先合并精确
  份额（`fractions.Fraction`，规则可能反复汇合，用 DAG 动态规划一次算清，
  **不枚举路径**，也不对共用入口的交易重复展开子图），再分到金额的最小存储
  单位（原金额 6 位小数 × 10⁶ 的整数单位），不沿途取整、不用浮点比较余数。
- 对交易金额取**绝对值**：每个中心先取精确应得单位数的整数下界，剩余单位按
  **小数余数从大到小**、**同余数按中心 ID 的 ASCII 字典序**各补 1，最后恢复
  原金额符号。每个可达中心恰有一行（即使入账额是零）；每笔有符号子额之和
  恒等于原额。
- 只消费指定版本的 `facts`：不重跑世代匹配、不读隔离行、不从当前版本补行；
  版本不存在、或其中任一交易的合同世代在 `roots` 里缺入口，整份账簿不出账。
- 事实按键集分页分批读出、明细分批写入，Python 内存不随事实总量增长。

### 8.3 账簿内容（自足，可直接用 DuckDB 查询）

| 表/视图 | 内容 |
| --- | --- |
| `ledger_meta` | 单行：所选版本 ID、交易批次、规则 canonical JSON 及其 sha256、事实/明细行数 |
| `control` | 逐笔控制数：原行 ID、入选合同世代、入口节点、原额整数单位（有符号 HUGEINT） |
| `allocation` | 入账明细，键 (原行 ID, 成本中心)：有符号整数单位、该中心合并份额的精确分子/分母（任意精度整数文本） |
| `reconciliation` | 核对视图：每笔 控制数 − 明细合计，恒为 0 |

```bash
# 脱离事实库, 直接用 vendored DuckDB 打开账簿对账
python3 -I - <<'EOF'
import sys; sys.path.insert(0, "vendor/duckdb_pkg")
import duckdb
con = duckdb.connect("run/ledger-2026-04.duckdb", read_only=True)
print(con.execute("SELECT * FROM ledger_meta").fetchall())
print(con.execute("SELECT * FROM allocation WHERE txn_row_id='TXN-001'").fetchall())
print(con.execute("SELECT COUNT(*) FROM reconciliation "
                  "WHERE unallocated_units <> 0").fetchall())  # 恒为 [(0,)]
EOF
```

上面 §8.1 的规则里，两条分支汇入 `CC_SHARED`：份额按**合并后**的
`3/10`、`7/10` 入账（`CC_EAST = 6/10×1/2`，`CC_SHARED = 6/10×1/2 + 4/10`），
每个中心恰一行，而不是按路径拆成多行：

```
('TXN-001', 'CC_EAST',  3000000, '3', '10')   -- 10.000000 -> 3/10
('TXN-001', 'CC_SHARED', 7000000, '7', '10')  -- 两条分支合并 7/10
```

### 8.4 交付保证与错误码

- 目标已存在 → 拒绝覆盖（`LEDGER_EXISTS`，退出码 5）。
- 账簿先完整写入同目录临时文件（含核对数据），提交并关闭后用 `os.link`
  **原子就位**；失败只残留 `*.tmp-<pid>` 临时文件并被立即清理，目标路径绝不
  留下能被当成成品的文件；事实库全程只读打开，分摊不改变事实库。
- 合法空版本（已发布但 0 条事实）交付只有规则与版本标识的空账簿。
- 错误码（均退出码 5）：`INVALID_ALLOCATION_RULES` / `VERSION_NOT_FOUND` /
  `FACT_DB_NOT_FOUND` / `MISSING_ROOT_ENTRY` / `LEDGER_EXISTS`。

### 8.5 可复跑验证

```bash
python3 -I tests/test_allocate.py   # 68 项
```

覆盖：规则静态校验全分支；两条分支汇入同一中心的合并份额；负额/零额/同余数
（ASCII 字典序）/余数从大到小；跨版本原行 ID 重用（账簿只含所选版本）；
程序生成的 50 层反复分合规则（入口到任一中心路径数 2⁵¹，仅 2 个入账中心，
秒级完成——证明不枚举路径、不按交易重复展开）；缺入口/循环规则拒绝出账且
不留成品；空版本空账簿；账簿脱离事实库独立对账；分摊前后事实库字节级不变。

