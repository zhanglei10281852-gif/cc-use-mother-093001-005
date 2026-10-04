# 地下感知主动预警服务

面向重庆式智能感知设备（压力、可燃气体）的主动预警服务：接收带**设备序号 + 采集游标**的观测，
维护设备安装、校准、健康与维护窗口历史，在**固定规则版本**下完成乱序去重、质量判定、
趋势聚合和多级告警；告警的合并、升级、确认、转派、解除与误报复核全程留下原因。

纯 Python 标准库实现（SQLite + http.server + argparse/unittest），无第三方依赖。

## 领域语义（如何消化"假告警"）

| 运维噪声 | 处理机制 |
| --- | --- |
| 换表后游标归零、新旧序列混淆 | 每次安装生成独立 `install_id`（`DEV#I1`、`DEV#I2`…），观测按时间归入当时安装；游标、去重、桶、告警全部按 install 隔离，**新旧序列永不串联** |
| 网络重发、重复消息 | `UNIQUE(install_id, sequence)` 去重，重复观测直接返回 `duplicate=true`，**不触达任何统计** |
| 校准造成的跳变 | 观测按当时生效校准系数换算；校准生效后 `calibration_settle_s` 内为 `suspect` |
| 短时离线后的恢复首点 | 与上一条历史观测间隔超过 `short_offline_gap_s` 判 `suspect`，不参与聚合 |
| 换表/校准维护期 | 维护窗口内观测判 `suppressed`，只计数、**不触发告警** |
| 设备故障 / 超物理量程 | `bad`，只计数不进趋势 |
| 真正持续恶化被淹没 | 只有 GOOD 值进 5 分钟聚合桶；连续 N 桶越限才 WARN，连续 M 桶达危急阈值才 CRITICAL，缺失桶（离线缺口）**重置连击**，连续 K 桶回落才自动解除 |
| 调整规则怕污染历史结论 | 处理时**钉选当前规则版本**并写入观测/桶/告警；新版本只影响后续数据；历史只能通过**显式重算任务**改变 |

### 桶定稿与乱序/迟到

- 桶在「更晚的桶已有数据」或墙钟 `sweep` 到桶结束时刻时**定稿（finalize）**；定稿前允许乱序补点。
- 定稿后才到的旧观测标 `late=1`，照常入库、可被解释查询看到，但**不改写已定稿统计**；要修正只能发起显式重算。

### 规则版本

规则整体版本化（`rule_versions` 表，payload 冻结）。默认 v1：

- pressure：WARN 高 4.0 / CRIT 高 5.0 MPa（含低报），桶宽 300s，连续 2 桶触发、2 桶危急、2 桶恢复，斜率 0.25/分钟
- gas：WARN 高 25 %LEL / CRIT 高 50，斜率 5/分钟

### 告警生命周期（每一步都有带原因的事件）

`CREATED → MERGED（后续桶并入）→ LEVEL_CHANGED（自动升级）`，人工可：
`ack 确认 / assign 转派 / escalate 手动升级 / resolve 解除 / merge 合并 / review 误报复核`。

- 确认、转派、升级、解除、误报复核、合并**必须填原因**，否则报错。
- 每次状态/级别迁移写 `alert_events`（操作人、原因、前后状态、时间、明细）。
- 自动解除：连续 `recover_streak` 个聚合桶回落；手动解除/误报关闭后不再被自动状态机改写。

### 显式重算任务（可断点续跑）

`recalc_jobs` 三阶段：`queued`（保存人工处置痕迹 → 重算质量 → 重建桶，单事务可整体重试）
→ `rebuilding`（**逐桶小事务评估并推进 `cursor_bucket`**）→ `done`（按新版本重放告警状态机并恢复人工痕迹）。
进程崩溃重启后，`pending/running` 任务被重新领取，从最后检查点继续；误报判定、确认、转派、升级等人工历史保留。

## 运行

```bash
# 测试
python -m unittest discover -s tests -v
# 编译检查
python -m compileall -q src tests run_cli.py
# 契约冒烟
python run_cli.py
# HTTP 服务
PYTHONPATH=src python -m sensor_alerts.cli --db data/a.db serve --port 8080
```

## CLI 示例

```bash
DB="--db data/a.db"
python src/sensor_alerts/cli.py $DB register-device P1 --metric pressure \
    --hard-min -1 --hard-max 100 --installed-at 2026-10-04T08:00:00+00:00
python src/sensor_alerts/cli.py $DB calibration P1 --id CAL1 --factor 1.02 \
    --at 2026-10-04T09:00:00+00:00 --note "周期校准"
python src/sensor_alerts/cli.py $DB maintenance P1 --id W1 \
    --start 2026-10-04T09:00:00+00:00 --end 2026-10-04T09:30:00+00:00 --reason "换表复检"
python src/sensor_alerts/cli.py $DB health P1 --status degraded --at 2026-10-04T10:00:00+00:00 --note "漂移"
python src/sensor_alerts/cli.py $DB replace-meter P1 --at 2026-10-04T12:00:00+00:00 --reason "到期轮换"

python src/sensor_alerts/cli.py $DB ingest observations.json   # 支持单条或 {"observations":[...]}
python src/sensor_alerts/cli.py $DB sweep --now 2026-10-04T12:30:00+00:00
python src/sensor_alerts/cli.py $DB alerts
python src/sensor_alerts/cli.py $DB events AL-xxxx
python src/sensor_alerts/cli.py $DB ack AL-xxxx --actor zhang --reason "已电话通知巡检组"
python src/sensor_alerts/cli.py $DB assign AL-xxxx --actor zhang --assignee 班组A --reason "专业对口"
python src/sensor_alerts/cli.py $DB escalate AL-xxxx --actor li --reason "现场有泄漏迹象"
python src/sensor_alerts/cli.py $DB review AL-xxxx --actor 专家王 --reason "标定气体未接，误报"
python src/sensor_alerts/cli.py $DB resolve AL-xxxx --actor zhang --reason "现场处置完成压力恢复"

# 全链路解释：原始读数 → 校准 → 质量原因 → 桶聚合 → 触发阈值 → 告警时间线
python src/sensor_alerts/cli.py $DB explain-alert AL-xxxx
python src/sensor_alerts/cli.py $DB explain-observation --device P1 --sequence 7

# 规则版本与显式重算
python src/sensor_alerts/cli.py $DB rules
python src/sensor_alerts/cli.py $DB create-rule rules_v2.json --note "降低气体门限"
python src/sensor_alerts/cli.py $DB recalc-create P1 --rule-version 2 --from-bucket 2026-10-04T09:00:00+00:00
python src/sensor_alerts/cli.py $DB recalc-run
python src/sensor_alerts/cli.py $DB recalc-list
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康与当前规则版本 |
| POST | `/devices` | 注册设备并首次安装 |
| POST | `/devices/{id}/replace-meter` | 换表（开启新 install 序列）|
| GET | `/devices/{id}/installs` `/cursor` `/explain?sequence=N` | 安装序列、游标、单观测解释 |
| POST | `/devices/{id}/calibrations` `/health` `/maintenance` | 校准/健康/维护窗口 |
| POST | `/observations` | 接入单条或批量观测 |
| POST | `/sweep` | 墙钟定稿（body 可传 `{"now": ...}`）|
| GET | `/alerts` `/alerts/{id}` `/alerts/{id}/events` `/alerts/{id}/explain` | 告警与时间线 |
| POST | `/alerts/{id}/ack|assign|escalate|resolve|review` `/merge` | 处置（均需 `reason`）|
| GET/POST | `/rules` | 规则版本列表 / 新建版本（只影响后续）|
| POST/GET | `/recalc/jobs` `/recalc/run` | 创建/运行显式重算任务 |

## 持久化

单文件 SQLite（默认 `data/sensor_alerts.db`，WAL 模式）：游标 `dedup_cursor`、未结/历史告警、
告警事件、重算任务与检查点均落盘，重启后游标继续去重、未完成重算自动续跑。

## 代码结构

```
src/sensor_alerts/
  contracts.py   领域常量与不可变契约（观测/校准/健康/维护窗口/事件类型）
  storage.py     SQLite schema、时间序列化、事务
  rules.py       固定版本规则与版本注册表
  registry.py    设备、安装/换表、校准、健康、维护窗口（as-of 查询）
  processor.py   接入去重、质量判定、桶定稿、告警状态机、解释、重算任务
  api.py         HTTP 接口
  cli.py         命令行
tests/           unittest 测试（26 个）
```
