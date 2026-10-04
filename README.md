# 地下感知主动预警服务

面向重庆式智能感知设备（压力、可燃气体、健康状态）的**主动预警服务**。接收带设备序号与
采集游标的观测，维护设备安装、校准、健康与维护窗口历史，在**固定规则版本**下完成：

- 乱序去重（重复观测不改变任何统计）
- 质量判定（维护窗口、物理量程、安装/换表身份）
- 校准调整与趋势聚合
- 多级告警的合并、升级、确认、转派、解除与误报复核（每一步都记录原因）
- 显式、可断点续跑的重算任务（新规则只影响后续计算或显式重算）
- 游标与未结告警本地持久化（SQLite），重启后继续处理
- 从原始读数到最终状态的**全过程解释**（HTTP 接口与命令行均可）

## 设计原则

| 痛点 | 对策 |
| --- | --- |
| 换表/校准/短时离线制造大量假告警 | 维护窗口内读数判 `MAINTENANCE`、超量程毛刺判 `OUT_OF_RANGE`，均不参与统计；短时/长时离线由独立扫描判定，恢复即解除 |
| 真正持续恶化淹没在重复消息里 | 同一规则组的异常**合并为一条告警**，重复消息记 `SUPPRESSED`；仅级别跃迁时 `ESCALATED`；另设趋势规则捕捉未越限但持续上行 |
| 规则一改，历史全乱 | 规则集**版本不可变**；每条观测、每个告警事件都记录评估时版本；新版本只影响切换后的计算，历史只能由**显式重算任务**改写，旧结论标记 `SUPERSEDED` 并留痕 |
| 换表把新旧序列串联 | 每次安装是独立 `installation_id`，观测主键为 `(installation_id, sequence)`；游标、窗口、告警均按安装实例隔离；上报序列号与在役表不符判为换表残留 |
| 重启丢状态 | 游标、设备状态、告警、事件、重算任务全部落 SQLite；崩溃时 `RUNNING` 任务重启自动退回 `PENDING` 续跑，重跑幂等 |

## 目录结构

```
src/sensor_alerts/
  contracts.py   枚举与值对象（质量、级别、告警状态、观测、校准）
  rules.py       固定版本规则集（阈值/趋势/离线），内置 rules-v1
  store.py       SQLite schema 与持久化
  service.py     核心引擎：摄入、质量、评估、告警状态机、重算、解释
  api.py         标准库 HTTP JSON 接口（零第三方依赖）
  cli.py         命令行（含 explain 全过程解释）
tests/           30 个单元/集成/接口测试
```

## 快速开始

需要 Python 3.10+，仅使用标准库。

```bash
# 测试
python -m unittest discover -s tests -v
python -m compileall -q src tests run_cli.py

# 命令行演示：写入一套压力持续恶化 + 维护窗口样本
PYTHONPATH=src python -m sensor_alerts.cli --db data/alerts.db seed-demo
PYTHONPATH=src python -m sensor_alerts.cli --db data/alerts.db alerts --active
PYTHONPATH=src python -m sensor_alerts.cli --db data/alerts.db explain <ALT-ID>

# HTTP 服务
PYTHONPATH=src python -m sensor_alerts.api --db data/alerts.db --port 8080
```

## 告警生命周期

```
                 命中阈值/趋势
  （无） ───────────────────────►  OPEN
                                   │ acknowledge(actor, reason)
                                   ▼
                              ACKNOWLEDGED
                                   │ assign(actor, assignee, reason)
                                   ▼
                                ASSIGNED ──assign──┐
                                   │               │
              连续 N 次合格读数恢复 / 人工 resolve  │
                                   ▼               │
                                RESOLVED           │
                                   │ false-alarm 复核（未结/已解除均可）
                                   ▼
                              FALSE_ALARM
  显式重算任务以新版本重判：旧告警 ──► SUPERSEDED（保留全部事件与原因）
```

- 所有人工操作强制要求 `actor` 与 `reason`，转派还需 `assignee`；
- 合并（`SUPPRESSED`）、升级（`ESCALATED`）、恢复计数（`RECOVERY_OBS`）均由系统记录原因；
- 误报复核可把 `RESOLVED` 改判为 `FALSE_ALARM`，复核说明进事件流。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康与设备最后合格读数 |
| GET | `/rules` | 现行版本与规则内容 |
| POST | `/rules` | 上传规则 JSON 注册**新版本**（不改变在用版本） |
| POST | `/rules/activate` | 切换在用版本（只影响后续计算） |
| POST | `/devices` | 登记设备 `{device_id, metric: pressure|gas|health, name}` |
| POST | `/devices/{id}/installations` | 换表安装 `{serial_no, installed_at}` |
| POST | `/installations/{id}/remove` | 拆除 `{removed_at, reason}` |
| POST | `/devices/{id}/calibrations` | 校准 `{calibration_id, factor, calibrated_at}` |
| POST | `/devices/{id}/maintenance` | 开维护窗口 `{start_at, end_at?, reason}` |
| POST | `/maintenance/{id}/close` | 关闭维护窗口 |
| POST | `/observations` | 摄入 `{device_id, sequence, observed_at, value, serial_no?}` |
| POST | `/sweep/offline` | 离线扫描（可传 `now`） |
| GET | `/alerts?device_id=&active_only=` | 告警列表 |
| GET | `/alerts/{id}` `/events` `/explain` | 告警详情 / 事件流 / 全过程解释 |
| POST | `/alerts/{id}/acknowledge|assign|resolve|false-alarm` | 处置（强制原因） |
| POST | `/recompute` | 创建并立即运行重算 `{device_id, rule_version, reason, from_observed_at?, to_observed_at?}` |
| POST | `/recompute/run-pending` | 续跑未完成任务（重启后调用） |
| GET | `/jobs` `/jobs/{id}` | 重算任务状态与检查点 |

## 规则版本治理

```bash
# 1. 导出现行规则
PYTHONPATH=src python -m sensor_alerts.cli --db data/alerts.db rules --export /tmp/r.json
# 2. 修改 version 与阈值（版本号必须改）
# 3. 注册 → 切换
PYTHONPATH=src python -m sensor_alerts.cli --db data/alerts.db register-rules /tmp/r.json
PYTHONPATH=src python -m sensor_alerts.cli --db data/alerts.db rules --activate rules-v2
# 4. 只重算某设备的历史区间
PYTHONPATH=src python -m sensor_alerts.cli --db data/alerts.db recompute P-1 rules-v2 \
    "季度阈值复核" --from-at 2026-09-01T00:00:00Z --to-at 2026-10-01T00:00:00Z
```

重算语义：旧版本产生、且未被人工定为误报的告警在区间内标记 `SUPERSEDED`（附任务号与
原因）；按新版本回放合格观测生成新告警；任务可重复运行，结论幂等；`FALSE_ALARM` 是
人工结论，重算不会覆盖。

## 数据模型要点

- `observations` 主键 `(installation_id, sequence)`：天然去重，换表后序号可重新从 0 开始。
- `ingestion_cursors`：按安装实例记录最后序号/时刻、合格数、重复数、拒识数。
- `alerts`：规则组 `rule_group`（如 `level:pressure`、`trend:pressure-rising`、`offline`）
  决定合并范围；`rule_version` 固定评估版本；`created_by_job_id` 标识重算产物。
- `alert_events`：只追加的事件流，解释链与审计均以此为准。
- `recompute_jobs`：状态 `PENDING/RUNNING/DONE/FAILED` 与检查点，支持崩溃续跑。
