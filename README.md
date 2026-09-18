# mitmproxy with custom addons

## sacp

获取易收租用户端小程序的请求参数；首次获取齐全或参数变化时立即查询，之后默认每小时更新。重复捕获相同参数不会额外触发查询。

- MQTT：自动发现余额、累计读数、月用量、月费用，以及每块表的抄表时间。
- Home Assistant WebSocket：将每日水电用量回填到实际所属日期，历史读数与统计只保存在 HA。
- 查询、MQTT 发布和历史回填独立运行，一路断线不会阻塞另一路。两种输出可分别启用。

1. 将客户端 HTTP 代理指向 mitmproxy，打开易收租用户端小程序的房间信息页面。
2. 在 Home Assistant 中配置 MQTT 集成，连接同一 broker 并启用自动发现。
3. 如需准确的每日能源统计，配置 `sacp_ha_url` 和 `sacp_ha_token`，启用下面的历史回填功能。

SACP 不创建数据库或数据文件。请求参数仅保存在内存中，重启后需重新获取；统计基准会从 HA 恢复。

| 配置 | 默认值 / 说明 |
| --- | --- |
| `sacp_ha_mqtt_host` | broker 地址，不填则关闭 MQTT 展示 |
| `sacp_ha_mqtt_port` | `1883` |
| `sacp_ha_mqtt_username`、`sacp_ha_mqtt_password` | 可选，省略时匿名连接 |
| `sacp_ha_mqtt_client_id` | `sacp`，多个实例需使用不同 ID |
| `sacp_ha_mqtt_prefix` | `sacp`，状态主题前缀 |
| `sacp_ha_discovery_prefix` | `homeassistant`，发现主题前缀 |
| `sacp_ha_interval` | `3600`，更新间隔（秒） |
| `sacp_ha_url` | HA 基础地址，例如 `http://homeassistant.local:8123`；留空关闭历史回填 |
| `sacp_ha_token` | HA 管理员的长期访问令牌，需与 `sacp_ha_url` 同时填写 |

### 启用历史统计

配置示例（合并到 mitmproxy 的 `config.yaml`；地址和令牌替换为自己的配置）：

```yaml
sacp_ha_mqtt_host: mqtt.example.local
sacp_ha_url: http://homeassistant.local:8123
sacp_ha_token: YOUR_HA_ADMIN_LONG_LIVED_ACCESS_TOKEN
sacp_ha_interval: 3600
```

HA 需要启用 Recorder。实现按 HA Core 2026.9 的原生 WebSocket 协议编写，使用 `recorder/statistics_during_period` 和 `recorder/import_statistics`，不需要安装自定义集成。令牌在 HA 用户个人资料的安全页面创建，所属用户需要管理员权限。程序不会在日志中输出令牌。

首次同步只建立基准；拿到下一天的读数后，开始计算每日用量。HA 的“开发者工具 → 统计”中会出现：

- `sacp:<buildingId>_electricity_<meterId>`：电量，kWh。
- `sacp:<buildingId>_cold_water_<meterId>`：冷水用量，m³。
- `sacp:<buildingId>_hot_water_<meterId>`：热水用量，m³。

这些是外部统计，不是额外的 `sensor.*` 实体。在能源面板中选择名称带 `(Daily)` 的相应统计；不要同时将原 MQTT 累计读数或月用量作为同一项消耗来源，以免重复统计。余额和费用继续通过 MQTT 展示，暂不回填费用。

### 数据日期和故障处理

- `dataItemValue` 是累计表读数，`dataItemValueTime` 按 `Asia/Shanghai` 解析。比如 9 月 11 日零点的读数减去 9 月 10 日零点的读数，得到 9 月 10 日用量。
- 每日增量写入所属日期的最后一个小时。例如 9 月 10 日用量写入当地时间 23:00 对应的统计槽位。每日/月合计可用，小时图的 23 点柱只是日结归属，不表示实际小时用量；不做 24 小时平均分摊。
- 以统计 ID 和抄表时间去重。重复查询、连接重试和重启续算不会再次累计；支持修正 HA 中最新一天的读数。早于最新已同步日期的读数视为过期，不自动改写更早的历史。
- HA 导入返回成功后还会读回确认，确认入库才清除内存中的待发送读数。失败每 30 秒重试；按表和抄表时间合并内存缓存，最多保留 1024 条，超过上限丢弃最早入队的记录并打印警告。SACP 重启或运行时修改配置会丢失未发送缓存。
- 缺少相邻日期时无法确定每天用量：保留已有累计统计，用新读数重新建立基准，并打印警告。**缺口期间的用量不计入回填统计，因此包含缺口的月合计也不完整**；不会把多天合计错误地记到最后一天。无法从当前快照恢复的日期需要历史接口或人工补充。
- 同一表读数下降时也会警告并重新建立基准；更换 `meterId` 会生成独立统计，需要在能源面板配置新表，旧表历史仍保留。
- 历史回填只接受非负、非未来的本地零点读数。上游刷新时间未知，因此保持每小时查询。MQTT 仍可显示上游返回的最新读数和时间。

### 本地验证

```sh
uv sync --locked
uv run ruff check .
uv run ruff format --check .
```
