# mitmproxy with custom addons

## sacp

获取易收租用户端小程序的请求参数；首次获取齐全或参数变化时立即查询，之后默认每小时更新。重复捕获相同参数不会额外触发查询。

1. 将客户端 HTTP 代理指向 mitmproxy，打开易收租用户端小程序的房间信息页面。
2. 在 Home Assistant 中配置 MQTT 集成，连接同一 broker 并启用自动发现。
3. 如需准确的每日能源统计，配置 `sacp_ha_url` 和 `sacp_ha_token`，启用下面的历史回填功能。

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
| `sacp_ha_token` | HA 管理员的长期访问令牌；为空时读取环境变量 `SACP_HA_TOKEN` |