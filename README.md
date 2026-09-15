# mitmproxy with custom addons

## sacp

获取易收租用户端小程序的请求参数；配置 MQTT 后，首次获取齐全或参数变化时立即查询并向 Home Assistant 发布一次水电数据，之后默认每小时更新。重复捕获相同参数不会额外触发更新。

1. 将客户端 HTTP 代理指向 mitmproxy，打开易收租用户端小程序的房间信息页面。
2. 在 Home Assistant 中配置 MQTT 集成，连接同一 broker 并启用自动发现。

请求参数仅保存在内存中，重启后需重新获取。

| 配置 | 默认值 / 说明 |
| --- | --- |
| `sacp_ha_mqtt_host` | broker 地址，不填则关闭 Home Assistant 功能 |
| `sacp_ha_mqtt_port` | `1883` |
| `sacp_ha_mqtt_username`、`sacp_ha_mqtt_password` | 可选，省略时匿名连接 |
| `sacp_ha_mqtt_client_id` | `sacp`，多个实例需使用不同 ID |
| `sacp_ha_mqtt_prefix` | `sacp`，状态主题前缀 |
| `sacp_ha_discovery_prefix` | `homeassistant`，发现主题前缀 |
| `sacp_ha_interval` | `3600`，更新间隔（秒） |
