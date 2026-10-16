<div align="center">
<h1>astrbot_plugin_revoke_guard</h1>
<img src="logo.png" alt="logo" width="128">
<h3>QQ群消息撤回监控（防撤回）</h3>

[![License](https://img.shields.io/badge/License-GPLv3-blue?style=flat-square)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.14+-blue?style=flat-square)](https://www.python.org/)
[![AstrBot](https://img.shields.io/badge/Framework-AstrBot-ff6b6b?style=flat-square)](https://github.com/AstrBotDevs/AstrBot)

</div>

## ✨ 特性

监控撤回行为。支持对`文字`、`图片`、`视频`、`文件`、`聊天记录`、`小程序`消息的撤回行为进行“恢复”

## ⚙️ 插件配置

首次加载后，请在 `AstrBot 后台` -> `插件页面` 找到本插件进行设置

## 📂 插件结构

```
astrbot_plugin_revoke_guard/
├── main.py               # 插件入口：RevokeGuard 类、事件处理器、生命周期
├── core/                 # 实现代码，按职责拆分
│   ├── constants.py      # 常量与映射表（组件类型、文案、默认值）
│   ├── utils.py          # 通用小工具
│   ├── components.py     # 组件序列化/反序列化、组件 → 协议端消息段
│   ├── media.py          # 图片/视频/语音/文件的下载与缓存
│   ├── settings.py       # 配置解析与过滤规则
│   ├── storage.py        # 缓存落盘、缓存任务调度与轮询
│   ├── onebot.py         # OneBot v11 协议端接口辅助
│   ├── relay.py          # 合并转发消息的中转
│   ├── sender.py         # 发送层（伪造聊天记录、转发、文本兜底）
│   ├── cache_flow.py     # 收到消息 → 缓存
│   └── recovery.py       # 收到撤回 → 恢复并通知
├── _conf_schema.json
├── metadata.yaml
├── requirements.txt
├── logo.png
└── LICENSE
```

`main.py` 只保留插件入口，业务逻辑都在 `core/` 下。这样拆分是因为 AstrBot 的两个限制：

- 插件类以 `cls.__module__` 注册，**类必须定义在 `main.py`**；
- 事件处理器的绑定按 `handler.__module__` 匹配，**带 `@filter` 装饰器的方法也必须定义在 `main.py`**。

`RevokeGuard` 通过多重继承把 `core/` 里的各个 mixin 组合起来，两个处理器只做一行转发。
