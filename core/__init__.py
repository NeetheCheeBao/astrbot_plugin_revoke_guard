"""astrbot_plugin_revoke_guard 内部实现包。

`main.py` 只保留插件入口：Star 子类、事件处理器注册与生命周期。
其余逻辑按职责拆分到本包，`RevokeGuard` 通过混入（mixin）组合这些实现：

- ``constants``  常量与映射表
- ``utils``      与业务无关的通用小工具
- ``components`` 消息组件的序列化/反序列化与文案生成
- ``media``      图片/视频/语音/文件的下载与本地缓存
- ``settings``   配置解析与基于配置的过滤规则
- ``storage``    缓存目录、缓存记录文件、缓存任务调度与轮询
- ``onebot``     OneBot v11 协议端接口辅助（自身账号、群名片、撤回消息）
- ``relay``      合并转发消息的中转
- ``sender``     发送层（伪造聊天记录、单条转发、文本兜底）
- ``cache_flow`` 收到消息 → 写入缓存
- ``recovery``   收到撤回事件 → 恢复内容并通知

注意：AstrBot 以 ``cls.__module__`` 注册插件类，并以
``handler.__module__`` 绑定事件处理器，因此**插件类与带 ``@filter`` 装饰器的
方法必须留在 ``main.py``**，本包内只放不参与注册的实现。
"""
