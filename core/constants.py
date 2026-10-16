"""常量与映射表。"""

from astrbot.api import message_components as Comp

# 反序列化时可还原的组件类型。Text 是 Plain 的别名。
COMPONENT_MAP = {
    "Plain": Comp.Plain,
    "Text": Comp.Plain,
    "Image": Comp.Image,
    "Face": Comp.Face,
    "At": Comp.At,
    "Video": Comp.Video,
    "Record": Comp.Record,
    "File": Comp.File,
    "Json": Comp.Json,
    "Reply": Comp.Reply,
}

# 缓存记录中出现这些类型以外的组件时，会在通知里提示「包含不支持的组件」。
# Forward 不在 COMPONENT_MAP 中（它由 relay/recovery 单独处理），但属于已知类型。
SUPPORTED_COMPONENT_TYPES = set(COMPONENT_MAP) | {"Forward"}

# 「特殊内容」：伪造聊天记录失败后禁止按原消息 ID 单条转发（会变成 Bot 视角的中转记录），
# 只能走组件直发。
SPECIAL_COMPONENT_TYPES = ("Video", "Record", "Json", "File", "Forward")
SPECIAL_COMPONENT_TYPE_SET = frozenset(SPECIAL_COMPONENT_TYPES)

# 特殊内容类型的中文名，用于兜底文案。
SPECIAL_TYPE_CN = {
    "Image": "图片",
    "Video": "视频",
    "Record": "语音",
    "File": "文件",
    "Forward": "合并转发",
    "Json": "小程序",
}

# go-cqhttp 消息段 → 纯文本占位符。
GOCQ_PART_PLACEHOLDER = {
    "image": "[图片]",
    "face": "[表情]",
    "video": "[视频]",
    "record": "[语音]",
    "file": "[文件]",
    "json": "[小程序]",
    "forward": "[合并转发]",
    "at": "[@]",
}

# 配置默认值。
DEFAULT_CACHE_EXPIRATION_TIME = 300
DEFAULT_FILE_SIZE_THRESHOLD_MB = 60

# 伪造聊天记录节点在拿不到真实账号时的兜底 QQ 号。
FALLBACK_NODE_UIN = 10000
