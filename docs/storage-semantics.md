# 数据与兼容边界

物理表包括 meta、collections、versions、commits、receipts、index_defs、index_values、pins、consumers。versions 的主键为集合、键、修订；body 的 SQL NULL 表示删除，JSON null 只能作为文档字段。collections 保存当前模式；index_values 是当前 head 的投影。commits 保存完整的操作序列；receipts 保存不受历史回收影响的请求摘要与结果。幂等键不能被不同操作复用，回放不重新执行前置条件。

floor 是仍可读取的最早修订，不代表每条物理版本的最小修订；每个键在 floor 之前的最后版本可能是构造 floor 读视图的基准，必须保留。commits 仅需保存大于 floor 的提交，消费者从自己的 ack 之后读取。pin 与消费者位置不能低于现存 floor。

分段回收把三种事实分开记录：持久计划只写入目标修订、块大小与暂停状态，不改动 floor 也不删除数据；单次 step 在一个短 IMMEDIATE 事务内删除 `[floor, through]` 区间内被后续版本覆盖的版本和对应 commits，并在同一事务把 floor 设为 `through`。因此对外可见的 floor 永远不会领先于已提交的删除：读取 floor 之上的任何修订，其每个键的基准版本都还在。目标与保留边界取 plan 目标、所有 pin 与消费者 ack 的最小值；边界在 step 的事务内重新读取，因此回收期间新增 pin 或消费者只能保护尚未删除的修订，注册低于当前 floor 的修订会被拒绝。计划、暂停状态与累计计数持久化在 meta 中，关闭并重新打开数据库后用 `resume` 继续。原 `compact(target)` 一次调用在内部使用默认块大小逐段推进至安全边界，返回字段与含义保持不变。

模式包含 fields、required、additional，支持点路径类型；additional=false 限制顶层未知根字段，不递归禁止已声明对象中的子字段。字段不允许 null，除非该字段未声明类型；缺失可通过 required 控制。integer 排除 bool，number 接受整数及有限浮点。JSON 对象不允许非字符串键；浮点必须有限。索引忽略缺失字段与对象/数组，空值作为可索引标量，数值 1 和 1.0 共用索引值，true 与 1 不共用。

游标携带数据库身份、谓词摘要、读版本和最后键，并由数据库私有密钥签名。它不是权限凭据，不冻结模式，也不是保留租约。需要稳定的长时间读取，调用者自行管理具名保留点。

副本包是内部可信通道数据，校验和只用于检测传输损坏。正常备份恢复新主库保留逻辑内容而更换身份；副本初始化保留上游身份。分段追赶未完成时暂不提供当前索引，避免最新唯一约束错误约束中间历史视图。目录 epoch 可在没有文档提交时前进，最终空提交包仍能传递新的模式与索引。
