# 异步 AI 列（赛题三）

在已有 AI Model 和 Endpoint 配置的基础上，可以声明持久化的异步生成列：

```sql
CREATE TABLE docs (
  id BIGINT PRIMARY KEY,
  content TEXT,
  tags JSON AI COLUMN (AI_COMPLETE('text_model', content))
    WITH (sync_mode='async', max_retries=2),
  emb VECTOR(4) AI COLUMN (AI_EMBED('embed_model', content))
    WITH (sync_mode='async', max_retries=2)
);
INSERT INTO docs(id, content) VALUES (1, 'example');
SHOW AI COLUMNS FROM docs;
SHOW CREATE TABLE docs;
```

支持 `ALTER TABLE ... ADD COLUMN ... AI COLUMN (...)`。表达式必须以
`AI_COMPLETE` 或 `AI_EMBED` 为根，只能依赖普通列和确定性的标量表达式。
模型名称必须是字符串常量，Embedding 的显式维度必须与结果列一致。
JSON、整数和其他数值结果通过单独的 system 消息补充类型要求，保留原始用户输入。
业务 SQL 不能给 AI 结果列直接赋值。

插入、依赖值变更或主键变更时，结果失效和任务写入与业务数据处于同一事务。
无关列更新保留已有结果；回滚恢复结果并撤销任务；删除清理对应任务。
结果和索引仍通过原生 DML 路径维护，包括向量索引的失效、写回和回滚。

列定义及其依赖保存在 Schema 和 `oceanbase.__all_ai_column` 中，每个 AI 列有
三个隐藏列保存状态、请求标识和定义版本。隐藏列通过持久化的列 ID 关联，支持
Schema 拷贝、序列化和列 ID 重映射。添加或重定义 AI 列后，后台为已有行补充任务。

`oceanbase.__all_ai_column_task` 是事务内写入的持久化任务表。后台并发领取请求组，
持有租约，调用现有 AI 函数并通过原生列转换检查结果。结果先暂存于任务中，待同一行
同次变更的所有任务达到终态后，用一个事务发布成功和失败状态。请求标识、任务归属和
当前定义版本共同防止迟到结果覆盖新数据。进程重新启动后会恢复持久化任务。

自动重试受 `max_retries` 限制（默认 2，范围 0～100），失败后延迟重试，耗尽后
标记为 FAILED。PENDING 和 FAILED 的结果均为 SQL NULL；READY 的结果也可以是
SQL NULL，状态独立于结果值。手动重试可以选中任意状态的行：

```sql
ALTER TABLE docs RETRY AI COLUMN tags WHERE status='FAILED' AND id IN (1, 2);
SET SESSION ai_column_strict_read=ON;
SELECT tags FROM docs;
```

`status` 是重试专用伪列，取值为 PENDING、READY、FAILED；反引号引用的
`status` 表示同名业务列。重试只影响指定列及命中的行，不能与其他 ALTER 动作混用。

严格读取默认关闭。开启后，每次执行都会检查语句实际引用的 AI 列，任一引用列存在
非 READY 行即返回错误 11120，包括 SELECT、UPDATE 和 DELETE。检查覆盖整列，
不受用户查询条件限制；只访问普通列或其他全部 READY 的 AI 列不受影响。

## 验证入口

原生 Schema 和 Parser 回归测试：

```bash
cmake --build build_release --target ai_column_tests -j2
./build_release/unittest/ai_column_tests
```

官方集成用例位于 `tools/deploy/mysql_test/test_suite/ai_column/`，依次为
`ai_column_ddl`、`ai_column_t1_dml`、`ai_column_worker`、
`ai_column_retry_strict`、`ai_column_vector_index`。
用例使用本地 Mock Endpoint，无需真实模型凭据。

## 本次验证记录（2026-10-30）

基于官方 `2026-competition` 的 `5bce9e06d1281347ad61344e4b01fab9158c58a2`
实现并使用 Clang 17 完整编译数据库。8 项原生 Schema/Parser 测试通过；在全新
测试实例中执行上述五组官方 Mysqltest，全部通过，测试脚本及预期结果保持原样。

另行验证了成功暂存的 SQL NULL 结果在严格读取下可读、AI 列禁止业务赋值、
SHOW CREATE 输出可直接重建、ADD COLUMN 为已有数据生成结果，以及含引号的
复合主键和主键变更。对隔离测试实例执行 SIGKILL 后，重启验证已提交任务与
READY NULL 状态仍存在，中断租约得到回收，任务能够继续完成并写回。

再次复查提交的干净检出：`build.sh release` 配置成功，已跟踪的 `src` 与 `rust`
内容与实测源码一致。补齐 `pretest` 聚合目标，并通过 CTest 运行原生测试。
编译脚本在成功后将同一个数据库可执行文件发布到现有工作流要求的
`build/bin/src/observer/seekdb`，同时保留 CMake 原有产物位置。
