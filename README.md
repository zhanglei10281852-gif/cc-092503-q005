# 科研样品全生命周期管理服务

这是一个面向科研机构样品库、实验室和课题组的模块化后端，集中管理样品接收、分装、借用、归还、消耗、销毁、库存盘点、谱系事件、保管位置、异常记录、登录权限、审计以及可恢复后台任务。项目使用 FastAPI 与 SQLite，所有运行数据保存在单个本地数据库文件中，不依赖另行部署的数据库、缓存或消息队列。

## 已有能力

- 身份与权限：支持引导管理员、登录、会话、用户、角色和细粒度权限。
- 批次与二维码：接收批次保存项目、数量和稳定二维码载荷。
- 样品档案：登记样品、数量、单位、保管位置和生命周期状态。
- 分装谱系：一次事务内扣减母样、创建子样、记录损耗与事件链。
- 借用归还：保存借用数量、到期时间、部分归还和最终归还状态。
- 预留与排队工作流：借用/续借申请按"优先级降序 + 提交顺序升序"排队，获批时在事务内原子占用可借数量；部分归还释放库存后自动推进满足条件的候补，后位申请不得越过队首。
- 续借审批：续借作为特殊申请排队，前方存在等待中的借用申请（含待审批与已批准候补）时禁止顺延，防止占用者借续借长期插队。
- 逾期召回：逾期扫描任务可安全重跑，同一借用的同一到期周期只生成一条召回记录，部分归还或续借后召回自动闭环。
- 重校验：样品隔离、进入销毁审批或数量变化时重新校验未执行申请，不可执行者置为失效，已占用的借用与预留保留。
- 库存台账：每次预留占用/释放写入 `loan_reservation_ledger`，可经预约状态接口查询库存影响。
- 权限留痕：越权访问以 `outcome=denied` 写入审计，可通过审计 API 查询。
- 实验消耗：使用幂等键登记消耗，防止重复请求二次扣减。
- 位置脱敏：普通权限只能看到受限位置的替代码，授权人员可查看精确位置。
- 双人审批：高风险操作要求申请人与审批人分离，并累计不同审批人的决定。
- 异常追踪：异常可以关联样品或接收批次，保存严重度和处理状态。
- 审计与任务：关键身份及业务操作留痕，后台任务支持去重、领取与完成。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/samples.db`，可用 `SAMPLE_DATABASE_PATH` 指定其他路径。

## 初始化与完整性检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 测试

```bash
python -m pytest
```

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
```

## 借用预留工作流接口

- `POST /api/loans/requests`：提交借用申请（需要 `loans.apply`），按优先级排队，不立即占用库存。
- `GET /api/loans/requests` / `GET /api/loans/requests/{id}`：查询队列与申请详情。
- `POST /api/loans/requests/{id}/approve|reject|cancel`：审批、驳回或取消；获批时原子占用，库存不足则成为已批准候补。
- `POST /api/loans/{loan_id}/renewals`：申请续借；前方有等待借用时审批被拒绝。
- `POST /api/loans/{loan_id}/returns`：部分或全部归还，释放预留并自动推进候补。
- `GET /api/loans/samples/{sample_id}/reservation`：查询总量、已预留、可借数量、队列与预留台账。
- `POST /api/loans/overdue/scan`：扫描逾期借用并生成召回记录（可重跑、按到期周期去重）。
- `GET /api/loans/recalls`：查询召回记录。
- `POST /api/samples/{id}/quarantine` 与 `/quarantine/release`：隔离与解除，触发申请重校验。
