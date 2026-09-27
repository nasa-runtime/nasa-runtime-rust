-- 业务作用：让租户实例管理查询按 saga_id keyset 前进，并让稀疏创建时间窗先缩小候选集再排序。
ALTER TABLE saga_instance
    ADD INDEX idx_tenant_saga (tenant_id, saga_id),
    ADD INDEX idx_tenant_workflow_saga (tenant_id, workflow_name, saga_id),
    ADD INDEX idx_tenant_created_saga (tenant_id, created_at, saga_id),
    ADD INDEX idx_tenant_workflow_created_saga (tenant_id, workflow_name, created_at, saga_id),
    ADD INDEX idx_tenant_status_saga (tenant_id, status, saga_id),
    ADD INDEX idx_tenant_workflow_status_saga (tenant_id, workflow_name, status, saga_id),
    ADD INDEX idx_tenant_status_created_saga (tenant_id, status, created_at, saga_id),
    ADD INDEX idx_tenant_workflow_status_created_saga (tenant_id, workflow_name, status, created_at, saga_id);
