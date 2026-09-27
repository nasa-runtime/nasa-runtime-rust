-- 业务作用：仅在回退到不提供实例管理查询的 binary 后移除相应索引，不改变实例事实。
ALTER TABLE saga_instance
    DROP INDEX idx_tenant_workflow_status_created_saga,
    DROP INDEX idx_tenant_status_created_saga,
    DROP INDEX idx_tenant_workflow_status_saga,
    DROP INDEX idx_tenant_status_saga,
    DROP INDEX idx_tenant_workflow_created_saga,
    DROP INDEX idx_tenant_created_saga,
    DROP INDEX idx_tenant_workflow_saga,
    DROP INDEX idx_tenant_saga;
