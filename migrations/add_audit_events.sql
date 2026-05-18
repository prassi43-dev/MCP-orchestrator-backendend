-- Add audit_events table (run in Supabase SQL Editor if you already have existing tables)
-- New installs: use create_tables.sql which includes this.

CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(128) NOT NULL,
    tenant_id UUID REFERENCES tenants(tenant_id) ON DELETE SET NULL,
    auth_user_id UUID,
    server_id UUID,
    payload JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_created ON audit_events(tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_type ON audit_events(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_events_auth_user ON audit_events(auth_user_id, created_at DESC);

ALTER TABLE audit_events ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role can do everything on audit_events" ON audit_events
    FOR ALL USING (true) WITH CHECK (true);
