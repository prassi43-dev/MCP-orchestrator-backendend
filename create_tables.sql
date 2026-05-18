-- SQL script to create tables in Supabase
-- Run this in your Supabase SQL Editor

-- Tenants table
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    metadata_json JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- MCP Servers table
CREATE TABLE IF NOT EXISTS mcp_servers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    server_id UUID NOT NULL DEFAULT gen_random_uuid(),
    base_url VARCHAR(512) NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    auth_json JSONB DEFAULT '{}',
    fallback_http_base VARCHAR(512),
    handshake_path VARCHAR(255),
    extra_headers_json JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_tools_refresh_at TIMESTAMPTZ,
    UNIQUE(tenant_id, server_id)
);

CREATE INDEX IF NOT EXISTS idx_tenant_server ON mcp_servers(tenant_id, server_id);

-- MCP Tools table
CREATE TABLE IF NOT EXISTS mcp_tools (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    server_id UUID NOT NULL,
    tool_name VARCHAR(255) NOT NULL,
    server_tool_name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    parameters_json JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(tenant_id, server_id, tool_name)
);

CREATE INDEX IF NOT EXISTS idx_tenant_server_tool ON mcp_tools(tenant_id, server_id, tool_name);

-- User profile (email, name) for display; keyed by Supabase auth user id
CREATE TABLE IF NOT EXISTS users (
    auth_user_id UUID PRIMARY KEY,
    email VARCHAR(255),
    full_name VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Link Supabase Auth user (auth.users.id) to a single tenant for API access
CREATE TABLE IF NOT EXISTS user_tenants (
    auth_user_id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_tenants_tenant ON user_tenants(tenant_id);

-- Audit / lifecycle events (one table for all event types)
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

-- Enable Row Level Security (optional, but recommended)
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE mcp_servers ENABLE ROW LEVEL SECURITY;
ALTER TABLE mcp_tools ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_events ENABLE ROW LEVEL SECURITY;

-- Create policies to allow service role to access everything
-- (Since we're using service_role_key, these policies allow full access)
CREATE POLICY "Service role can do everything on tenants" ON tenants
    FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Service role can do everything on mcp_servers" ON mcp_servers
    FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Service role can do everything on users" ON users
    FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Service role can do everything on user_tenants" ON user_tenants
    FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Service role can do everything on mcp_tools" ON mcp_tools
    FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Service role can do everything on audit_events" ON audit_events
    FOR ALL USING (true) WITH CHECK (true);

-- Chat conversation history
CREATE TABLE IF NOT EXISTS chat_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id VARCHAR(255) NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    auth_user_id UUID NOT NULL,
    server_id UUID,
    title VARCHAR(512),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(tenant_id, auth_user_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_tenant_user_updated
    ON chat_sessions(tenant_id, auth_user_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS chat_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chat_session_id UUID NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
    role VARCHAR(32) NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    tool_name VARCHAR(255),
    tool_call_id VARCHAR(255),
    sequence INT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_session_sequence
    ON chat_messages(chat_session_id, sequence);

ALTER TABLE chat_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE chat_messages ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role can do everything on chat_sessions" ON chat_sessions
    FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Service role can do everything on chat_messages" ON chat_messages
    FOR ALL USING (true) WITH CHECK (true);

