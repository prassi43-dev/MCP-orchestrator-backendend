# MCP Orchestrator – User Guide

This guide covers end-to-end setup and usage of the MCP Orchestrator: backend, frontend, Supabase, and all APIs.

---

## Table of contents

1. [Overview](#overview)
2. [Backend setup](#backend-setup)
3. [Frontend setup](#frontend-setup)
4. [Supabase setup](#supabase-setup)
5. [API reference](#api-reference)
6. [Running and testing APIs](#running-and-testing-apis)
7. [Architecture and optional configuration](#architecture-and-optional-configuration)
8. [Troubleshooting](#troubleshooting)

---

## Overview

The **MCP Orchestrator** is a central service that:

- **Registers** MCP (Model Context Protocol) servers per tenant
- **Discovers** tools from those servers and caches them
- **Runs** chat requests through an LLM (OpenAI) that can call MCP tools
- **Streams** events (tool calls, assistant replies) via SSE

**Stack:**

- **Backend:** FastAPI (Python), Supabase (PostgreSQL + REST), OpenAI API
- **Frontend:** Angular 18, Supabase Auth (email/password)
- **Auth:** Supabase JWT; backend verifies tokens and scopes all data by tenant

---

## Backend setup

### Prerequisites

- Python 3.10+
- OpenAI API key

### Steps

1. **Go to the backend directory**

   ```bash
   cd mcp-orchestration
   ```

2. **Create a virtual environment (recommended)**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment**

   Create a `.env` file in `mcp-orchestration/` with at least:

   ```env
   OPENAI_API_KEY=sk-your-openai-api-key
   SUPABASE_URL=https://YOUR-PROJECT-REF.supabase.co
   SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
   SUPABASE_JWT_SECRET=your-jwt-secret-base64
   ```

   Optional:

   - `MCP_BASE_URL` – default MCP server URL (default: `http://127.0.0.1:8001`)
   - `MCP_TIMEOUT_SECONDS` – timeout for MCP calls (default: `10.0`)
   - `LOGS_DIR` – directory for log files (default: `logs`)
   - `ORCHESTRATOR_DEBUG` – set to `true` to include raw model trace in chat response

5. **Run the server**

   ```bash
   uvicorn main:app --reload
   ```

   Or alternatively:

   ```bash
   python main.py
   ```

   API base URL: **http://localhost:8000** (by default).

   - Health: **http://localhost:8000/health**
   - OpenAPI docs: **http://localhost:8000/docs**

---

## Frontend setup

### Prerequisites

- Node.js 18+ and npm

### Steps

1. **Go to the frontend directory**

   ```bash
   cd angular-app
   ```

2. **Install dependencies**

   ```bash
   npm install
   ```

3. **Configure environment**

   Edit `src/environments/environment.ts` (and `environment.prod.ts` for production):

   ```ts
   export const environment = {
     production: false,
     apiUrl: 'http://localhost:8000',        // Backend API base URL
     supabaseUrl: 'https://YOUR-PROJECT-REF.supabase.co',
     supabaseAnonKey: 'your-supabase-anon-key',
   };
   ```

4. **Run the dev server**

   ```bash
   npm start
   ```

   App: **http://localhost:4200**

5. **Build for production**

   ```bash
   npm run build
   ```

   Output is in `dist/angular-app/`. Serve it with your backend or any static host; set `apiUrl` and Supabase keys for the target environment.

---

## Supabase setup

### 1. Create a project

1. Go to [Supabase](https://supabase.com) and create a project.
2. Wait for the database to be ready.

### 2. Get credentials

In the Supabase dashboard:

- **Settings → API**
  - **Project URL** → `SUPABASE_URL` (backend and frontend)
  - **anon (public) key** → `supabaseAnonKey` (frontend only)
  - **service_role key** → `SUPABASE_SERVICE_ROLE_KEY` (backend only; keep secret)
- **Settings → API → JWT Secret** → `SUPABASE_JWT_SECRET` (backend; used to verify access tokens)

### 3. Create database tables

1. In Supabase, open **SQL Editor**.
2. Run the contents of **`mcp-orchestration/create_tables.sql`** (from this repo).

   This creates:

   - `tenants` – workspaces
   - `mcp_servers` – MCP server registrations per tenant
   - `mcp_tools` – cached tool definitions
   - `users` – user profile (email, name) by Supabase auth user id
   - `user_tenants` – links each auth user to one tenant
   - `audit_events` – audit/lifecycle events (tenant created, MCP server registered/deleted, tool refresh started/completed/failed)

   Plus indexes and RLS policies so the service role can access data.

   **Existing databases:** If you already ran an older `create_tables.sql`, run **`mcp-orchestration/migrations/add_audit_events.sql`** in the SQL Editor to add only the `audit_events` table.

### 4. Enable Email auth

- **Authentication → Providers → Email**: enable “Email” and configure (e.g. confirm email off for dev).
- Users sign up / log in with email and password; the frontend uses Supabase Auth and sends the JWT to the backend.

### 5. Environment summary

| Variable                     | Where        | Purpose |
|-----------------------------|-------------|---------|
| `SUPABASE_URL`              | Backend     | Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Backend     | Server-side DB and Auth admin |
| `SUPABASE_JWT_SECRET`       | Backend     | Verify Supabase access tokens |
| `supabaseUrl`               | Frontend    | Supabase project URL |
| `supabaseAnonKey`           | Frontend    | Supabase anon key for Auth |

---

## API reference

All endpoints except **Health** and **Onboard** require a valid Supabase access token:

```http
Authorization: Bearer <supabase_access_token>
```

The backend resolves the user’s tenant from `user_tenants` and scopes all tenant and MCP server operations to that tenant.

---

### Health (no auth)

| Method | Path     | Description        |
|--------|----------|--------------------|
| `GET`  | `/health` | Health check       |

**Response:** `{ "status": "ok" }`

---

### Auth & onboarding

| Method | Path             | Description |
|--------|------------------|-------------|
| `POST` | `/auth/onboard`   | Link the authenticated user to a new tenant (call after signup). |

**Headers:** `Authorization: Bearer <access_token>`

**Body (JSON):**

```json
{
  "tenant_name": "My workspace",
  "full_name": "Jane Doe"
}
```

- `tenant_name` (optional): name of the new tenant; default `"My workspace"`.
- `full_name` (optional): user’s display name.

**Response:** `TenantResponse` (e.g. `tenant_id`, `name`, `metadata`, `created_at`).

**Note:** If the user already has a tenant, returns `400` with message that they should use the existing tenant.

---

### Tenant (current user)

| Method | Path            | Description |
|--------|-----------------|-------------|
| `GET`  | `/tenants/me`   | Get current user’s tenant, user profile, and MCP servers. |

**Response:** Object with `tenant_id`, `name`, `metadata`, `created_at`, `user` (e.g. `email`, `full_name`), `mcp_servers` (list with `server_id`, `display_name`, `base_url`, `last_tools_refresh_at`, `tool_count`).

---

### Tenant by ID (own tenant only)

| Method | Path                  | Description |
|--------|-----------------------|-------------|
| `GET`  | `/tenants/{tenant_id}`| Get tenant and its MCP servers. Allowed only for the current user’s tenant. |

**Response:** Same shape as tenant + `mcp_servers` (no `user`).

---

### MCP server management

| Method | Path                                                      | Description |
|--------|-----------------------------------------------------------|-------------|
| `POST` | `/tenants/{tenant_id}/mcp-servers`                        | Register a new MCP server. |
| `GET`  | `/tenants/{tenant_id}/mcp-servers`                        | List MCP servers for the tenant. |
| `DELETE` | `/tenants/{tenant_id}/mcp-servers/{server_id}`          | Delete an MCP server (and its tools). |
| `POST` | `/tenants/{tenant_id}/mcp-servers/{server_id}/refresh-tools` | Refresh tool catalog for that server. |

**Register server – body (JSON):**

```json
{
  "base_url": "https://your-mcp-server.com/sse",
  "display_name": "My MCP Server",
  "handshake_path": "/mcp",
  "auth": {},
  "fallback_http_base": null,
  "extra_headers": {}
}
```

- `base_url` (required): MCP server URL (e.g. HTTP/SSE endpoint).
- `display_name` (required): Name shown in the UI.
- `handshake_path` (optional): Path for MCP handshake (e.g. `/mcp`).
- `auth` (optional): Auth config (e.g. for OAuth).
- `fallback_http_base` (optional): Fallback base URL if needed.
- `extra_headers` (optional): Extra HTTP headers.

**Response (register):** `MCPServerResponse`: `id`, `tenant_id`, `server_id`, `display_name`, `base_url`, `created_at`.

**List servers – response:** `{ "servers": [ ... ] }` with `server_id`, `display_name`, `base_url`, `status`, `last_tools_refresh_at`, `tool_count`.

**Refresh tools – response:** `{ "tenant_id", "server_id", "tool_count", "tool_names" }`.

---

### Chat

| Method | Path   | Description |
|--------|--------|-------------|
| `POST` | `/chat`| Send a message and get an assistant reply (and optional tool calls). Supports streaming via SSE. |

**Query:**

- `stream` (optional, default: `false`): if `true`, response is SSE stream of events.

**Headers:**

- `Authorization: Bearer <access_token>`
- `X-Correlation-Id` (optional): correlation ID for logging.

**Body (JSON):**

```json
{
  "session_id": "unique-session-id",
  "message": "List the top stations by number of negative reviews.",
  "tenant_id": null,
  "server_id": null
}
```

- `session_id` (required): Client-chosen session id (conversation history is keyed by this).
- `message` (required): User message.
- `tenant_id` (optional): Defaults to current user’s tenant.
- `server_id` (optional): MCP server to use; if omitted and tenant has one server, that server is used; if multiple, must be set.

**Response (non-streaming):**

```json
{
  "session_id": "...",
  "message": "Assistant reply text...",
  "tool_calls": [ { "tool_name", "args", "error", "server" } ],
  "raw_model_trace": null
}
```

`raw_model_trace` is present only when `ORCHESTRATOR_DEBUG=true`.

**Response (streaming):** SSE stream; each event has `event` and `data` (JSON). Events include `session.start`, `tool.call.*`, `assistant.*`, `session.end`, etc.

---

### Events (SSE)

| Method | Path     | Description |
|--------|----------|-------------|
| `GET`  | `/events`| Stream session events via SSE. |

**Query:** `session_id` (required).

**Headers:** `Authorization: Bearer <access_token>`

**Response:** SSE stream of events for that session (same event shape as chat streaming).

---

### WebSocket (alternative to SSE)

| Method  | Path  | Description |
|---------|-------|-------------|
| `GET`   | `/ws` | WebSocket endpoint for session events. |

**Query:** `session_id` (required). Auth can be via query or header depending on your client.

**Response:** JSON messages for each event until `session.end`.

---

## Running and testing APIs

### 1. Get an access token

- **Option A (UI):** Sign up or log in on the Angular app (http://localhost:4200), then open DevTools → Application (or Network) and copy the Supabase access token from the request headers or from Supabase client storage.
- **Option B (Supabase Auth API):** Use Supabase Auth REST API or `signInWithPassword` to get `session.access_token`.

### 2. Call APIs with curl

**Health:**

```bash
curl -s http://localhost:8000/health
```

**Tenant and servers (replace `TOKEN`):**

```bash
curl -s -H "Authorization: Bearer TOKEN" http://localhost:8000/tenants/me
```

**Register MCP server (replace `TOKEN` and `TENANT_ID`):**

```bash
curl -s -X POST -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"base_url":"https://your-mcp.example.com/sse","display_name":"My Server"}' \
  http://localhost:8000/tenants/TENANT_ID/mcp-servers
```

**Refresh tools (replace `TOKEN`, `TENANT_ID`, `SERVER_ID`):**

```bash
curl -s -X POST -H "Authorization: Bearer TOKEN" \
  "http://localhost:8000/tenants/TENANT_ID/mcp-servers/SERVER_ID/refresh-tools"
```

**Chat (non-streaming):**

```bash
curl -s -X POST -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"session_id":"test-1","message":"Hello, list your tools."}' \
  "http://localhost:8000/chat"
```

**Chat (streaming):**

```bash
curl -s -N -X POST -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json" \
  -d '{"session_id":"test-2","message":"Hello"}' \
  "http://localhost:8000/chat?stream=true"
```

### 3. OpenAPI (Swagger)

- **http://localhost:8000/docs** – Interactive docs; you can add `Authorization: Bearer <token>` and try endpoints from the browser.

---

## Architecture and optional configuration

- **Tenants:** Each user is linked to one tenant via `user_tenants`. All MCP servers and tools are stored per tenant.
- **MCP servers:** Stored in Supabase; tools are discovered via MCP and cached in `mcp_tools`. “Refresh tools” re-discovers and updates the cache.
- **Chat:** Request goes to OpenAI with conversation history and available tools; the orchestrator executes tool calls via MCP and returns the final assistant reply (and optionally streams events).
- **Optional env (backend):**
  - `MCP_SERVERS`: JSON array of server configs (name, base_url, etc.) for static servers.
  - `LOGS_DIR`, `ORCHESTRATOR_LOG_MAX_BYTES`, `ORCHESTRATOR_LOG_BACKUP_COUNT`: Logging.
  - `STREAM_RAW_SSE`: Whether to stream raw SSE from MCP (default true).

---

## Troubleshooting

### Backend

- **“TABLES NOT FOUND” / PGRST205:** Run `mcp-orchestration/create_tables.sql` in the Supabase SQL Editor and restart the backend.
- **503 “Authentication is not configured”:** Set `SUPABASE_JWT_SECRET` in the backend `.env` (copy from Supabase → Settings → API → JWT Secret).
- **502 “MCP server unreachable”:** Check MCP server URL, network, and that the server is running; ensure CORS/URL is correct for HTTP/SSE.

### Frontend

- **401 / 403 on API calls:** Ensure the user is logged in and the Supabase access token is sent (Angular interceptor should add `Authorization: Bearer <token>`). For 403 “No tenant linked”, complete onboarding (`POST /auth/onboard`).
- **CORS errors:** Backend allows all origins by default; for production, restrict `allow_origins` in `main.py`.

### Supabase

- **RLS / permission errors:** The backend uses the **service role** key, which bypasses RLS; ensure the policies in `create_tables.sql` are applied.
- **JWT invalid:** Ensure `SUPABASE_JWT_SECRET` in the backend matches Supabase Dashboard → Settings → API → JWT Secret (same as used to sign tokens).

### Chat / tools

- **“No MCP servers configured”:** Register at least one MCP server for the tenant via Connect App or `POST /tenants/{id}/mcp-servers`.
- **“Multiple MCP servers found… specify server_id”:** Send `server_id` in the chat request body.
- **Tools not updating:** Call `POST /tenants/{tenant_id}/mcp-servers/{server_id}/refresh-tools` after changing tools on the MCP server.

---

## Quick start checklist

1. **Supabase:** Create project → run `create_tables.sql` → enable Email auth → copy URL, anon key, service_role key, JWT secret.
2. **Backend:** `cd mcp-orchestration` → venv → `pip install -r requirements.txt` → `.env` with `OPENAI_API_KEY`, `SUPABASE_*` → `uvicorn main:app --reload`.
3. **Frontend:** `cd angular-app` → `npm install` → set `apiUrl`, `supabaseUrl`, `supabaseAnonKey` in `environment.ts` → `npm start`.
4. **Use app:** Open http://localhost:4200 → Sign up → Complete setup (onboard) → Connect App → Register MCP server → Test MCP → Send a message.

For full API details and request/response schemas, use **http://localhost:8000/docs** when the backend is running.
