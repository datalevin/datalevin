# Datalevin MCP Server

`dtlv mcp` runs a Datalevin MCP server over `stdio`.

The MCP server is a local process adapter over Datalevin APIs:

- local MCP clients talk to `dtlv mcp` over `stdio`
- the MCP server opens local Datalevin databases directly
- the MCP server can also open remote `dtlv://...` targets behind the same
  local `stdio` process

The intended integration target is generic:

- any MCP-compliant client that can launch a local `stdio` server
- custom AI-powered applications that want a stable Datalevin tool surface

It is not designed around any specific branded host or coding-agent workflow.

## Starting the Server

Read-only mode is the default:

```console
dtlv mcp
```

Enable write tools explicitly when needed:

```console
dtlv --allow-writes mcp
```

## Tool Result Shape

`tools/call` replies follow the normal MCP JSON-RPC envelope and return both a
machine-readable payload and a text payload:

```json
{
  "jsonrpc": "2.0",
  "id": 7,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"database\":\"database-...\",\"result\":[[\":name\",\"Alice\"]]}"
      }
    ],
    "structuredContent": {
      "database": "database-...",
      "result": [[":name", "Alice"]]
    }
  }
}
```

Use `structuredContent` as the authoritative payload. `content[0].text` is a
compact text rendering for MCP clients that expect a text block.

When a tool fails, MCP returns `result.isError = true` and the
`structuredContent` value contains the machine-readable error payload.

## Large Responses

The MCP server applies its own response shaping on top of Datalevin's JSON API
limits.

Current defaults:

- max result items: `200`
- max serialized response bytes: `524288`

When a result is truncated, `structuredContent` includes:

```json
{
  "meta": {
    "truncated": true,
    "truncations": [
      {
        "kind": "items",
        "path": "result",
        "limit": 200,
        "returned": 200,
        "original": 684
      }
    ]
  }
}
```

### Truncation Records

`meta.truncations` is an array. Each element describes one truncation step.

Item truncation:

```json
{
  "kind": "items",
  "path": "result",
  "limit": 200,
  "returned": 200,
  "original": 684
}
```

- `kind = "items"` means the top-level `result` collection was shortened
- `path = "result"` identifies the truncated field
- `limit` is the applied MCP item cap
- `returned` is the number of items still present
- `original` is the original top-level item count before truncation

Byte truncation with preview:

```json
{
  "kind": "bytes",
  "path": "result",
  "limit": 524288,
  "mode": "preview"
}
```

Byte truncation with omission:

```json
{
  "kind": "bytes",
  "path": "result",
  "limit": 524288,
  "mode": "omitted"
}
```

- `kind = "bytes"` means the serialized response would have exceeded the MCP
  byte budget
- `mode = "preview"` means `structuredContent.result` still contains a shortened
  preview
- `mode = "omitted"` means `structuredContent.result` was set to `null` because
  even the preview form would still be too large

When truncation happens, `content[0].text` is no longer a full duplicate of the
structured payload. It becomes a small summary object instead:

```json
{
  "summary": "Result truncated.",
  "truncations": [
    {
      "kind": "items",
      "path": "result",
      "limit": 200,
      "returned": 200,
      "original": 684
    }
  ]
}
```

This avoids paying the full payload cost twice.

## Pagination Guidance

For naturally paged tools, pass explicit limits instead of relying on MCP
truncation:

- `datalevin_datoms`
- `datalevin_kv_range`
- `datalevin_search_datoms`

For Datalog queries, include query-level limiting when possible. MCP still
enforces its own caps even if the query itself does not.

## Remote Access

The MCP server itself remains a local `stdio` process. Remote Datalevin access
uses Datalevin's existing client/server support behind that process.

Examples:

- local Datalog DB: `{"dir":"/data/app-db"}`
- remote Datalog DB: `{"uri":"dtlv://user:pass@dbhost/app-db"}`
- local KV store: `{"dir":"/data/app-kv"}`
- remote KV store: `{"uri":"dtlv://user:pass@dbhost/app-kv"}`

## Current Notes

- write tools are disabled unless `--allow-writes` is passed at startup
- MCP ids such as `database`, `kv`, `searchIndex`, and `vectorIndex` are
  session-scoped
- the MCP transport is `stdio` only in v1
