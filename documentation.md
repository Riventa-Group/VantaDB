# VantaDB Documentation

**Version 1.0.0** | Built by Neroph

VantaDB is a high-performance document database engine written in Rust. It stores JSON documents in collections within databases, offers a MongoDB-compatible query language, and exposes both an interactive CLI shell and a gRPC wire protocol for programmatic access from any language.

---

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Running the Server](#running-the-server)
- [Authentication & Users](#authentication--users)
- [Databases & Collections](#databases--collections)
- [Document CRUD](#document-crud)
- [Update Operations](#update-operations)
- [Query & Filter Engine](#query--filter-engine)
- [Aggregation Pipeline](#aggregation-pipeline)
- [Indexes](#indexes)
- [Schema Validation](#schema-validation)
- [Transactions](#transactions)
- [Pagination & Sorting](#pagination--sorting)
- [gRPC API Reference](#grpc-api-reference)
- [Self-Check & Diagnostics](#self-check--diagnostics)
- [Data Directory & Storage](#data-directory--storage)
- [Updating VantaDB](#updating-vantadb)
- [Production Deployment](#production-deployment)
- [Integration Guide](#integration-guide)

---

## Installation

### Prerequisites

- **Rust toolchain** (1.70+): Install via [rustup](https://rustup.rs/)
- **Protobuf compiler** (`protoc`): Required for gRPC code generation

```bash
# Ubuntu / Debian
sudo apt update && sudo apt install -y protobuf-compiler

# macOS
brew install protobuf
```

### Build from Source

```bash
git clone https://github.com/nerophlabs/VantaDB.git
cd VantaDB

# Development build
cargo build

# Optimized release build (recommended for production)
cargo build --release
```

The release binary is at `target/release/vantadb`. Copy it to a location on your `$PATH`:

```bash
sudo cp target/release/vantadb /usr/local/bin/
```

### Verify Installation

```bash
vantadb --version
```

---

## Quick Start

```bash
# 1. Start the gRPC server (default port 5432)
vantadb --serve

# 2. In another terminal, open the interactive shell
sudo vantadb --login        # root login requires sudo

# 3. Create a database and collection
create db myapp
use myapp
create collection users

# 4. Insert a document
insert users {"name": "Alice", "age": 30, "email": "alice@example.com"}

# 5. Query it back
find users
```

---

## Running the Server

### CLI Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--serve` | Start the gRPC server | - |
| `--port <PORT>` | Set the gRPC listen port | `5432` |
| `--login` | Open the interactive shell (authenticate first) | - |
| `--status` | Print server status (databases, users, data dir) | - |
| `--benchmark` | Run the built-in benchmark suite | - |
| `--self-check` | Run diagnostic checks against a running server | - |
| `--user <NAME>` | Username for self-check authentication | - |
| `--password <PW>` | Password for self-check authentication | - |

### Start the Server

```bash
# Default port (5432)
vantadb --serve

# Custom port
vantadb --serve --port 9090
```

The server listens on `0.0.0.0:<port>` and serves both `VantaAuth` and `VantaDb` gRPC services.

### Running as a systemd Service

```ini
# /etc/systemd/system/vantadb.service
[Unit]
Description=VantaDB Database Server
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/vantadb --serve --port 5432
Restart=on-failure
RestartSec=5
User=vantadb
Group=vantadb
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now vantadb
```

---

## Authentication & Users

VantaDB has a built-in authentication system with role-based access control. All gRPC calls (except `Authenticate`) require a valid session token.

### Roles

| Role | CLI Alias | Permissions |
|------|-----------|-------------|
| `root` | - | Full system access. Auto-created on first run. Requires `sudo` for CLI login. |
| `admin` | `admin` | Create/drop databases, collections, indexes, schemas. Manage users. Read/write data. |
| `readwrite` | `rw` | Read and write data. Cannot manage databases or users. |
| `readonly` | `ro` | Read-only access to data. |

### Root Login

The `root` user is created automatically with no password. CLI login requires elevated privileges:

```bash
sudo vantadb --login
# Enter "root" as username — no password prompt
```

### Managing Users (CLI)

```bash
# List all users (admin/root only)
users

# Create a user
adduser alice admin       # prompts for password
adduser bob rw            # readwrite role
adduser viewer ro         # readonly role

# Change a password
passwd alice              # change another user's password (admin+)
passwd                    # change your own password

# Delete a user (root only)
rmuser alice

# Show current user
whoami
```

### Authentication via gRPC

```
rpc Authenticate(AuthRequest) returns (AuthResponse)
```

Send username and password, receive a session token. Include this token in all subsequent requests as a `Bearer` token in the `authorization` metadata key:

```
authorization: Bearer <token>
```

### gRPC User Management

| RPC | Requires | Description |
|-----|----------|-------------|
| `Authenticate` | None | Returns session token |
| `CreateUser` | Admin | Create a new user with role and optional database restrictions |
| `DeleteUser` | Root | Remove a user |
| `SetPassword` | Admin | Change a user's password |
| `ListUsers` | Admin | List all usernames |
| `GetUser` | Admin | Get user details (role, created_at, databases) |

---

## Databases & Collections

VantaDB organizes data into **databases**, each containing one or more **collections**. Collections hold JSON documents.

### CLI Commands

```bash
# Create a database
create db myapp

# Switch to a database (required before collection/document operations)
use myapp

# List all databases
show dbs

# Create a collection
create collection users

# List collections in the current database
show collections

# Drop a collection (admin+)
drop collection users

# Drop a database (admin+)
drop db myapp
```

### gRPC RPCs

| RPC | Request | Description |
|-----|---------|-------------|
| `CreateDatabase` | `DatabaseRequest { database }` | Create a new database |
| `DropDatabase` | `DatabaseRequest { database }` | Delete a database and all its data |
| `ListDatabases` | `Empty` | List all database names |
| `CreateCollection` | `CollectionRequest { database, collection }` | Create a collection |
| `DropCollection` | `CollectionRequest { database, collection }` | Delete a collection |
| `ListCollections` | `DatabaseRequest { database }` | List collections in a database |

---

## Document CRUD

Documents are JSON objects. VantaDB automatically assigns a UUID `_id` field if one is not provided.

### Insert

```bash
# CLI
insert users {"name": "Alice", "age": 30, "email": "alice@example.com"}
```

Returns the `_id` of the inserted document. You may supply your own `_id`:

```bash
insert users {"_id": "alice-001", "name": "Alice", "age": 30}
```

### Find

```bash
# All documents in a collection
find users

# Find by ID
find users 550e8400-e29b-41d4-a716-446655440000

# Find by field value
find users where city = "NYC"
find users where age = 30
```

### Delete

```bash
# Delete by document ID
delete users 550e8400-e29b-41d4-a716-446655440000
```

### Count

```bash
count users
```

### gRPC RPCs

| RPC | Request | Response |
|-----|---------|----------|
| `Insert` | `InsertRequest { database, collection, document_json }` | `InsertResponse { success, id, error }` |
| `FindById` | `FindByIdRequest { database, collection, id }` | `DocumentResponse { found, document_json }` |
| `FindAll` | `FindAllRequest { database, collection, pagination?, sort? }` | `DocumentsResponse { documents_json[], total_count }` |
| `FindWhere` | `FindWhereRequest { database, collection, field, value_json, pagination?, sort? }` | `DocumentsResponse` |
| `DeleteById` | `DeleteByIdRequest { database, collection, id }` | `DeleteResponse { success, found }` |
| `Count` | `CountRequest { database, collection }` | `CountResponse { count }` |

---

## Update Operations

VantaDB supports updating documents by ID or by filter, with a rich set of patch operators.

### Update by ID

```bash
# Simple field merge (implicit $set)
update users 550e8400-... {"age": 31, "verified": true}

# Using explicit operators
update users 550e8400-... {"$set": {"age": 31}, "$unset": ["temp_field"]}
```

### Update by Filter

```bash
update users where {"city": {"$eq": "NYC"}} set {"verified": true}
update users where {"age": {"$lt": 18}} set {"$set": {"status": "minor"}}
```

### Patch Operators

| Operator | Syntax | Description |
|----------|--------|-------------|
| *(plain object)* | `{"field": "value"}` | Shorthand for `$set` -- merges fields into the document |
| `$set` | `{"$set": {"field": "value"}}` | Set one or more fields |
| `$unset` | `{"$unset": ["field1", "field2"]}` | Remove fields from the document |
| `$inc` | `{"$inc": {"counter": 1}}` | Increment a numeric field (supports negative values) |
| `$push` | `{"$push": {"tags": "new-tag"}}` | Append a value to an array field |
| `$pull` | `{"$pull": {"tags": "old-tag"}}` | Remove a value from an array field |

The `_id` field is protected and cannot be modified by any update operator.

### gRPC RPCs

| RPC | Request | Response |
|-----|---------|----------|
| `Update` | `UpdateRequest { database, collection, id, patch_json }` | `UpdateResponse { success, modified_count }` |
| `UpdateWhere` | `UpdateWhereRequest { database, collection, filter_json, patch_json }` | `UpdateResponse { success, modified_count }` |

---

## Query & Filter Engine

The `query` command and `Query` RPC accept MongoDB-compatible filter expressions for powerful document retrieval.

### CLI Usage

```bash
# Basic filter
query users {"age": {"$gt": 25}}

# Combine with sort and pagination
query users {"city": "NYC"} --sort age --desc --page 1 --size 10
```

### Filter Operators

#### Comparison

| Operator | Example | Description |
|----------|---------|-------------|
| `$eq` | `{"age": {"$eq": 30}}` | Equal to |
| `$ne` | `{"status": {"$ne": "inactive"}}` | Not equal to |
| `$gt` | `{"age": {"$gt": 25}}` | Greater than |
| `$gte` | `{"age": {"$gte": 25}}` | Greater than or equal |
| `$lt` | `{"age": {"$lt": 50}}` | Less than |
| `$lte` | `{"age": {"$lte": 50}}` | Less than or equal |

Implicit equality is also supported: `{"age": 30}` is equivalent to `{"age": {"$eq": 30}}`.

#### Set Membership

| Operator | Example | Description |
|----------|---------|-------------|
| `$in` | `{"status": {"$in": ["active", "pending"]}}` | Value is in the set |
| `$nin` | `{"role": {"$nin": ["banned", "suspended"]}}` | Value is NOT in the set |

#### Logical

| Operator | Example | Description |
|----------|---------|-------------|
| `$and` | `{"$and": [{"age": {"$gt": 18}}, {"status": "active"}]}` | All conditions must match |
| `$or` | `{"$or": [{"city": "NYC"}, {"city": "LA"}]}` | At least one condition must match |
| `$not` | `{"$not": {"status": "banned"}}` | Negates the condition |

#### Existence & String

| Operator | Example | Description |
|----------|---------|-------------|
| `$exists` | `{"email": {"$exists": true}}` | Field exists (or not when `false`) |
| `$regex` | `{"name": {"$regex": "^A"}}` | Regex pattern match |
| `$contains` | `{"bio": {"$contains": "engineer"}}` | Substring match |
| `$starts_with` | `{"name": {"$starts_with": "Al"}}` | Prefix match |
| `$ends_with` | `{"email": {"$ends_with": ".com"}}` | Suffix match |

#### Nested Fields (Dot Notation)

Access nested document fields using dot-separated paths:

```json
{"address.city": "NYC"}
{"profile.settings.theme": {"$eq": "dark"}}
```

### gRPC RPC

```
rpc Query(QueryRequest) returns (DocumentsResponse)
```

`QueryRequest` fields: `database`, `collection`, `filter_json`, `pagination?`, `sort?`.

---

## Aggregation Pipeline

VantaDB supports a multi-stage aggregation pipeline similar to MongoDB's. Pass a JSON array of pipeline stages.

### CLI Usage

```bash
# Group by city and count
aggregate users [{"$group": {"_id": "$city", "count": {"$sum": 1}}}]

# Alias: agg
agg users [{"$match": {"age": {"$gte": 21}}}, {"$group": {"_id": "$city", "avg_age": {"$avg": "$age"}}}]
```

### Pipeline Stages

#### `$match` -- Filter Documents

```json
{"$match": {"age": {"$gt": 25}}}
```

Uses the same filter syntax as the [query engine](#query--filter-engine).

#### `$group` -- Group & Aggregate

```json
{"$group": {
  "_id": "$city",
  "total": {"$sum": 1},
  "avg_age": {"$avg": "$age"},
  "youngest": {"$min": "$age"},
  "oldest": {"$max": "$age"},
  "count": {"$count": {}},
  "names": {"$push": "$name"},
  "first_name": {"$first": "$name"},
  "last_name": {"$last": "$name"}
}}
```

The `_id` field specifies the grouping key. Use `"$fieldName"` syntax to reference document fields.

**Supported accumulators:**

| Accumulator | Description |
|-------------|-------------|
| `$sum` | Sum of values, or count when passed a constant (e.g., `{"$sum": 1}`) |
| `$avg` | Average of numeric values |
| `$min` | Minimum value |
| `$max` | Maximum value |
| `$count` | Count of documents in the group |
| `$push` | Collect all values into an array |
| `$first` | First value in the group |
| `$last` | Last value in the group |

Compound group keys are supported:

```json
{"$group": {"_id": {"city": "$city", "state": "$state"}, "count": {"$sum": 1}}}
```

#### `$sort` -- Sort Results

```json
{"$sort": {"age": 1}}
{"$sort": {"age": -1, "name": 1}}
```

Use `1` for ascending and `-1` for descending. Multiple fields are supported.

#### `$limit` -- Limit Results

```json
{"$limit": 10}
```

#### `$skip` -- Skip Results

```json
{"$skip": 20}
```

#### `$project` -- Include/Exclude Fields

```json
{"$project": {"name": 1, "age": 1}}
{"$project": {"password": 0, "internal_notes": 0}}
```

Use `1` (or `true`) to include, `0` (or `false`) to exclude. The `_id` field is always included when using inclusion mode.

#### `$count` -- Count Documents

```json
{"$count": "total_users"}
```

Returns a single document: `{"total_users": 42}`.

#### `$unwind` -- Expand Arrays

```json
{"$unwind": "$tags"}
{"$unwind": {"path": "$tags"}}
```

Produces one document per array element.

#### `$lookup` -- Cross-Collection Join

```json
{"$lookup": {
  "from": "orders",
  "localField": "_id",
  "foreignField": "user_id",
  "as": "user_orders"
}}
```

Performs a left join with another collection in the same database.

### gRPC RPC

```
rpc Aggregate(AggregateRequest) returns (AggregateResponse)
```

`AggregateRequest` fields: `database`, `collection`, `pipeline_json` (JSON array string).

---

## Indexes

Indexes improve query performance on frequently accessed fields. VantaDB uses in-memory B-tree indexes.

### CLI Commands

```bash
# Create an index on a field
create index users email

# Create a unique index (enforces uniqueness on the field)
create index users email unique

# List indexes on a collection
show indexes users

# Drop an index
drop index users email
```

### gRPC RPCs

| RPC | Request | Description |
|-----|---------|-------------|
| `CreateIndex` | `IndexRequest { database, collection, field, unique }` | Create an index |
| `DropIndex` | `IndexRequest { database, collection, field }` | Remove an index |
| `ListIndexes` | `ListIndexesRequest { database, collection }` | List all indexes on a collection |

Indexes are automatically maintained on insert, update, and delete operations. Index definitions are persisted and survive server restarts.

---

## Schema Validation

Enforce structure on collections with schema definitions. Schemas are validated on every insert and update.

### Setting a Schema

```bash
schema set users {
  "strict": true,
  "fields": [
    {"name": "name", "type": "string", "required": true, "min_length": 1, "max_length": 100},
    {"name": "age", "type": "number", "required": true, "min": 0, "max": 150},
    {"name": "email", "type": "string", "required": true, "pattern": "^[^@]+@[^@]+\\.[^@]+$"},
    {"name": "role", "type": "string", "enum": ["user", "admin", "moderator"]},
    {"name": "tags", "type": "array"},
    {"name": "settings", "type": "object"},
    {"name": "active", "type": "bool"}
  ]
}
```

### Schema Options

**Collection-level:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `strict` | bool | `false` | When `true`, rejects documents with fields not defined in the schema |
| `fields` | array | required | Array of field definitions |

**Field-level:**

| Option | Type | Applies To | Description |
|--------|------|------------|-------------|
| `name` | string | all | Field name |
| `type` | string | all | One of: `string`, `number`, `bool`, `object`, `array`, `any` |
| `required` | bool | all | If `true`, the field must be present |
| `min` | number | number | Minimum numeric value |
| `max` | number | number | Maximum numeric value |
| `min_length` | number | string | Minimum string length |
| `max_length` | number | string | Maximum string length |
| `pattern` | string | string | Regex pattern the string must match |
| `enum` | array | all | Allowed values (whitelist) |

### CLI Commands

```bash
# Set a schema
schema set users {"fields": [{"name": "name", "type": "string", "required": true}]}

# View the current schema
schema get users

# Remove a schema (admin+)
schema drop users
```

### gRPC RPCs

| RPC | Request | Description |
|-----|---------|-------------|
| `SetSchema` | `SetSchemaRequest { database, collection, schema_json }` | Set/replace schema |
| `GetSchema` | `GetSchemaRequest { database, collection }` | Retrieve schema |
| `DropSchema` | `GetSchemaRequest { database, collection }` | Remove schema enforcement |

---

## Transactions

VantaDB supports multi-operation transactions. Operations are buffered and applied atomically on commit, or discarded entirely on rollback.

### CLI Usage

```bash
# Start a transaction
begin

# Queue operations (not applied yet)
tx insert orders {"product": "Widget", "qty": 5, "total": 49.95}
tx update inventory item-001 {"$inc": {"stock": -5}}
tx delete cart cart-item-123

# Apply all operations
commit

# Or discard everything
rollback
```

Only one transaction can be active at a time in a CLI session. Operations are buffered locally until `commit` is called.

### Transaction Operations

| Command | Description |
|---------|-------------|
| `begin` | Start a new transaction |
| `tx insert <collection> <json>` | Queue an insert |
| `tx update <collection> <id> <json>` | Queue an update |
| `tx delete <collection> <id>` | Queue a delete |
| `commit` | Apply all queued operations |
| `rollback` | Discard all queued operations |

### gRPC RPCs

| RPC | Request | Response |
|-----|---------|----------|
| `BeginTx` | `Empty` | `TxResponse { tx_id }` |
| `TxInsert` | `TxInsertRequest { tx_id, database, collection, document_json }` | `StatusResponse` |
| `TxUpdate` | `TxUpdateRequest { tx_id, database, collection, id, patch_json }` | `StatusResponse` |
| `TxDelete` | `TxDeleteRequest { tx_id, database, collection, id }` | `StatusResponse` |
| `CommitTx` | `TxRequest { tx_id }` | `StatusResponse` |
| `RollbackTx` | `TxRequest { tx_id }` | `StatusResponse` |

When using gRPC, the `tx_id` returned by `BeginTx` must be passed to all subsequent transaction RPCs. Multiple concurrent transactions from different clients are supported.

---

## Pagination & Sorting

Pagination and sorting are available on `find`, `query`, `FindAll`, `FindWhere`, and `Query` operations.

### CLI Usage

```bash
# Sort by age descending
find users --sort age --desc

# Paginate: page 2 with 10 items per page
find users --page 2 --size 10

# Combine
find users where city = "NYC" --sort name --page 1 --size 20

# Works with query too
query users {"age": {"$gt": 18}} --sort age --page 1 --size 50
```

### gRPC Messages

```protobuf
message PaginationOptions {
  uint32 page      = 1;  // 1-indexed page number (0 = no pagination)
  uint32 page_size = 2;  // items per page (default 50)
}

message SortOptions {
  string field      = 1;  // field name to sort by
  bool   descending = 2;  // true = descending, false = ascending
}
```

The `DocumentsResponse` includes `total_count` so you can calculate total pages:

```
total_pages = ceil(total_count / page_size)
```

---

## gRPC API Reference

VantaDB exposes two gRPC services. The full protobuf definition is in `proto/vantadb.proto`.

### Connecting

```
Host: <server-ip>
Port: 5432 (default)
Protocol: gRPC (HTTP/2)
```

### Authentication Flow

1. Call `VantaAuth.Authenticate` with username and password
2. Receive a session `token` in the response
3. Attach the token to all subsequent requests as metadata:
   ```
   authorization: Bearer <token>
   ```

### Service: VantaAuth

| RPC | Auth Required | Min Role |
|-----|---------------|----------|
| `Authenticate` | No | - |
| `CreateUser` | Yes | Admin |
| `DeleteUser` | Yes | Root |
| `SetPassword` | Yes | Admin |
| `ListUsers` | Yes | Admin |
| `GetUser` | Yes | Admin |

### Service: VantaDb

| RPC | Min Role | Description |
|-----|----------|-------------|
| `CreateDatabase` | Admin | Create database |
| `DropDatabase` | Admin | Delete database |
| `ListDatabases` | ReadOnly | List databases |
| `CreateCollection` | ReadWrite | Create collection |
| `DropCollection` | Admin | Delete collection |
| `ListCollections` | ReadOnly | List collections |
| `Insert` | ReadWrite | Insert document |
| `FindById` | ReadOnly | Get document by ID |
| `FindAll` | ReadOnly | Get all documents |
| `FindWhere` | ReadOnly | Filter by field value |
| `DeleteById` | ReadWrite | Delete document |
| `Count` | ReadOnly | Count documents |
| `Update` | ReadWrite | Update document by ID |
| `UpdateWhere` | ReadWrite | Update documents by filter |
| `Query` | ReadOnly | Rich filter query |
| `Aggregate` | ReadOnly | Aggregation pipeline |
| `CreateIndex` | ReadWrite | Create index |
| `DropIndex` | Admin | Drop index |
| `ListIndexes` | ReadOnly | List indexes |
| `SetSchema` | Admin | Set collection schema |
| `GetSchema` | ReadOnly | Get collection schema |
| `DropSchema` | Admin | Remove schema |
| `BeginTx` | ReadOnly | Start transaction |
| `CommitTx` | ReadWrite | Commit transaction |
| `RollbackTx` | ReadOnly | Rollback transaction |
| `TxInsert` | ReadWrite | Buffer insert in transaction |
| `TxUpdate` | ReadWrite | Buffer update in transaction |
| `TxDelete` | ReadWrite | Buffer delete in transaction |

### Client Code Generation

Generate client code from the proto file for your language:

```bash
# Go
protoc --go_out=. --go-grpc_out=. proto/vantadb.proto

# Python
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/vantadb.proto

# Node.js / TypeScript
npx grpc_tools_node_protoc --js_out=import_style=commonjs:. --grpc_out=. proto/vantadb.proto

# Java
protoc --java_out=. --grpc-java_out=. proto/vantadb.proto
```

---

## Self-Check & Diagnostics

VantaDB includes a built-in self-check command that connects to a running server and validates every feature via gRPC.

### Usage

```bash
# Start the server first
vantadb --serve &

# Run self-check (requires valid credentials)
vantadb --self-check --user root --password "" --port 5432
```

The self-check tests:
- Authentication service (login, list users, get user)
- Database operations (create, list, drop)
- Collection operations (create, list, drop)
- Document CRUD (insert, find, count, delete)
- Sorting and pagination
- Update operations (by ID, by filter)
- Rich query/filter engine (all operators)
- Aggregation pipeline (group, match, sum, avg)
- Index management (create, list, drop, unique indexes)
- Schema validation (set, get, drop)
- Transactions (begin, insert/update/delete, commit, rollback)

Each check reports PASS/FAIL with timing. Use this to verify a deployment is working correctly.

---

## Data Directory & Storage

### Default Location

```
~/.vantadb/
  system/     # Auth data (users, sessions)
  data/       # All databases
    _meta/    # Database metadata
    mydb/     # One directory per database
      users/  # One directory per collection
```

### Storage Engine

VantaDB uses a custom storage engine with:

- **Write-Ahead Log (WAL)**: All writes are journaled before being applied
- **256-shard DashMap**: Concurrent in-memory hash map for fast lookups
- **Memory-mapped I/O**: Efficient disk access via `memmap2`
- **mimalloc**: High-performance memory allocator
- **Argon2**: Password hashing for authentication

### Backup

Back up the entire `~/.vantadb/` directory. The database is consistent when the server is stopped.

```bash
# Stop server, backup, restart
systemctl stop vantadb
tar czf vantadb-backup-$(date +%Y%m%d).tar.gz ~/.vantadb/
systemctl start vantadb
```

---

## Updating VantaDB

### From Source

```bash
cd VantaDB
git pull origin main
cargo build --release

# Stop the running server
sudo systemctl stop vantadb   # or kill the process

# Replace the binary
sudo cp target/release/vantadb /usr/local/bin/

# Restart
sudo systemctl start vantadb
```

### Pre-Update Checklist

1. **Back up your data**: `tar czf vantadb-backup.tar.gz ~/.vantadb/`
2. **Check the changelog** for breaking changes
3. **Stop the server** before replacing the binary
4. **Run self-check** after restarting to verify everything works:
   ```bash
   vantadb --self-check --user root --password "" --port 5432
   ```

---

## Production Deployment

### Recommended Configuration

- **Build with release profile**: `cargo build --release` (enables LTO, strip, and full optimization)
- **Run as a dedicated user**: Create a `vantadb` system user
- **Set file descriptor limits**: VantaDB benefits from high `ulimit -n` (65535+)
- **Use a reverse proxy** (e.g., Envoy, nginx with gRPC support) for TLS termination
- **Monitor disk usage**: VantaDB stores data in `~/.vantadb/` by default

### Security Recommendations

- **Never expose the gRPC port directly to the public internet** without TLS
- **Use strong passwords** for all non-root users
- **Create application-specific users** with the minimum required role (`readonly` or `readwrite`)
- **Root access requires sudo** at the CLI level -- keep your system secure
- **Restrict network access** to the gRPC port using firewall rules

### Performance Tuning

The release build (`cargo build --release`) applies:
- `opt-level = 3` (maximum optimization)
- `lto = true` (link-time optimization)
- `codegen-units = 1` (single codegen unit for better optimization)
- `strip = true` (smaller binary)
- `panic = "abort"` (no unwinding overhead)

The mimalloc allocator is used globally for reduced allocation latency.

---

## Integration Guide

### Connecting from Your Application

VantaDB uses gRPC (Protocol Buffers over HTTP/2). Generate client code from `proto/vantadb.proto` for your language, then follow this pattern:

#### Python Example

```python
import grpc
import vantadb_pb2
import vantadb_pb2_grpc

# Connect
channel = grpc.insecure_channel("localhost:5432")
auth_stub = vantadb_pb2_grpc.VantaAuthStub(channel)
db_stub = vantadb_pb2_grpc.VantaDbStub(channel)

# Authenticate
auth_resp = auth_stub.Authenticate(
    vantadb_pb2.AuthRequest(username="myapp", password="secret")
)
token = auth_resp.token
metadata = [("authorization", f"Bearer {token}")]

# Create a database
db_stub.CreateDatabase(
    vantadb_pb2.DatabaseRequest(database="myapp"),
    metadata=metadata
)

# Create a collection
db_stub.CreateCollection(
    vantadb_pb2.CollectionRequest(database="myapp", collection="users"),
    metadata=metadata
)

# Insert a document
import json
resp = db_stub.Insert(
    vantadb_pb2.InsertRequest(
        database="myapp",
        collection="users",
        document_json=json.dumps({"name": "Alice", "age": 30})
    ),
    metadata=metadata
)
print(f"Inserted: {resp.id}")

# Query with filters
resp = db_stub.Query(
    vantadb_pb2.QueryRequest(
        database="myapp",
        collection="users",
        filter_json=json.dumps({"age": {"$gte": 21}}),
        pagination=vantadb_pb2.PaginationOptions(page=1, page_size=20),
        sort=vantadb_pb2.SortOptions(field="age", descending=True)
    ),
    metadata=metadata
)
for doc_json in resp.documents_json:
    print(json.loads(doc_json))

# Aggregation
resp = db_stub.Aggregate(
    vantadb_pb2.AggregateRequest(
        database="myapp",
        collection="users",
        pipeline_json=json.dumps([
            {"$group": {"_id": "$city", "count": {"$sum": 1}}}
        ])
    ),
    metadata=metadata
)
```

#### Go Example

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
    pb "your-module/vantadb"
)

func main() {
    conn, _ := grpc.Dial("localhost:5432", grpc.WithInsecure())
    defer conn.Close()

    authClient := pb.NewVantaAuthClient(conn)
    dbClient := pb.NewVantaDbClient(conn)

    // Authenticate
    authResp, _ := authClient.Authenticate(context.Background(), &pb.AuthRequest{
        Username: "myapp",
        Password: "secret",
    })
    token := authResp.Token

    // Create authenticated context
    ctx := metadata.AppendToOutgoingContext(
        context.Background(),
        "authorization", "Bearer "+token,
    )

    // Insert
    doc, _ := json.Marshal(map[string]any{"name": "Alice", "age": 30})
    resp, _ := dbClient.Insert(ctx, &pb.InsertRequest{
        Database:     "myapp",
        Collection:   "users",
        DocumentJson: string(doc),
    })
    fmt.Println("Inserted:", resp.Id)

    // Query
    filter, _ := json.Marshal(map[string]any{"age": map[string]any{"$gte": 21}})
    qResp, _ := dbClient.Query(ctx, &pb.QueryRequest{
        Database:   "myapp",
        Collection: "users",
        FilterJson: string(filter),
        Pagination: &pb.PaginationOptions{Page: 1, PageSize: 20},
        Sort:       &pb.SortOptions{Field: "age", Descending: true},
    })
    for _, d := range qResp.DocumentsJson {
        fmt.Println(d)
    }
}
```

#### Node.js / TypeScript Example

```typescript
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

const packageDef = protoLoader.loadSync("proto/vantadb.proto");
const proto = grpc.loadPackageDefinition(packageDef) as any;

const channel = new grpc.credentials.createInsecure();
const authClient = new proto.vantadb.VantaAuth("localhost:5432", channel);
const dbClient = new proto.vantadb.VantaDb("localhost:5432", channel);

// Authenticate
authClient.Authenticate({ username: "myapp", password: "secret" }, (err, resp) => {
  const metadata = new grpc.Metadata();
  metadata.add("authorization", `Bearer ${resp.token}`);

  // Insert
  dbClient.Insert({
    database: "myapp",
    collection: "users",
    document_json: JSON.stringify({ name: "Alice", age: 30 })
  }, metadata, (err, resp) => {
    console.log("Inserted:", resp.id);
  });

  // Query
  dbClient.Query({
    database: "myapp",
    collection: "users",
    filter_json: JSON.stringify({ age: { $gte: 21 } }),
    pagination: { page: 1, page_size: 20 },
    sort: { field: "age", descending: true }
  }, metadata, (err, resp) => {
    resp.documents_json.forEach(doc => console.log(JSON.parse(doc)));
  });
});
```

### Integration Best Practices

1. **Reuse connections**: Create one gRPC channel and share it across your application
2. **Cache the auth token**: Authenticate once at startup, re-authenticate on `UNAUTHENTICATED` errors
3. **Use connection pooling** for high-throughput scenarios
4. **Set deadlines/timeouts** on gRPC calls to prevent hanging requests
5. **Handle pagination** for large result sets -- never rely on unbounded `FindAll`
6. **Use schemas** in production to catch data integrity issues early
7. **Create indexes** on fields you frequently filter or sort by
8. **Use transactions** for multi-document operations that must be atomic

---

## CLI Command Reference

| Command | Description |
|---------|-------------|
| `help` | Show all commands |
| `exit` / `quit` / `q` | Exit the shell |
| `clear` / `cls` | Clear the screen |
| `whoami` | Show current user and role |
| `status` | Show server status |
| `benchmark` / `bench` | Run the benchmark suite |
| `use <db>` | Switch to a database |
| `show dbs` | List all databases |
| `show collections` | List collections in current db |
| `show indexes <col>` | List indexes on a collection |
| `create db <name>` | Create a database |
| `create collection <name>` | Create a collection |
| `create index <col> <field> [unique]` | Create an index |
| `drop db <name>` | Drop a database |
| `drop collection <name>` | Drop a collection |
| `drop index <col> <field>` | Drop an index |
| `insert <col> <json>` | Insert a document |
| `find <col>` | List all documents |
| `find <col> <id>` | Find document by ID |
| `find <col> where <field> = <value>` | Find by field value |
| `delete <col> <id>` | Delete a document |
| `count <col>` | Count documents |
| `update <col> <id> <json>` | Update document by ID |
| `update <col> where <filter> set <patch>` | Update by filter |
| `query <col> <filter_json>` | Rich query with filter |
| `aggregate <col> <pipeline_json>` | Run aggregation pipeline |
| `agg <col> <pipeline_json>` | Alias for aggregate |
| `schema set <col> <json>` | Set collection schema |
| `schema get <col>` | View collection schema |
| `schema drop <col>` | Remove schema |
| `begin` | Start a transaction |
| `tx insert <col> <json>` | Insert in transaction |
| `tx update <col> <id> <json>` | Update in transaction |
| `tx delete <col> <id>` | Delete in transaction |
| `commit` | Commit transaction |
| `rollback` | Rollback transaction |
| `users` | List all users |
| `adduser <name> <role>` | Create a user |
| `rmuser <name>` | Delete a user |
| `passwd [name]` | Change password |

All find/query commands accept optional flags: `--sort <field>`, `--desc`, `--page N`, `--size N`.

---

*Built with care by [Riventa Group](https://riventa.group)*
