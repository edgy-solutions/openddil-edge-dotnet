# OpenDDIL Edge SDK (.NET)

.NET client SDK for OpenDDIL — manages the local SQLite **Outbox** for spooling events in DDIL (Denied, Degraded, Intermittent, Limited) environments.

## Architecture

```mermaid
graph LR
    UI["Edge UI / App"] --> OPS["InventoryOperations"]
    OPS --> OM["OpenDdilOutboxManager"]
    OM --> DB["SQLite Outbox"]
    DB --> RELAY["Relay (flush)"]
    RELAY --> RP["Redpanda (HQ)"]

    ESQL["ElectricSQL"] --> RC["SQLite Read-Cache"]
    RC --> UI
```

## How It Works

1. **UI calls `InventoryOperations.AllocateInventory()`** — constructs a Protobuf event
2. **OutboxManager wraps it** in a CloudEvent envelope and serializes to binary
3. **Inserts into local SQLite** `outbox_events` table — works fully offline
4. **Relay (separate component)** queries pending events and flushes to Redpanda when connected
5. **ElectricSQL** syncs the updated Postgres state back to the local read-cache

## Key Files

| File | Purpose |
|---|---|
| `src/OpenDdilOutboxManager.cs` | Core Outbox manager — enqueue, get pending, mark processed |
| `src/InventoryOperations.cs` | High-level `AllocateInventory()` wrapper for UI developers |
| `src/OutboxRelayService.cs` | `BackgroundService` that flushes Outbox → Redpanda with exponential backoff |
| `sql/outbox_schema.sql` | SQLite schema for the `outbox_events` table |

## Quick Start

```csharp
var outbox = new OpenDdilOutboxManager("openddil_edge.db", "edge-node-42");
var ops = new InventoryOperations(outbox, "operator-jane");

// Works fully offline — writes to local SQLite only
ops.AllocateInventory(
    itemId: "550e8400-e29b-41d4-a716-446655440000",
    quantity: 5,
    currentAvailableCount: 100
);

// Check sync status
Console.WriteLine($"Pending sync: {outbox.GetPendingCount()} events");
```

## Dependencies

- `Microsoft.Data.Sqlite` — SQLite access
- `Google.Protobuf` — Protobuf serialization
- `Confluent.Kafka` — Kafka/Redpanda producer (used by Relay)
- `Microsoft.Extensions.Hosting` — BackgroundService base (used by Relay)
- Generated classes from `openddil-contracts` (`make csharp`)

## AI Documentation

| File | Purpose |
|---|---|
| [`llms.txt`](llms.txt) | Structured project summary for LLM discovery |
| [`.cursorrules`](.cursorrules) | C# coding style and SDK conventions |
| [`AGENTS.md`](AGENTS.md) | AI agent safety guidelines |
