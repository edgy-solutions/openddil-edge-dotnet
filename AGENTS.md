# AGENTS.md — OpenDDIL Edge SDK (.NET)

Guidelines and safety constraints for AI agents working in this repository.

## Repository Scope

This repo contains the **.NET Edge SDK** — the client-side code that runs on disconnected Edge devices. It manages the local SQLite Outbox for spooling events.

## What You CAN Do

- **Add new Operations classes** for new bounded contexts (e.g., `LogisticsOperations`).
- **Add new methods** to existing Operations classes for new event types.
- **Modify `OpenDdilOutboxManager`** to add utility methods (e.g., purge processed events).
- **Update documentation** (README, llms.txt, .cursorrules, this file).

## What You MUST NOT Do

- ❌ **Never make network calls**. This SDK is local-only. The Relay handles network I/O.
- ❌ **Never define Protobuf messages here**. All contracts live in `openddil-contracts`.
- ❌ **Never modify the Outbox schema** without making the same change in `openddil-edge-python`.
- ❌ **Never store raw JSON** in payload_bytes. Always use Protobuf binary serialization.
- ❌ **Never skip the CloudEvent envelope**. HQ processors expect every event to be wrapped.
- ❌ **Never delete or modify the `id` column semantics**. UUIDs are used for end-to-end idempotency.

## Adding a New Event Type

1. Ensure the Protobuf message exists in `openddil-contracts`.
2. Run `make csharp` in `openddil-contracts` to regenerate C# classes.
3. Create or update an Operations class (e.g., `InventoryOperations`).
4. The new method should:
   - Construct the domain event with all fields
   - Accept the current read-cache values for conflict detection
   - Call `_outbox.Enqueue()` with the correct `eventType` URI
   - Return the event ID
5. Update README and llms.txt.

## Symmetry Rule

> ⚠️ The .NET and Python Edge SDKs MUST remain symmetric. Any change to the Outbox schema, CloudEvent wrapping logic, or Operations API surface MUST be mirrored in `openddil-edge-python`.
