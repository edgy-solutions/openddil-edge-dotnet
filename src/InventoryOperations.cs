// =============================================================================
// OpenDDIL Edge SDK (.NET) — Inventory Operations
// =============================================================================
// High-level wrapper methods for UI developers. These methods construct the
// domain event, enqueue it in the Outbox, and return immediately — no network
// call required. The Relay handles flushing when connectivity allows.
// =============================================================================

using System;
using Google.Protobuf.WellKnownTypes;
using OpenDDIL.Edge;
using OpenDDIL.Inventory.V1;

namespace OpenDDIL.Edge.Inventory
{
    /// <summary>
    /// Provides high-level inventory operations for Edge UI developers.
    /// All operations are local-first: they write to the SQLite Outbox and
    /// return immediately, even when fully disconnected.
    /// </summary>
    public class InventoryOperations
    {
        private readonly OpenDdilOutboxManager _outbox;
        private readonly string _userId;

        public InventoryOperations(OpenDdilOutboxManager outbox, string userId)
        {
            _outbox = outbox;
            _userId = userId;
        }

        /// <summary>
        /// Allocates inventory at the Edge. Creates an ItemAllocatedEvent and
        /// spools it to the local Outbox for eventual sync to HQ.
        ///
        /// This method works fully offline. The event will be flushed to HQ
        /// via Redpanda when connectivity is restored.
        /// </summary>
        /// <param name="itemId">The inventory item ID to allocate from.</param>
        /// <param name="quantity">Number of units to allocate.</param>
        /// <param name="currentAvailableCount">
        /// The available_count currently shown in the local SQLite read-cache
        /// (synced via ElectricSQL). Used for conflict detection at HQ.
        /// </param>
        /// <returns>The event ID for tracking/correlation.</returns>
        public string AllocateInventory(string itemId, int quantity, int currentAvailableCount)
        {
            // Build the domain event
            var evt = new ItemAllocatedEvent
            {
                EventId = Guid.NewGuid().ToString(),
                ItemId = itemId,
                Quantity = quantity,
                OriginalCountAtAction = currentAvailableCount,
                Timestamp = Timestamp.FromDateTime(DateTime.UtcNow),
                UserId = _userId
            };

            // Enqueue into the local Outbox (wrapped in CloudEvent envelope)
            var eventId = _outbox.Enqueue(
                domainEvent: evt,
                eventType: "openddil.inventory.v1.ItemAllocatedEvent",
                userId: _userId,
                subject: itemId
            );

            Console.WriteLine(
                $"[OUTBOX] Allocated {quantity} units from item {itemId} " +
                $"(was {currentAvailableCount}). Event {eventId} queued.");

            return eventId;
        }
    }
}

// =============================================================================
// Usage Example
// =============================================================================
//
// var outbox = new OpenDdilOutboxManager("openddil_edge.db", "edge-node-42");
// var ops = new InventoryOperations(outbox, "operator-jane");
//
// // This works fully offline — writes to local SQLite only
// ops.AllocateInventory(
//     itemId: "550e8400-e29b-41d4-a716-446655440000",
//     quantity: 5,
//     currentAvailableCount: 100
// );
//
// // Check how many events are waiting to sync
// Console.WriteLine($"Pending sync: {outbox.GetPendingCount()} events");
// =============================================================================
