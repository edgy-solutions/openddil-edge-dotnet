// =============================================================================
// OpenDDIL Edge SDK (.NET) — Outbox Manager
// =============================================================================
// Manages the local SQLite Outbox for spooling events in DDIL environments.
// Events are serialized as Protobuf binaries and inserted into the Outbox.
// The Relay (separate component) flushes pending events to HQ when connected.
// =============================================================================

using System;
using System.Data;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Data.Sqlite;
using OpenDDIL.Events.V1;

namespace OpenDDIL.Edge
{
    /// <summary>
    /// Manages the local SQLite Outbox for spooling CloudEvent-wrapped domain
    /// events. Thread-safe for concurrent Edge UI operations.
    /// </summary>
    public class OpenDdilOutboxManager : IDisposable
    {
        private readonly SqliteConnection _connection;
        private readonly string _edgeNodeId;
        private long _sequence;

        /// <summary>
        /// Initializes the Outbox Manager, creating the Outbox table if needed.
        /// </summary>
        /// <param name="dbPath">Path to the local SQLite database file.</param>
        /// <param name="edgeNodeId">Unique identifier for this Edge node.</param>
        public OpenDdilOutboxManager(string dbPath, string edgeNodeId)
        {
            _edgeNodeId = edgeNodeId;
            _connection = new SqliteConnection($"Data Source={dbPath}");
            _connection.Open();

            InitializeSchema();
            _sequence = GetMaxSequence();
        }

        /// <summary>
        /// Enqueues a domain event into the local Outbox, wrapped in a CloudEvent.
        /// The event will be flushed to HQ by the Relay when connectivity allows.
        /// </summary>
        /// <typeparam name="T">A Protobuf IMessage type (e.g., ItemAllocatedEvent).</typeparam>
        /// <param name="domainEvent">The domain event to enqueue.</param>
        /// <param name="eventType">CloudEvent type URI (e.g., "openddil.inventory.v1.ItemAllocatedEvent").</param>
        /// <param name="userId">The operator who triggered this event.</param>
        /// <param name="subject">The entity ID this event is about (optional).</param>
        /// <returns>The event ID (UUID) assigned to this event.</returns>
        public string Enqueue<T>(T domainEvent, string eventType, string userId, string? subject = null)
            where T : IMessage<T>
        {
            var eventId = Guid.NewGuid().ToString();
            var now = Timestamp.FromDateTime(DateTime.UtcNow);
            var seq = System.Threading.Interlocked.Increment(ref _sequence);

            // Wrap the domain event in a CloudEvent envelope
            var cloudEvent = new CloudEvent
            {
                Id = eventId,
                Source = $"openddil://edge/{_edgeNodeId}",
                SpecVersion = "1.0",
                Type = eventType,
                Time = now,
                DataContentType = "application/protobuf",
                Subject = subject ?? "",
                UserId = userId,
                EdgeNodeId = _edgeNodeId,
                Sequence = (ulong)seq,
                Data = Any.Pack(domainEvent)
            };

            // Serialize the full CloudEvent envelope to Protobuf binary
            byte[] payloadBytes = cloudEvent.ToByteArray();

            // Insert into the local SQLite Outbox
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                INSERT INTO outbox_events (id, event_type, payload_bytes)
                VALUES (@id, @eventType, @payloadBytes)";
            cmd.Parameters.AddWithValue("@id", eventId);
            cmd.Parameters.AddWithValue("@eventType", eventType);
            cmd.Parameters.AddWithValue("@payloadBytes", payloadBytes);
            cmd.ExecuteNonQuery();

            return eventId;
        }

        /// <summary>
        /// Retrieves all pending (unflushed) events from the Outbox, ordered by creation time.
        /// Used by the Relay to flush events to HQ.
        /// </summary>
        /// <param name="batchSize">Maximum number of events to retrieve.</param>
        /// <returns>List of (id, payload_bytes) tuples.</returns>
        public List<(string Id, byte[] PayloadBytes)> GetPendingEvents(int batchSize = 100)
        {
            var events = new List<(string, byte[])>();

            using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                SELECT id, payload_bytes
                FROM outbox_events
                WHERE is_processed = 0
                ORDER BY created_at ASC
                LIMIT @batchSize";
            cmd.Parameters.AddWithValue("@batchSize", batchSize);

            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                events.Add((reader.GetString(0), (byte[])reader[1]));
            }

            return events;
        }

        /// <summary>
        /// Marks events as flushed after the Relay has successfully sent them to HQ.
        /// </summary>
        /// <param name="eventIds">The IDs of successfully flushed events.</param>
        public void MarkAsProcessed(IEnumerable<string> eventIds)
        {
            using var transaction = _connection.BeginTransaction();
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "UPDATE outbox_events SET is_processed = 1 WHERE id = @id";
            var param = cmd.Parameters.Add("@id", SqliteType.Text);

            foreach (var id in eventIds)
            {
                param.Value = id;
                cmd.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        /// <summary>
        /// Returns the count of pending (unflushed) events in the Outbox.
        /// Useful for UI status indicators.
        /// </summary>
        public long GetPendingCount()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM outbox_events WHERE is_processed = 0";
            return (long)cmd.ExecuteScalar()!;
        }

        private void InitializeSchema()
        {
            // Read and execute the embedded schema SQL
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS outbox_events (
                    id              TEXT    PRIMARY KEY,
                    event_type      TEXT    NOT NULL,
                    payload_bytes   BLOB    NOT NULL,
                    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                    is_processed    INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_pending
                    ON outbox_events (is_processed, created_at)
                    WHERE is_processed = 0;";
            cmd.ExecuteNonQuery();
        }

        private long GetMaxSequence()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM outbox_events";
            return (long)cmd.ExecuteScalar()!;
        }

        public void Dispose()
        {
            _connection?.Close();
            _connection?.Dispose();
        }
    }
}
