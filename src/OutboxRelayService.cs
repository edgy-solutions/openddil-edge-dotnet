// =============================================================================
// OpenDDIL Edge SDK (.NET) — Relay Agent
// =============================================================================
// Background service that flushes the local SQLite Outbox to Redpanda (HQ)
// when connectivity is available. Designed for DDIL environments:
//   - Swallows network exceptions and retries gracefully
//   - Only marks events as processed after Redpanda ACK
//   - Uses exponential backoff on connection failures
//   - Sends raw Protobuf bytes for maximum bandwidth efficiency
// =============================================================================

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenDDIL.Edge;

namespace OpenDDIL.Edge.Relay
{
    /// <summary>
    /// Background service that polls the local SQLite Outbox and publishes
    /// pending events to Redpanda. Resilient to network failures — keeps
    /// polling and retrying automatically when connectivity is restored.
    /// </summary>
    public class OutboxRelayService : BackgroundService
    {
        private readonly OpenDdilOutboxManager _outbox;
        private readonly ILogger<OutboxRelayService> _logger;
        private readonly string _bootstrapServers;
        private readonly string _topic;
        private readonly int _batchSize;

        // Backoff settings for DDIL resilience
        private static readonly TimeSpan MinPollInterval = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan MaxBackoff = TimeSpan.FromMinutes(5);
        private static readonly TimeSpan IdlePollInterval = TimeSpan.FromSeconds(10);

        public OutboxRelayService(
            OpenDdilOutboxManager outbox,
            ILogger<OutboxRelayService> logger,
            string bootstrapServers = "localhost:9092",
            string topic = "inventory-commands",
            int batchSize = 50)
        {
            _outbox = outbox;
            _logger = logger;
            _bootstrapServers = bootstrapServers;
            _topic = topic;
            _batchSize = batchSize;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "[RELAY] Starting Outbox Relay. Bootstrap={Bootstrap}, Topic={Topic}",
                _bootstrapServers, _topic);

            var config = new ProducerConfig
            {
                BootstrapServers = _bootstrapServers,
                // Exactly-once semantics: enable idempotent producer
                EnableIdempotence = true,
                Acks = Acks.All,
                // Compression for bandwidth-limited DDIL links
                CompressionType = CompressionType.Zstd,
                // Retry settings for intermittent connectivity
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 500,
                // Batch settings for efficiency
                LingerMs = 100,
                BatchSize = 65536,
            };

            var currentBackoff = MinPollInterval;

            while (!stoppingToken.IsCancellationRequested)
            {
                IProducer<string, byte[]>? producer = null;

                try
                {
                    producer = new ProducerBuilder<string, byte[]>(config).Build();
                    _logger.LogInformation("[RELAY] Connected to Redpanda.");
                    currentBackoff = MinPollInterval; // Reset backoff on successful connection

                    // Inner loop: poll and flush while connected
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        var events = _outbox.GetPendingEvents(_batchSize);

                        if (events.Count == 0)
                        {
                            // Nothing to flush — idle poll
                            await Task.Delay(IdlePollInterval, stoppingToken);
                            continue;
                        }

                        _logger.LogInformation("[RELAY] Flushing {Count} events.", events.Count);
                        var flushedIds = new List<string>();

                        foreach (var (id, payloadBytes) in events)
                        {
                            try
                            {
                                // Publish raw Protobuf bytes — no re-serialization overhead
                                var result = await producer.ProduceAsync(
                                    _topic,
                                    new Message<string, byte[]>
                                    {
                                        Key = id,  // Event ID as partition key
                                        Value = payloadBytes
                                    },
                                    stoppingToken);

                                // Only mark processed after successful ACK
                                flushedIds.Add(id);

                                _logger.LogDebug(
                                    "[RELAY] Event {EventId} → partition {Partition} offset {Offset}",
                                    id, result.Partition.Value, result.Offset.Value);
                            }
                            catch (ProduceException<string, byte[]> ex)
                            {
                                // Individual message failure — skip and retry next cycle
                                _logger.LogWarning(
                                    ex, "[RELAY] Failed to produce event {EventId}. Will retry.", id);
                                break; // Stop this batch, retry on next poll
                            }
                        }

                        // Mark successfully flushed events
                        if (flushedIds.Count > 0)
                        {
                            _outbox.MarkAsProcessed(flushedIds);
                            _logger.LogInformation(
                                "[RELAY] Marked {Count} events as processed.", flushedIds.Count);
                        }

                        // Brief pause between batches
                        await Task.Delay(MinPollInterval, stoppingToken);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    // Graceful shutdown
                    _logger.LogInformation("[RELAY] Shutting down.");
                    break;
                }
                catch (Exception ex)
                {
                    // Connection failure — backoff and retry
                    _logger.LogWarning(
                        ex, "[RELAY] Connection lost. Retrying in {Backoff}...", currentBackoff);

                    await Task.Delay(currentBackoff, stoppingToken);

                    // Exponential backoff with cap
                    currentBackoff = TimeSpan.FromTicks(
                        Math.Min(currentBackoff.Ticks * 2, MaxBackoff.Ticks));
                }
                finally
                {
                    producer?.Dispose();
                }
            }

            _logger.LogInformation(
                "[RELAY] Stopped. {Pending} events still pending.",
                _outbox.GetPendingCount());
        }
    }
}
