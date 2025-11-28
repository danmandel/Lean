/*
 * VIBE-FI.COM - Event Sink for External Consumption
 * Emits structured NDJSON events for order lifecycle, fills, holdings, cash, and equity snapshots.
 * Publishes to Redis pub/sub for real-time streaming with file fallback.
 */

using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QCOrder = QuantConnect.Orders.Order;
using QuantConnect.Securities;
using StackExchange.Redis;

namespace QuantConnect.Lean.Engine.Results
{
    /// <summary>
    /// Static event sink that writes NDJSON events to a file and publishes to Redis for external consumption.
    /// Thread-safe via locking around all file operations.
    /// </summary>
    public static class VibeEventSink
    {
        private static readonly object _lock = new object();
        private static StreamWriter _writer;
        private static string _filePath;
        private static ConnectionMultiplexer _redis;
        private static ISubscriber _publisher;
        private static string _strategyInstanceId;
        private static readonly JsonSerializerSettings _serializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            NullValueHandling = NullValueHandling.Ignore,
            DateFormatString = "yyyy-MM-ddTHH:mm:ss.fffZ"
        };

        /// <summary>
        /// Initialize the event sink with the results destination folder.
        /// </summary>
        public static void Initialize(string resultsDestinationFolder)
        {
            lock (_lock)
            {
                if (_writer != null)
                {
                    return; // Already initialized
                }

                // Get strategy instance ID from config (used as Redis channel suffix)
                _strategyInstanceId = Config.Get("job-project-id", "");

                // Initialize file writer (fallback)
                try
                {
                    _filePath = Path.Combine(resultsDestinationFolder, "vibe-events.ndjson");
                    
                    // Ensure directory exists
                    var directory = Path.GetDirectoryName(_filePath);
                    if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                    {
                        Directory.CreateDirectory(directory);
                    }

                    _writer = new StreamWriter(_filePath, append: true) { AutoFlush = true };
                    Log.Trace($"VibeEventSink.Initialize(): Writing events to {_filePath}");
                }
                catch (Exception ex)
                {
                    Log.Error($"VibeEventSink.Initialize(): Failed to initialize file sink: {ex.Message}");
                }

                // Initialize Redis publisher
                var redisUrl = Config.Get("vibe-redis-url", "");
                if (!string.IsNullOrEmpty(redisUrl))
                {
                    try
                    {
                        var options = ConfigurationOptions.Parse(redisUrl);
                        options.AbortOnConnectFail = false;
                        options.ConnectTimeout = 5000;
                        options.SyncTimeout = 1000;
                        
                        _redis = ConnectionMultiplexer.Connect(options);
                        _publisher = _redis.GetSubscriber();
                        Log.Trace($"VibeEventSink.Initialize(): Connected to Redis at {redisUrl}");
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"VibeEventSink.Initialize(): Failed to connect to Redis: {ex.Message}");
                        // Continue without Redis - file fallback will still work
                    }
                }
                else
                {
                    Log.Trace("VibeEventSink.Initialize(): No Redis URL configured, using file-only mode");
                }
            }
        }

        /// <summary>
        /// Emit an event to both the NDJSON file and Redis pub/sub.
        /// </summary>
        /// <param name="eventType">Type of event (order, fill, holdings, cash, equity)</param>
        /// <param name="payload">The event payload object</param>
        public static void Emit(string eventType, object payload)
        {
            lock (_lock)
            {
                var eventWrapper = new
                {
                    type = eventType,
                    timestamp = DateTime.UtcNow,
                    data = payload
                };

                string json;
                try
                {
                    json = JsonConvert.SerializeObject(eventWrapper, Formatting.None, _serializerSettings);
                }
                catch (Exception ex)
                {
                    Log.Error($"VibeEventSink.Emit(): Failed to serialize event: {ex.Message}");
                    return;
                }

                // Write to file (fallback)
                if (_writer != null)
                {
                    try
                    {
                        _writer.WriteLine(json);
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"VibeEventSink.Emit(): Failed to write event to file: {ex.Message}");
                    }
                }

                // Publish to Redis (primary real-time channel)
                if (_publisher != null && !string.IsNullOrEmpty(_strategyInstanceId))
                {
                    try
                    {
                        var channel = $"vibe:events:{_strategyInstanceId}";
                        _publisher.Publish(RedisChannel.Literal(channel), json, CommandFlags.FireAndForget);
                    }
                    catch (Exception ex)
                    {
                        // Don't block on Redis failures - file fallback is available
                        Log.Debug($"VibeEventSink.Emit(): Redis publish failed (file fallback active): {ex.Message}");
                    }
                }
            }
        }

        /// <summary>
        /// Emit an order event.
        /// </summary>
        public static void EmitOrderEvent(OrderEvent orderEvent, QCOrder order)
        {
            var payload = new
            {
                orderId = order?.Id ?? orderEvent.OrderId,
                eventId = orderEvent.Id,
                symbol = orderEvent.Symbol?.Value,
                status = orderEvent.Status.ToString(),
                direction = orderEvent.Direction.ToString(),
                quantity = orderEvent.Quantity,
                fillPrice = orderEvent.FillPrice,
                fillQuantity = orderEvent.FillQuantity,
                orderFee = orderEvent.OrderFee?.Value.Amount ?? 0m,
                orderFeeCurrency = orderEvent.OrderFee?.Value.Currency ?? "USD",
                message = orderEvent.Message,
                utcTime = orderEvent.UtcTime,
                // Include order details if available
                orderType = order?.Type.ToString(),
                orderPrice = order?.Price,
                orderQuantity = order?.Quantity,
                brokerIds = order?.BrokerId
            };

            Emit("order", payload);

            // If this is a fill event, also emit a dedicated fill event
            if (orderEvent.FillQuantity != 0)
            {
                EmitFill(orderEvent, order);
            }
        }

        /// <summary>
        /// Emit a fill event for executed trades.
        /// </summary>
        private static void EmitFill(OrderEvent orderEvent, QCOrder order)
        {
            var payload = new
            {
                orderId = order?.Id ?? orderEvent.OrderId,
                eventId = orderEvent.Id,
                symbol = orderEvent.Symbol?.Value,
                direction = orderEvent.Direction.ToString(),
                fillPrice = orderEvent.FillPrice,
                fillQuantity = orderEvent.FillQuantity,
                fee = orderEvent.OrderFee?.Value.Amount ?? 0m,
                feeCurrency = orderEvent.OrderFee?.Value.Currency ?? "USD",
                utcTime = orderEvent.UtcTime,
                brokerIds = order?.BrokerId
            };

            Emit("fill", payload);
        }

        /// <summary>
        /// Emit a holdings snapshot.
        /// </summary>
        public static void EmitHoldings(Dictionary<string, Holding> holdings, DateTime utcTime)
        {
            var holdingsList = new List<object>();
            foreach (var kvp in holdings)
            {
                var holding = kvp.Value;
                holdingsList.Add(new
                {
                    symbol = holding.Symbol?.Value ?? kvp.Key,
                    quantity = holding.Quantity,
                    averagePrice = holding.AveragePrice,
                    marketPrice = holding.MarketPrice,
                    marketValue = holding.MarketValue,
                    unrealizedPnL = holding.UnrealizedPnL,
                    unrealizedPnLPercent = holding.UnrealizedPnLPercent
                });
            }

            var payload = new
            {
                utcTime = utcTime,
                holdings = holdingsList
            };

            Emit("holdings", payload);
        }

        /// <summary>
        /// Emit a cash balance snapshot.
        /// </summary>
        public static void EmitCash(CashBook cashBook, DateTime utcTime)
        {
            var cashList = new List<object>();
            foreach (var kvp in cashBook)
            {
                var cash = kvp.Value;
                cashList.Add(new
                {
                    currency = kvp.Key,
                    amount = cash.Amount,
                    conversionRate = cash.ConversionRate,
                    valueInAccountCurrency = cash.ValueInAccountCurrency
                });
            }

            var payload = new
            {
                utcTime = utcTime,
                accountCurrency = cashBook.AccountCurrency,
                totalValueInAccountCurrency = cashBook.TotalValueInAccountCurrency,
                cash = cashList
            };

            Emit("cash", payload);
        }

        /// <summary>
        /// Emit an equity snapshot.
        /// </summary>
        public static void EmitEquity(
            decimal totalPortfolioValue,
            decimal totalHoldingsValue,
            decimal totalUnrealizedProfit,
            decimal totalFees,
            decimal netProfit,
            DateTime utcTime)
        {
            var payload = new
            {
                utcTime = utcTime,
                totalPortfolioValue = totalPortfolioValue,
                totalHoldingsValue = totalHoldingsValue,
                totalUnrealizedProfit = totalUnrealizedProfit,
                totalFees = totalFees,
                netProfit = netProfit
            };

            Emit("equity", payload);
        }

        /// <summary>
        /// Close the event sink and flush any pending writes.
        /// </summary>
        public static void Close()
        {
            lock (_lock)
            {
                // Close file writer
                if (_writer != null)
                {
                    try
                    {
                        _writer.Flush();
                        _writer.Close();
                        _writer.Dispose();
                        Log.Trace("VibeEventSink.Close(): File sink closed");
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"VibeEventSink.Close(): Error closing file sink: {ex.Message}");
                    }
                    finally
                    {
                        _writer = null;
                    }
                }

                // Close Redis connection
                if (_redis != null)
                {
                    try
                    {
                        _redis.Close();
                        _redis.Dispose();
                        Log.Trace("VibeEventSink.Close(): Redis connection closed");
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"VibeEventSink.Close(): Error closing Redis connection: {ex.Message}");
                    }
                    finally
                    {
                        _redis = null;
                        _publisher = null;
                    }
                }
            }
        }
    }
}
