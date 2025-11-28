/*
 * VIBE-FI.COM - Event Sink for External Consumption
 * Emits structured NDJSON events for order lifecycle, fills, holdings, cash, and equity snapshots.
 */

using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;

namespace QuantConnect.Lean.Engine.Results
{
    /// <summary>
    /// Static event sink that writes NDJSON events to a file for external consumption.
    /// Thread-safe via locking around all file operations.
    /// </summary>
    public static class VibeEventSink
    {
        private static readonly object _lock = new object();
        private static StreamWriter _writer;
        private static string _filePath;
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
                    Log.Error($"VibeEventSink.Initialize(): Failed to initialize event sink: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Emit an event to the NDJSON file.
        /// </summary>
        /// <param name="eventType">Type of event (order, fill, holdings, cash, equity)</param>
        /// <param name="payload">The event payload object</param>
        public static void Emit(string eventType, object payload)
        {
            lock (_lock)
            {
                if (_writer == null)
                {
                    return; // Not initialized or failed to initialize
                }

                try
                {
                    var eventWrapper = new
                    {
                        type = eventType,
                        timestamp = DateTime.UtcNow,
                        data = payload
                    };

                    var json = JsonConvert.SerializeObject(eventWrapper, Formatting.None, _serializerSettings);
                    _writer.WriteLine(json);
                }
                catch (Exception ex)
                {
                    Log.Error($"VibeEventSink.Emit(): Failed to write event: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Emit an order event.
        /// </summary>
        public static void EmitOrderEvent(OrderEvent orderEvent, Order order)
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
                orderFee = orderEvent.OrderFee?.Value?.Amount ?? 0m,
                orderFeeCurrency = orderEvent.OrderFee?.Value?.Currency ?? "USD",
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
        private static void EmitFill(OrderEvent orderEvent, Order order)
        {
            var payload = new
            {
                orderId = order?.Id ?? orderEvent.OrderId,
                eventId = orderEvent.Id,
                symbol = orderEvent.Symbol?.Value,
                direction = orderEvent.Direction.ToString(),
                fillPrice = orderEvent.FillPrice,
                fillQuantity = orderEvent.FillQuantity,
                fee = orderEvent.OrderFee?.Value?.Amount ?? 0m,
                feeCurrency = orderEvent.OrderFee?.Value?.Currency ?? "USD",
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
                if (_writer != null)
                {
                    try
                    {
                        _writer.Flush();
                        _writer.Close();
                        _writer.Dispose();
                        Log.Trace("VibeEventSink.Close(): Event sink closed");
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"VibeEventSink.Close(): Error closing event sink: {ex.Message}");
                    }
                    finally
                    {
                        _writer = null;
                    }
                }
            }
        }
    }
}

