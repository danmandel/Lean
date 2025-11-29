/*
 * VIBE-FI.COM - Ably Data Queue Handler
 * Receives real-time market data from Ably pub/sub channels.
 * Subscribes to market:{symbol} channels for OHLCV bar data.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Threading;
using System.Threading.Tasks;
using IO.Ably;
using IO.Ably.Realtime;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Securities;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Util;

namespace QuantConnect.Lean.Engine.DataFeeds
{
    /// <summary>
    /// Data queue handler that receives market data from Ably pub/sub channels.
    /// Used for live trading with centralized market data distribution.
    /// </summary>
    [Export(typeof(IDataQueueHandler))]
    public class AblyDataQueueHandler : IDataQueueHandler
    {
        private AblyRealtime _ably;
        private readonly IDataAggregator _aggregator;
        private readonly EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;
        private readonly MarketHoursDatabase _marketHoursDatabase;
        private readonly Dictionary<Symbol, TimeZoneOffsetProvider> _symbolExchangeTimeZones;
        private readonly Dictionary<string, IRealtimeChannel> _channels;
        private readonly object _lock = new object();
        private bool _isConnected;

        /// <summary>
        /// Returns whether the data provider is connected
        /// </summary>
        public bool IsConnected => _isConnected;

        /// <summary>
        /// Initializes a new instance of the AblyDataQueueHandler
        /// </summary>
        public AblyDataQueueHandler()
        {
            _aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
                Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"));
            
            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
            _symbolExchangeTimeZones = new Dictionary<Symbol, TimeZoneOffsetProvider>();
            _channels = new Dictionary<string, IRealtimeChannel>();
            
            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            _subscriptionManager.SubscribeImpl += (symbols, tickType) => SubscribeSymbols(symbols);
            _subscriptionManager.UnsubscribeImpl += (symbols, tickType) => UnsubscribeSymbols(symbols);

            Initialize();
        }

        private void Initialize()
        {
            var apiKey = Config.Get("ably-api-key", "");
            if (string.IsNullOrEmpty(apiKey))
            {
                Log.Error("AblyDataQueueHandler.Initialize(): ably-api-key not configured");
                return;
            }

            try
            {
                var options = new ClientOptions(apiKey)
                {
                    AutoConnect = true,
                    EchoMessages = false,
                    ClientId = Config.Get("job-project-id", "lean-" + Guid.NewGuid().ToString("N").Substring(0, 8))
                };

                _ably = new AblyRealtime(options);

                _ably.Connection.On(ConnectionEvent.Connected, args =>
                {
                    _isConnected = true;
                    Log.Trace("AblyDataQueueHandler.Initialize(): Connected to Ably");
                });

                _ably.Connection.On(ConnectionEvent.Disconnected, args =>
                {
                    _isConnected = false;
                    Log.Trace("AblyDataQueueHandler.Initialize(): Disconnected from Ably");
                });

                _ably.Connection.On(ConnectionEvent.Failed, args =>
                {
                    _isConnected = false;
                    Log.Error($"AblyDataQueueHandler.Initialize(): Connection failed: {args.Reason}");
                });

                Log.Trace("AblyDataQueueHandler.Initialize(): Ably client initialized");
            }
            catch (Exception ex)
            {
                Log.Error($"AblyDataQueueHandler.Initialize(): Failed to initialize Ably: {ex.Message}");
            }
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            var enumerator = _aggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);
            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            _aggregator.Remove(dataConfig);
        }

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        public void SetJob(LiveNodePacket job)
        {
            // Job configuration if needed
        }

        private bool SubscribeSymbols(IEnumerable<Symbol> symbols)
        {
            if (_ably == null) return false;

            foreach (var symbol in symbols)
            {
                var channelName = $"market:{symbol.Value}";

                lock (_lock)
                {
                    if (_channels.ContainsKey(channelName))
                        continue;

                    var channel = _ably.Channels.Get(channelName);
                    
                    channel.Subscribe("bar", message =>
                    {
                        try
                        {
                            ProcessBarMessage(symbol, message);
                        }
                        catch (Exception ex)
                        {
                            Log.Error($"AblyDataQueueHandler.SubscribeSymbols(): Error processing message for {symbol}: {ex.Message}");
                        }
                    });

                    _channels[channelName] = channel;
                    Log.Trace($"AblyDataQueueHandler.SubscribeSymbols(): Subscribed to {channelName}");
                }
            }

            return true;
        }

        private bool UnsubscribeSymbols(IEnumerable<Symbol> symbols)
        {
            foreach (var symbol in symbols)
            {
                var channelName = $"market:{symbol.Value}";

                lock (_lock)
                {
                    if (_channels.TryGetValue(channelName, out var channel))
                    {
                        channel.Unsubscribe();
                        _channels.Remove(channelName);
                        Log.Trace($"AblyDataQueueHandler.UnsubscribeSymbols(): Unsubscribed from {channelName}");
                    }
                }
            }

            return true;
        }

        private void ProcessBarMessage(Symbol symbol, Message message)
        {
            var json = message.Data?.ToString();
            if (string.IsNullOrEmpty(json)) return;

            var barData = JsonConvert.DeserializeObject<AblyBarData>(json);
            if (barData == null) return;

            var offsetProvider = GetTimeZoneOffsetProvider(symbol);
            var utcTime = DateTime.Parse(barData.Timestamp).ToUniversalTime();
            var exchangeTime = offsetProvider.ConvertFromUtc(utcTime);

            var tradeBar = new TradeBar
            {
                Symbol = symbol,
                Time = exchangeTime,
                Open = barData.Open,
                High = barData.High,
                Low = barData.Low,
                Close = barData.Close,
                Volume = barData.Volume,
                Period = TimeSpan.FromMinutes(1)
            };

            _aggregator.Update(tradeBar);
        }

        private TimeZoneOffsetProvider GetTimeZoneOffsetProvider(Symbol symbol)
        {
            if (!_symbolExchangeTimeZones.TryGetValue(symbol, out var offsetProvider))
            {
                var exchangeTimeZone = _marketHoursDatabase.GetExchangeHours(
                    symbol.ID.Market, symbol, symbol.SecurityType).TimeZone;
                _symbolExchangeTimeZones[symbol] = offsetProvider = 
                    new TimeZoneOffsetProvider(exchangeTimeZone, DateTime.UtcNow, Time.EndOfTime);
            }
            return offsetProvider;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing resources
        /// </summary>
        public void Dispose()
        {
            lock (_lock)
            {
                foreach (var channel in _channels.Values)
                {
                    channel.Unsubscribe();
                }
                _channels.Clear();
            }

            _ably?.Close();
            _ably = null;
            _isConnected = false;

            Log.Trace("AblyDataQueueHandler.Dispose(): Closed Ably connection");
        }

        /// <summary>
        /// Bar data structure from Ably messages
        /// </summary>
        private class AblyBarData
        {
            [JsonProperty("symbol")]
            public string Symbol { get; set; }

            [JsonProperty("timestamp")]
            public string Timestamp { get; set; }

            [JsonProperty("open")]
            public decimal Open { get; set; }

            [JsonProperty("high")]
            public decimal High { get; set; }

            [JsonProperty("low")]
            public decimal Low { get; set; }

            [JsonProperty("close")]
            public decimal Close { get; set; }

            [JsonProperty("volume")]
            public decimal Volume { get; set; }
        }
    }
}
