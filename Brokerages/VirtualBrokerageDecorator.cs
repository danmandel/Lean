/*
 * VIBE-FI.COM - Virtual Brokerage Isolation
 * Wraps any IBrokerage to provide capital isolation per strategy instance.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;

namespace QuantConnect.Brokerages
{
    /// <summary>
    /// Wraps any IBrokerage to provide virtual capital isolation.
    /// Maintains its own cash and position tracking, independent of the real account.
    /// Orders are still executed on the real brokerage but validated against virtual capital.
    /// </summary>
    [BrokerageFactory(typeof(VirtualBrokerageFactory))]
    public class VirtualBrokerageDecorator : IBrokerage
    {
        private readonly IBrokerage _innerBrokerage;
        private readonly string _strategyInstanceId;
        private readonly string _accountCurrency;
        private readonly decimal _allocatedCapital;

        // Virtual state - isolated from real account
        private decimal _virtualCash;
        private readonly Dictionary<Symbol, Holding> _virtualPositions = new();
        private readonly HashSet<int> _trackedOrderIds = new();

        /// <summary>
        /// Creates a new VirtualBrokerageDecorator wrapping the specified brokerage.
        /// </summary>
        /// <param name="innerBrokerage">The real brokerage to wrap</param>
        /// <param name="allocatedCapital">The initial capital allocation for this strategy</param>
        /// <param name="currentCash">The current cash (may differ from allocated if restored)</param>
        /// <param name="strategyInstanceId">The strategy instance ID for logging</param>
        /// <param name="restoredPositions">Optional positions to restore on startup</param>
        /// <param name="accountCurrency">The account currency (default USD)</param>
        public VirtualBrokerageDecorator(
            IBrokerage innerBrokerage,
            decimal allocatedCapital,
            decimal currentCash,
            string strategyInstanceId,
            List<Holding> restoredPositions = null,
            string accountCurrency = Currencies.USD)
        {
            _innerBrokerage = innerBrokerage ?? throw new ArgumentNullException(nameof(innerBrokerage));
            _allocatedCapital = allocatedCapital;
            _virtualCash = currentCash;
            _strategyInstanceId = strategyInstanceId;
            _accountCurrency = accountCurrency;

            // Restore positions if provided
            if (restoredPositions != null)
            {
                foreach (var holding in restoredPositions)
                {
                    if (holding.Symbol != null && holding.Quantity != 0)
                    {
                        _virtualPositions[holding.Symbol] = holding;
                    }
                }
                Log.Trace($"VirtualBrokerageDecorator: Restored {_virtualPositions.Count} positions");
            }

            // Wire up events - intercept order events to update virtual state
            _innerBrokerage.OrdersStatusChanged += OnInnerOrdersStatusChanged;
            _innerBrokerage.OrderIdChanged += (s, e) => OrderIdChanged?.Invoke(this, e);
            _innerBrokerage.OrderUpdated += (s, e) => OrderUpdated?.Invoke(this, e);
            _innerBrokerage.OptionPositionAssigned += (s, e) => OptionPositionAssigned?.Invoke(this, e);
            _innerBrokerage.OptionNotification += (s, e) => OptionNotification?.Invoke(this, e);
            _innerBrokerage.NewBrokerageOrderNotification += (s, e) => NewBrokerageOrderNotification?.Invoke(this, e);
            _innerBrokerage.DelistingNotification += (s, e) => DelistingNotification?.Invoke(this, e);
            _innerBrokerage.AccountChanged += (s, e) => AccountChanged?.Invoke(this, e);
            _innerBrokerage.Message += (s, e) => Message?.Invoke(this, e);

            Log.Trace($"VirtualBrokerageDecorator: Initialized for strategy {strategyInstanceId} " +
                      $"with allocated=${allocatedCapital}, cash=${currentCash}");
        }

        /// <summary>
        /// Factory method to create a VirtualBrokerageDecorator from LEAN config.
        /// </summary>
        public static VirtualBrokerageDecorator FromConfig(IBrokerage innerBrokerage, string accountCurrency = Currencies.USD)
        {
            var allocatedCapital = Config.GetValue("virtual-allocated-capital", 100000m);
            var currentCash = Config.GetValue("virtual-current-cash", allocatedCapital);
            var strategyInstanceId = Config.Get("virtual-strategy-instance-id", "unknown");
            var positionsJson = Config.Get("virtual-positions-json", "");

            List<Holding> restoredPositions = null;
            if (!string.IsNullOrEmpty(positionsJson))
            {
                try
                {
                    restoredPositions = JsonConvert.DeserializeObject<List<Holding>>(positionsJson);
                    Log.Trace($"VirtualBrokerageDecorator.FromConfig: Parsed {restoredPositions?.Count ?? 0} positions from config");
                }
                catch (Exception ex)
                {
                    Log.Error($"VirtualBrokerageDecorator.FromConfig: Failed to parse positions JSON: {ex.Message}");
                }
            }

            return new VirtualBrokerageDecorator(
                innerBrokerage,
                allocatedCapital,
                currentCash,
                strategyInstanceId,
                restoredPositions,
                accountCurrency);
        }

        #region Virtual State Methods (Isolation Layer)

        /// <summary>
        /// Returns ONLY the virtual allocated cash, not the real account balance
        /// </summary>
        public List<CashAmount> GetCashBalance()
        {
            Log.Trace($"VirtualBrokerageDecorator.GetCashBalance: Returning virtual cash ${_virtualCash}");
            return new List<CashAmount> { new CashAmount(_virtualCash, _accountCurrency) };
        }

        /// <summary>
        /// Returns ONLY positions created by this strategy instance
        /// </summary>
        public List<Holding> GetAccountHoldings()
        {
            var holdings = _virtualPositions.Values.ToList();
            Log.Trace($"VirtualBrokerageDecorator.GetAccountHoldings: Returning {holdings.Count} virtual positions");
            return holdings;
        }

        /// <summary>
        /// Returns ONLY orders placed by this strategy instance
        /// </summary>
        public List<Order> GetOpenOrders()
        {
            var allOrders = _innerBrokerage.GetOpenOrders();
            var trackedOrders = allOrders.Where(o => _trackedOrderIds.Contains(o.Id)).ToList();
            Log.Trace($"VirtualBrokerageDecorator.GetOpenOrders: Returning {trackedOrders.Count} of {allOrders.Count} orders");
            return trackedOrders;
        }

        /// <summary>
        /// Gets the current virtual equity (cash + positions value)
        /// </summary>
        public decimal GetVirtualEquity()
        {
            var positionsValue = _virtualPositions.Values.Sum(h => h.MarketValue);
            return _virtualCash + positionsValue;
        }

        #endregion

        #region Order Execution (Hard Validation + Delegation)

        /// <summary>
        /// Places an order after validating it doesn't exceed virtual capital.
        /// </summary>
        public bool PlaceOrder(Order order)
        {
            // Estimate order value for validation
            var estimatedValue = EstimateOrderValue(order);

            // For buy orders, check if we have enough virtual cash
            if (order.Direction == OrderDirection.Buy && estimatedValue > _virtualCash)
            {
                var message = $"Order rejected: Estimated value ${estimatedValue:F2} exceeds virtual cash ${_virtualCash:F2}";
                Log.Error($"VirtualBrokerageDecorator.PlaceOrder: {message}");
                Message?.Invoke(this, new BrokerageMessageEvent(BrokerageMessageType.Warning, -1, message));
                return false;
            }

            // Track this order
            _trackedOrderIds.Add(order.Id);
            Log.Trace($"VirtualBrokerageDecorator.PlaceOrder: Tracking order {order.Id} for strategy {_strategyInstanceId}");

            return _innerBrokerage.PlaceOrder(order);
        }

        /// <summary>
        /// Updates an existing order.
        /// </summary>
        public bool UpdateOrder(Order order)
        {
            return _innerBrokerage.UpdateOrder(order);
        }

        /// <summary>
        /// Cancels an existing order.
        /// </summary>
        public bool CancelOrder(Order order)
        {
            return _innerBrokerage.CancelOrder(order);
        }

        /// <summary>
        /// Estimates the cash value of an order for validation purposes.
        /// </summary>
        private decimal EstimateOrderValue(Order order)
        {
            // Use order price if available, otherwise use a reasonable estimate
            var price = order.Price > 0 ? order.Price : 0m;

            // For market orders, we don't have a price yet - use limit price or 0
            if (order.Type == OrderType.Market && price == 0)
            {
                // Try to get current position's market price as estimate
                if (_virtualPositions.TryGetValue(order.Symbol, out var holding))
                {
                    price = holding.MarketPrice;
                }
            }

            return Math.Abs(order.Quantity) * price;
        }

        #endregion

        #region Virtual State Updates

        private void OnInnerOrdersStatusChanged(object sender, List<OrderEvent> orderEvents)
        {
            foreach (var orderEvent in orderEvents)
            {
                // Only process events for orders we're tracking
                if (!_trackedOrderIds.Contains(orderEvent.OrderId))
                    continue;

                if (orderEvent.Status == OrderStatus.Filled ||
                    orderEvent.Status == OrderStatus.PartiallyFilled)
                {
                    UpdateVirtualStateFromFill(orderEvent);
                }

                // Clean up completed orders from tracking
                if (orderEvent.Status == OrderStatus.Filled ||
                    orderEvent.Status == OrderStatus.Canceled ||
                    orderEvent.Status == OrderStatus.Invalid)
                {
                    _trackedOrderIds.Remove(orderEvent.OrderId);
                }
            }

            // Forward all events
            OrdersStatusChanged?.Invoke(this, orderEvents);
        }

        private void UpdateVirtualStateFromFill(OrderEvent fill)
        {
            var symbol = fill.Symbol;
            var fillValue = fill.FillPrice * Math.Abs(fill.FillQuantity);
            var fee = fill.OrderFee?.Value.Amount ?? 0m;

            // Update cash based on direction
            if (fill.Direction == OrderDirection.Buy)
            {
                _virtualCash -= fillValue + fee;
            }
            else // Sell
            {
                _virtualCash += fillValue - fee;
            }

            // Update position
            if (!_virtualPositions.TryGetValue(symbol, out var holding))
            {
                holding = new Holding
                {
                    Symbol = symbol,
                    Quantity = 0,
                    AveragePrice = 0,
                    CurrencySymbol = _accountCurrency
                };
                _virtualPositions[symbol] = holding;
            }

            var fillQuantity = fill.Direction == OrderDirection.Buy
                ? Math.Abs(fill.FillQuantity)
                : -Math.Abs(fill.FillQuantity);

            var newQuantity = holding.Quantity + fillQuantity;

            if (newQuantity == 0)
            {
                // Position closed
                _virtualPositions.Remove(symbol);
                Log.Trace($"VirtualBrokerageDecorator: Closed position in {symbol}");
            }
            else
            {
                // Update average price for buys that increase position
                if (fill.Direction == OrderDirection.Buy && holding.Quantity >= 0)
                {
                    var totalCost = (holding.AveragePrice * Math.Abs(holding.Quantity)) + fillValue;
                    holding.AveragePrice = totalCost / Math.Abs(newQuantity);
                }
                else if (fill.Direction == OrderDirection.Sell && holding.Quantity <= 0)
                {
                    // Short position getting shorter
                    var totalCost = (holding.AveragePrice * Math.Abs(holding.Quantity)) + fillValue;
                    holding.AveragePrice = totalCost / Math.Abs(newQuantity);
                }

                holding.Quantity = newQuantity;
                holding.MarketPrice = fill.FillPrice;
                holding.MarketValue = Math.Abs(newQuantity) * fill.FillPrice;

                Log.Trace($"VirtualBrokerageDecorator: Updated {symbol} position to {newQuantity} @ ${holding.AveragePrice:F2}");
            }

            Log.Trace($"VirtualBrokerageDecorator: Virtual cash now ${_virtualCash:F2}, " +
                      $"Positions: {_virtualPositions.Count}");
        }

        #endregion

        #region Delegated Properties & Methods (Pass-through to Inner Brokerage)

        /// <summary>
        /// Gets the name of this brokerage wrapper
        /// </summary>
        public string Name => $"Virtual({_innerBrokerage.Name})";

        /// <summary>
        /// Returns true if the inner brokerage is connected
        /// </summary>
        public bool IsConnected => _innerBrokerage.IsConnected;

        /// <summary>
        /// Virtual brokerage manages its own state, doesn't rely on instant updates
        /// </summary>
        public bool AccountInstantlyUpdated => false;

        /// <summary>
        /// Returns the account base currency
        /// </summary>
        public string AccountBaseCurrency => _accountCurrency;

        /// <summary>
        /// Gets or sets whether concurrent processing is enabled
        /// </summary>
        public bool ConcurrencyEnabled
        {
            get => _innerBrokerage.ConcurrencyEnabled;
            set => _innerBrokerage.ConcurrencyEnabled = value;
        }

        /// <summary>
        /// Connects to the inner brokerage
        /// </summary>
        public void Connect() => _innerBrokerage.Connect();

        /// <summary>
        /// Disconnects from the inner brokerage
        /// </summary>
        public void Disconnect() => _innerBrokerage.Disconnect();

        /// <summary>
        /// Gets historical data from the inner brokerage
        /// </summary>
        public IEnumerable<BaseData> GetHistory(HistoryRequest request) => _innerBrokerage.GetHistory(request);

        #region IBrokerageCashSynchronizer (No-op - Virtual brokerage manages its own state)

        private DateTime _lastSyncTime = DateTime.UtcNow;

        /// <summary>
        /// Gets the datetime of the last sync (UTC).
        /// Virtual brokerage doesn't sync from real account.
        /// </summary>
        public DateTime LastSyncDateTimeUtc => _lastSyncTime;

        /// <summary>
        /// Returns whether the brokerage should perform cash synchronization.
        /// Always returns false - virtual brokerage manages its own isolated state.
        /// </summary>
        public bool ShouldPerformCashSync(DateTime currentTimeUtc) => false;

        /// <summary>
        /// Synchronizes the cashbook with the brokerage account.
        /// No-op for virtual brokerage - we maintain isolated state from fills only.
        /// </summary>
        public bool PerformCashSync(IAlgorithm algorithm, DateTime currentTimeUtc, Func<TimeSpan> getTimeSinceLastFill)
        {
            // Virtual brokerage doesn't sync from real account - state is derived from fills only
            _lastSyncTime = currentTimeUtc;
            Log.Trace("VirtualBrokerageDecorator.PerformCashSync: Skipped (virtual isolation active)");
            return true;
        }

        #endregion

        /// <summary>
        /// Disposes of resources
        /// </summary>
        public void Dispose()
        {
            _innerBrokerage.OrdersStatusChanged -= OnInnerOrdersStatusChanged;
            _innerBrokerage.Dispose();
            Log.Trace($"VirtualBrokerageDecorator: Disposed for strategy {_strategyInstanceId}");
        }

        #endregion // Delegated Properties & Methods

        #region Events (Forwarded from Inner Brokerage)

        /// <inheritdoc/>
        public event EventHandler<BrokerageOrderIdChangedEvent> OrderIdChanged;

        /// <inheritdoc/>
        public event EventHandler<List<OrderEvent>> OrdersStatusChanged;

        /// <inheritdoc/>
        public event EventHandler<OrderUpdateEvent> OrderUpdated;

        /// <inheritdoc/>
        public event EventHandler<OrderEvent> OptionPositionAssigned;

        /// <inheritdoc/>
        public event EventHandler<OptionNotificationEventArgs> OptionNotification;

        /// <inheritdoc/>
        public event EventHandler<NewBrokerageOrderNotificationEventArgs> NewBrokerageOrderNotification;

        /// <inheritdoc/>
        public event EventHandler<DelistingNotificationEventArgs> DelistingNotification;

        /// <inheritdoc/>
        public event EventHandler<AccountEvent> AccountChanged;

        /// <inheritdoc/>
        public event EventHandler<BrokerageMessageEvent> Message;

        #endregion
    }
}

