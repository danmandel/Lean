/*
 * VIBE-FI.COM
 * Factory for AblyDataQueueHandler discovery by BrokerageSetupHandler.
 */

using System.Collections.Generic;
using System.ComponentModel.Composition;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Lean.Engine.DataFeeds;

namespace QuantConnect.Brokerages
{
    /// <summary>
    /// Factory for AblyDataQueueHandler - needed for BrokerageSetupHandler discovery.
    /// This allows Lean to "preload" the data queue handler configuration.
    /// </summary>
    [Export(typeof(IBrokerageFactory))]
    public class AblyDataQueueHandlerFactory : BrokerageFactory
    {
        /// <summary>
        /// Gets the brokerage data required to run the brokerage from configuration.
        /// </summary>
        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "ably-api-key", Config.Get("ably-api-key", "") },
            { "ably-user-id", Config.Get("ably-user-id", "") }
        };

        /// <summary>
        /// Initializes a new instance of the AblyDataQueueHandlerFactory class.
        /// </summary>
        public AblyDataQueueHandlerFactory() : base(typeof(AblyDataQueueHandler))
        {
        }

        /// <summary>
        /// Creates the brokerage (data queue handler in this case).
        /// </summary>
        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            // AblyDataQueueHandler is a data queue handler, not a full brokerage.
            // However, we can return an instance if needed, or let Composer handle it via IDataQueueHandler.
            // BrokerageSetupHandler expects an IBrokerage, but AblyDataQueueHandler only implements IDataQueueHandler.
            // So we return null here, as the main goal of this factory is configuration discovery.
            return null;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
        }
    }
}

