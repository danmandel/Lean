/*
 * VIBE-FI.COM
 * Factory for AblyDataQueueHandler discovery by BrokerageSetupHandler.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;

namespace QuantConnect.Brokerages
{
    /// <summary>
    /// Factory for AblyDataQueueHandler - needed for BrokerageSetupHandler discovery.
    /// This allows Lean to "preload" the data queue handler configuration.
    /// </summary>
    [Export(typeof(IBrokerageFactory))]
    public class AblyDataQueueHandlerFactory : BrokerageFactory
    {
        private const string HandlerTypeName = "QuantConnect.Lean.Engine.DataFeeds.AblyDataQueueHandler, QuantConnect.Lean.Engine";

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
        public AblyDataQueueHandlerFactory() : base(GetBrokerageType())
        {
        }

        private static Type GetBrokerageType()
        {
            // Resolve by name to avoid Brokerages project depending on the engine assembly at compile time
            return Type.GetType(HandlerTypeName, throwOnError: false) ?? typeof(Brokerage);
        }

        /// <summary>
        /// Returns the brokerage model to use with this data source.
        /// </summary>
        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            return new DefaultBrokerageModel();
        }

        /// <summary>
        /// Creates the brokerage (data queue handler in this case).
        /// </summary>
        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            // AblyDataQueueHandler is a data queue handler, not a full brokerage.
            // However, we can return an instance if needed, or let Composer handle it via IDataQueueHandler.
            // BrokerageSetupHandler expects an IBrokerage, but AblyDataQueueHandler only implements IDataQueueHandler.
            // So we throw here, as the main goal of this factory is configuration discovery.
            throw new NotSupportedException(
                "AblyDataQueueHandlerFactory is used for configuration discovery only. " +
                "Configure Ably as a data queue handler via 'data-queue-handler'.");
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
        }
    }
}
