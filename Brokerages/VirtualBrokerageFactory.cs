/*
 * VIBE-FI.COM - Virtual Brokerage Factory
 * Creates a VirtualBrokerageDecorator that wraps any brokerage dynamically.
 */

using System;
using System.Collections.Generic;
using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Brokerages
{
    /// <summary>
    /// Factory that wraps any brokerage with virtual isolation.
    /// Reads the real brokerage type from config and decorates it with VirtualBrokerageDecorator.
    /// </summary>
    public class VirtualBrokerageFactory : BrokerageFactory
    {
        private IBrokerageFactory _innerFactory;
        private readonly string _innerBrokerageName;

        /// <summary>
        /// Gets the brokerage data required to run the brokerage from configuration.
        /// Merges inner brokerage data with virtual brokerage config.
        /// </summary>
        public override Dictionary<string, string> BrokerageData
        {
            get
            {
                var data = _innerFactory?.BrokerageData ?? new Dictionary<string, string>();

                // Add virtual brokerage config
                data["virtual-allocated-capital"] = Config.Get("virtual-allocated-capital", "100000");
                data["virtual-current-cash"] = Config.Get("virtual-current-cash", "");
                data["virtual-strategy-instance-id"] = Config.Get("virtual-strategy-instance-id", "");
                data["virtual-positions-json"] = Config.Get("virtual-positions-json", "");
                data["virtual-inner-brokerage"] = Config.Get("virtual-inner-brokerage", "AlpacaBrokerage");

                return data;
            }
        }

        /// <summary>
        /// Initializes a new instance of the VirtualBrokerageFactory class.
        /// </summary>
        public VirtualBrokerageFactory() : base(typeof(VirtualBrokerageDecorator))
        {
            _innerBrokerageName = Config.Get("virtual-inner-brokerage", "AlpacaBrokerage");
            LoadInnerFactory();
        }

        /// <summary>
        /// Loads the inner brokerage factory dynamically using Composer.
        /// </summary>
        private void LoadInnerFactory()
        {
            try
            {
                // Try to load factory by convention: {BrokerageName}Factory
                var factoryTypeName = $"{_innerBrokerageName}Factory";

                // Search across all loaded assemblies
                _innerFactory = Composer.Instance.GetExportedValueByTypeName<IBrokerageFactory>(factoryTypeName);

                if (_innerFactory != null)
                {
                    Log.Trace($"VirtualBrokerageFactory: Loaded inner factory {factoryTypeName}");
                }
                else
                {
                    Log.Error($"VirtualBrokerageFactory: Could not find factory {factoryTypeName}");
                }
            }
            catch (Exception ex)
            {
                Log.Error($"VirtualBrokerageFactory: Error loading inner factory: {ex.Message}");
            }
        }

        /// <summary>
        /// Gets a brokerage model from the inner factory.
        /// </summary>
        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)
        {
            if (_innerFactory == null)
            {
                throw new InvalidOperationException(
                    $"VirtualBrokerageFactory: Inner factory not loaded for {_innerBrokerageName}");
            }
            return _innerFactory.GetBrokerageModel(orderProvider);
        }

        /// <summary>
        /// Creates a new VirtualBrokerageDecorator wrapping the inner brokerage.
        /// </summary>
        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            if (_innerFactory == null)
            {
                // Try loading again in case it wasn't available during construction
                LoadInnerFactory();

                if (_innerFactory == null)
                {
                    throw new InvalidOperationException(
                        $"VirtualBrokerageFactory: Could not load inner brokerage factory for: {_innerBrokerageName}. " +
                        "Ensure the brokerage assembly is loaded and the factory class follows the naming convention {BrokerageName}Factory.");
                }
            }

            Log.Trace($"VirtualBrokerageFactory: Creating inner brokerage {_innerBrokerageName}");

            // Create the real brokerage
            var innerBrokerage = _innerFactory.CreateBrokerage(job, algorithm);

            Log.Trace($"VirtualBrokerageFactory: Inner brokerage created, wrapping with virtual isolation");

            // Wrap it with virtual isolation using config
            return VirtualBrokerageDecorator.FromConfig(innerBrokerage, algorithm.AccountCurrency);
        }

        /// <summary>
        /// Gets a brokerage message handler from the inner factory.
        /// </summary>
        public override IBrokerageMessageHandler CreateBrokerageMessageHandler(
            IAlgorithm algorithm,
            AlgorithmNodePacket job,
            IApi api)
        {
            return _innerFactory?.CreateBrokerageMessageHandler(algorithm, job, api)
                ?? base.CreateBrokerageMessageHandler(algorithm, job, api);
        }

        /// <summary>
        /// Performs cleanup of resources.
        /// </summary>
        public override void Dispose()
        {
            _innerFactory?.Dispose();
        }
    }
}

