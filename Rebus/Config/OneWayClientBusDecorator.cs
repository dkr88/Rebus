using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Bus.Advanced;
using Rebus.Logging;

namespace Rebus.Config
{
    class OneWayClientBusDecorator : IBus
    {
        readonly IBus _innerBus;
        readonly AdvancedApiDecorator _advancedApiDecorator;

        public OneWayClientBusDecorator(IBus innerBus, IRebusLoggerFactory rebusLoggerFactory)
        {
            _innerBus = innerBus;
            _advancedApiDecorator = new AdvancedApiDecorator(_innerBus.Advanced, rebusLoggerFactory);
        }

        public void Dispose()
        {
            _innerBus.Dispose();
        }

        public Task SendLocal(object commandMessage, Dictionary<string, string> optionalHeaders = null)
        {
            return _innerBus.SendLocal(commandMessage, optionalHeaders);
        }

        public Task Send(object commandMessage, Dictionary<string, string> optionalHeaders = null)
        {
            return _innerBus.Send(commandMessage, optionalHeaders);
        }

        public Task Reply(object replyMessage, Dictionary<string, string> optionalHeaders = null)
        {
            return _innerBus.Reply(replyMessage, optionalHeaders);
        }

        public Task Defer(TimeSpan delay, object message, Dictionary<string, string> optionalHeaders = null)
        {
            return _innerBus.Defer(delay, message, optionalHeaders);
        }

        public IAdvancedApi Advanced
        {
            get { return _advancedApiDecorator; }
        }

        public Task Subscribe<TEvent>()
        {
            return _innerBus.Subscribe<TEvent>();
        }

        public Task Unsubscribe<TEvent>()
        {
            return _innerBus.Unsubscribe<TEvent>();
        }

        public Task Publish(object eventMessage, Dictionary<string, string> optionalHeaders = null)
        {
            return _innerBus.Publish(eventMessage, optionalHeaders);
        }

        class AdvancedApiDecorator : IAdvancedApi
        {
            readonly IAdvancedApi _innerAdvancedApi;
            readonly IRebusLoggerFactory _rebusLoggerFactory;

            public AdvancedApiDecorator(IAdvancedApi innerAdvancedApi, IRebusLoggerFactory rebusLoggerFactory)
            {
                _innerAdvancedApi = innerAdvancedApi;
                _rebusLoggerFactory = rebusLoggerFactory;
            }

            public IWorkersApi Workers
            {
                get { return new OneWayClientWorkersApi(_rebusLoggerFactory); }
            }

            public ITopicsApi Topics
            {
                get { return _innerAdvancedApi.Topics; }
            }

            public IRoutingApi Routing
            {
                get { return _innerAdvancedApi.Routing; }
            }

            public ITransportMessageApi TransportMessage
            {
                get { return _innerAdvancedApi.TransportMessage; }
            }
        }

        class OneWayClientWorkersApi : IWorkersApi
        {
            readonly ILog _log;

            public OneWayClientWorkersApi(IRebusLoggerFactory rebusLoggerFactory)
            {
                _log = rebusLoggerFactory.GetCurrentClassLogger();
            }

            public int Count
            {
                get { return 0; }
            }

            public void SetNumberOfWorkers(int numberOfWorkers)
            {
                if (numberOfWorkers <= 0) return;

                _log.Warn("Attempted to set number of workers to {0}, but this is a one-way client!", numberOfWorkers);
            }
        }
    }
}