﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Retry.Simple;
using Rebus.Transport.InMem;
#pragma warning disable 1998

namespace Rebus.Tests.Integration
{
    [TestFixture]
    public class TestSecondLevelRetries : FixtureBase
    {
        const string InputQueueName = "2nd level goodness";
        BuiltinHandlerActivator _activator;
        IBus _bus;
        InMemNetwork _network;

        protected override void SetUp()
        {
            _activator = Using(new BuiltinHandlerActivator());

            _network = new InMemNetwork();

            _bus = Configure.With(_activator)
                .Transport(t => t.UseInMemoryTransport(_network, InputQueueName))
                .Options(o => o.SimpleRetryStrategy(secondLevelRetriesEnabled: true))
                .Start();
        }

        [Test]
        public async Task ItWorks()
        {
            var counter = new SharedCounter(1);

            Using(counter);

            _activator.Handle<string>(async str =>
            {
                throw new ApplicationException("1st level!!");
            });

            _activator.Handle<Failed<string>>(async failed =>
            {
                if (failed.Message != "hej med dig!")
                {
                    counter.Fail("Did not receive the expected message!");
                    return;
                }

                counter.Decrement();
            });

            await _bus.SendLocal("hej med dig!");

            counter.WaitForResetEvent();
        }

        [Test]
        public async Task StillWorksWhenIncomingMessageCannotBeDeserialized()
        {
            const string brokenJsonString = @"{'broken': 'json', // DIE!!1}";

            var headers = new Dictionary<string, string>
            {
                {Headers.MessageId, Guid.NewGuid().ToString()},
                {Headers.ContentType, "application/json;charset=utf-8"},
            };
            var body = Encoding.UTF8.GetBytes(brokenJsonString);
            var transportMessage = new TransportMessage(headers, body);
            var inMemTransportMessage = new InMemTransportMessage(transportMessage);  
            _network.Deliver(InputQueueName, inMemTransportMessage);

            await Task.Delay(1000);

            var failedMessage = _network.GetNextOrNull("error");

            Assert.That(failedMessage, Is.Not.Null);
            var bodyString = Encoding.UTF8.GetString(failedMessage.Body);
            Assert.That(bodyString, Is.EqualTo(brokenJsonString));
        }

        [Test]
        public async Task FailedMessageAllowsForAccessingHeaders()
        {
            var counter = new SharedCounter(1);

            Using(counter);

            _activator.Handle<string>(async str =>
            {
                throw new ApplicationException("1st level!!");
            });

            var headersOfFailedMessage = new Dictionary<string,string>();

            _activator.Handle<Failed<string>>(async failed =>
            {
                if (failed.Message != "hej med dig!")
                {
                    counter.Fail("Did not receive the expected message!");
                    return;
                }

                foreach (var kvp in failed.Headers)
                {
                    headersOfFailedMessage.Add(kvp.Key, kvp.Value);
                }

                Console.WriteLine();
                Console.WriteLine("----------------------------------------------------------------------------------------------------");
                Console.WriteLine(failed.ErrorDescription);
                Console.WriteLine("----------------------------------------------------------------------------------------------------");
                Console.WriteLine();

                counter.Decrement();
            });

            var headers = new Dictionary<string, string>
            {
                {"custom-header", "with-a-custom-value" }
            };

            await _bus.SendLocal("hej med dig!", headers);

            counter.WaitForResetEvent();

            Console.WriteLine(string.Join(Environment.NewLine , headersOfFailedMessage.Select(kvp => string.Format("    {0}: {1}", kvp.Key, kvp.Value))));

            Assert.That(headersOfFailedMessage["custom-header"], Is.EqualTo("with-a-custom-value"));
        }
    }
}