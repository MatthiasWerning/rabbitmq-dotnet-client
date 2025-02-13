﻿using System;
using System.Diagnostics;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Impl;

namespace RabbitMQ.Client.ConsumerDispatching
{
    internal sealed class AsyncConsumerDispatcher : ConsumerDispatcherChannelBase
    {
        internal AsyncConsumerDispatcher(ChannelBase channel, int concurrency)
            : base(channel, concurrency)
        {
        }

        protected override async Task ProcessChannelAsync()
        {
            try
            {
                while (await _reader.WaitToReadAsync().ConfigureAwait(false))
                {
                    while (_reader.TryRead(out WorkStruct work))
                    {
                        using (work)
                        {
                            try
                            {
                                switch (work.WorkType)
                                {
                                    case WorkType.Deliver:
                                        using (Activity? activity = RabbitMQActivitySource.Deliver(work.RoutingKey!, work.Exchange!,
                                            work.DeliveryTag, work.BasicProperties!, work.Body.Size))
                                        {
                                            await work.AsyncConsumer.HandleBasicDeliver(
                                                work.ConsumerTag!, work.DeliveryTag, work.Redelivered,
                                                work.Exchange!, work.RoutingKey!, work.BasicProperties!, work.Body.Memory)
                                                .ConfigureAwait(false);
                                        }
                                        break;
                                    case WorkType.Cancel:
                                        await work.AsyncConsumer.HandleBasicCancel(work.ConsumerTag!)
                                            .ConfigureAwait(false);
                                        break;
                                    case WorkType.CancelOk:
                                        await work.AsyncConsumer.HandleBasicCancelOk(work.ConsumerTag!)
                                            .ConfigureAwait(false);
                                        break;
                                    case WorkType.ConsumeOk:
                                        await work.AsyncConsumer.HandleBasicConsumeOk(work.ConsumerTag!)
                                            .ConfigureAwait(false);
                                        break;
                                    case WorkType.Shutdown:
                                        await work.AsyncConsumer.HandleChannelShutdown(_channel, work.Reason!)
                                            .ConfigureAwait(false);
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                _channel.OnCallbackException(CallbackExceptionEventArgs.Build(e, work.WorkType.ToString(), work.Consumer));
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                if (false == _reader.Completion.IsCompleted)
                {
                    throw;
                }
            }
        }
    }
}
