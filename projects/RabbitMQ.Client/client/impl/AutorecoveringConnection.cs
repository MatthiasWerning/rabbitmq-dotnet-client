// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
//
// The APL v2.0:
//
//---------------------------------------------------------------------------
//   Copyright (c) 2007-2024 Broadcom. All Rights Reserved.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       https://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//---------------------------------------------------------------------------
//
// The MPL v2.0:
//
//---------------------------------------------------------------------------
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
//  Copyright (c) 2007-2024 Broadcom. All Rights Reserved.
//---------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Impl;

namespace RabbitMQ.Client.Framing.Impl
{
    internal sealed partial class AutorecoveringConnection : IConnection
    {
        private readonly ConnectionConfig _config;
        // list of endpoints provided on initial connection.
        // on re-connection, the next host in the line is chosen using
        // IHostnameSelector
        private readonly IEndpointResolver _endpoints;

        private Connection _innerConnection;
        private bool _disposed;

        private Connection InnerConnection
        {
            get
            {
                ThrowIfDisposed();
                return _innerConnection;
            }
        }

        internal AutorecoveringConnection(ConnectionConfig config, IEndpointResolver endpoints, Connection innerConnection)
        {
            _config = config;
            _endpoints = endpoints;
            _innerConnection = innerConnection;

            ConnectionShutdown += HandleConnectionShutdown;
            _recoverySucceededWrapper = new EventingWrapper<EventArgs>("OnConnectionRecovery", onException);
            _connectionRecoveryErrorWrapper = new EventingWrapper<ConnectionRecoveryErrorEventArgs>("OnConnectionRecoveryError", onException);
            _consumerTagChangeAfterRecoveryWrapper = new EventingWrapper<ConsumerTagChangedAfterRecoveryEventArgs>("OnConsumerRecovery", onException);
            _queueNameChangedAfterRecoveryWrapper = new EventingWrapper<QueueNameChangedAfterRecoveryEventArgs>("OnQueueRecovery", onException);

            void onException(Exception exception, string context) =>
                _innerConnection.OnCallbackException(CallbackExceptionEventArgs.Build(exception, context));
        }

        internal static async ValueTask<AutorecoveringConnection> CreateAsync(ConnectionConfig config, IEndpointResolver endpoints,
            CancellationToken cancellationToken)
        {
            IFrameHandler fh = await endpoints.SelectOneAsync(config.FrameHandlerFactoryAsync, cancellationToken)
                .ConfigureAwait(false);
            Connection innerConnection = new(config, fh);
            AutorecoveringConnection connection = new(config, endpoints, innerConnection);
            await innerConnection.OpenAsync(cancellationToken)
                .ConfigureAwait(false);
            return connection;
        }

        public event EventHandler<EventArgs> RecoverySucceeded
        {
            add => _recoverySucceededWrapper.AddHandler(value);
            remove => _recoverySucceededWrapper.RemoveHandler(value);
        }
        private EventingWrapper<EventArgs> _recoverySucceededWrapper;

        public event EventHandler<ConnectionRecoveryErrorEventArgs> ConnectionRecoveryError
        {
            add => _connectionRecoveryErrorWrapper.AddHandler(value);
            remove => _connectionRecoveryErrorWrapper.RemoveHandler(value);
        }
        private EventingWrapper<ConnectionRecoveryErrorEventArgs> _connectionRecoveryErrorWrapper;

        public event EventHandler<CallbackExceptionEventArgs> CallbackException
        {
            add => InnerConnection.CallbackException += value;
            remove => InnerConnection.CallbackException -= value;
        }

        public event EventHandler<ConnectionBlockedEventArgs> ConnectionBlocked
        {
            add => InnerConnection.ConnectionBlocked += value;
            remove => InnerConnection.ConnectionBlocked -= value;
        }

        public event EventHandler<ShutdownEventArgs> ConnectionShutdown
        {
            add => InnerConnection.ConnectionShutdown += value;
            remove => InnerConnection.ConnectionShutdown -= value;
        }

        public event EventHandler<EventArgs> ConnectionUnblocked
        {
            add => InnerConnection.ConnectionUnblocked += value;
            remove => InnerConnection.ConnectionUnblocked -= value;
        }

        public event EventHandler<ConsumerTagChangedAfterRecoveryEventArgs> ConsumerTagChangeAfterRecovery
        {
            add => _consumerTagChangeAfterRecoveryWrapper.AddHandler(value);
            remove => _consumerTagChangeAfterRecoveryWrapper.RemoveHandler(value);
        }
        private EventingWrapper<ConsumerTagChangedAfterRecoveryEventArgs> _consumerTagChangeAfterRecoveryWrapper;

        public event EventHandler<QueueNameChangedAfterRecoveryEventArgs> QueueNameChangedAfterRecovery
        {
            add => _queueNameChangedAfterRecoveryWrapper.AddHandler(value);
            remove => _queueNameChangedAfterRecoveryWrapper.RemoveHandler(value);
        }
        private EventingWrapper<QueueNameChangedAfterRecoveryEventArgs> _queueNameChangedAfterRecoveryWrapper;

        public event EventHandler<RecoveringConsumerEventArgs> RecoveringConsumer
        {
            add => _consumerAboutToBeRecovered.AddHandler(value);
            remove => _consumerAboutToBeRecovered.RemoveHandler(value);
        }
        private EventingWrapper<RecoveringConsumerEventArgs> _consumerAboutToBeRecovered;

        public string? ClientProvidedName => _config.ClientProvidedName;

        public ushort ChannelMax => InnerConnection.ChannelMax;

        public IDictionary<string, object?> ClientProperties => InnerConnection.ClientProperties;

        public ShutdownEventArgs? CloseReason => InnerConnection.CloseReason;

        public AmqpTcpEndpoint Endpoint => InnerConnection.Endpoint;

        public uint FrameMax => InnerConnection.FrameMax;

        public TimeSpan Heartbeat => InnerConnection.Heartbeat;

        public bool IsOpen => _innerConnection.IsOpen;

        public int LocalPort => InnerConnection.LocalPort;

        public int RemotePort => InnerConnection.RemotePort;

        public IDictionary<string, object?>? ServerProperties => InnerConnection.ServerProperties;

        public IEnumerable<ShutdownReportEntry> ShutdownReport => InnerConnection.ShutdownReport;

        public IProtocol Protocol => Endpoint.Protocol;

        public bool DispatchConsumersAsyncEnabled => _config.DispatchConsumersAsync;

        public async ValueTask<RecoveryAwareChannel> CreateNonRecoveringChannelAsync(CancellationToken cancellationToken)
        {
            ISession session = InnerConnection.CreateSession();
            var result = new RecoveryAwareChannel(_config, session);
            return (RecoveryAwareChannel)await result.OpenAsync(cancellationToken).ConfigureAwait(false);
        }

        public override string ToString()
            => $"AutorecoveringConnection({InnerConnection.Id},{Endpoint},{GetHashCode()})";

        internal Task CloseFrameHandlerAsync()
        {
            return InnerConnection.FrameHandler.CloseAsync(CancellationToken.None);
        }

        ///<summary>API-side invocation of updating the secret.</summary>
        public Task UpdateSecretAsync(string newSecret, string reason,
            CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            EnsureIsOpen();
            return _innerConnection.UpdateSecretAsync(newSecret, reason, cancellationToken);
        }

        ///<summary>Asynchronous API-side invocation of connection.close with timeout.</summary>
        public async Task CloseAsync(ushort reasonCode, string reasonText, TimeSpan timeout, bool abort,
            CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            Task CloseInnerConnectionAsync()
            {
                if (_innerConnection.IsOpen)
                {
                    return _innerConnection.CloseAsync(reasonCode, reasonText, timeout, abort, cancellationToken);
                }
                else
                {
                    return Task.CompletedTask;
                }
            }

            try
            {
                await StopRecoveryLoopAsync(cancellationToken)
                    .ConfigureAwait(false);

                await CloseInnerConnectionAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                try
                {
                    await CloseInnerConnectionAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception innerConnectionException)
                {
                    throw new AggregateException(ex, innerConnectionException);
                }

                throw;
            }
        }

        public async Task<IChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
        {
            EnsureIsOpen();
            RecoveryAwareChannel recoveryAwareChannel = await CreateNonRecoveringChannelAsync(cancellationToken)
                .ConfigureAwait(false);
            AutorecoveringChannel channel = new AutorecoveringChannel(this, recoveryAwareChannel);
            await RecordChannelAsync(channel, channelsSemaphoreHeld: false, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
            return channel;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                _innerConnection.Dispose();
            }
            catch (OperationInterruptedException)
            {
                // ignored, see rabbitmq/rabbitmq-dotnet-client#133
            }
            finally
            {
                _channels.Clear();
                _recordedEntitiesSemaphore.Dispose();
                _channelsSemaphore.Dispose();
                _recoveryCancellationTokenSource.Dispose();
                _disposed = true;
            }
        }

        private void EnsureIsOpen()
            => InnerConnection.EnsureIsOpen();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                ThrowDisposed();
            }

            static void ThrowDisposed() => throw new ObjectDisposedException(typeof(AutorecoveringConnection).FullName);
        }
    }
}
