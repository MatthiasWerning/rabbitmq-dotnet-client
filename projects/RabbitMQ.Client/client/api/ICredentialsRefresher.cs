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
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Tracing;
using System.Threading.Tasks;
using System.Timers;

namespace RabbitMQ.Client
{
    public interface ICredentialsRefresher : IDisposable
    {
        ICredentialsProvider Register(ICredentialsProvider provider, NotifyCredentialRefreshedAsync callback);
        bool Unregister(ICredentialsProvider provider);

        delegate Task NotifyCredentialRefreshedAsync(bool successfully);
    }

    [EventSource(Name = "TimerBasedCredentialRefresher")]
    public class TimerBasedCredentialRefresherEventSource : EventSource
    {
        public static TimerBasedCredentialRefresherEventSource Log { get; } = new TimerBasedCredentialRefresherEventSource();

        [Event(1)]
        public void Registered(string name) => WriteEvent(1, "Registered", name);
        [Event(2)]
        public void Unregistered(string name) => WriteEvent(2, "UnRegistered", name);
        [Event(3)]
#if NET6_0_OR_GREATER
        [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026:RequiresUnreferencedCode", Justification = "Parameters to this method are primitive and are trimmer safe")]
#endif
        public void ScheduledTimer(string name, double interval) => WriteEvent(3, "ScheduledTimer", name, interval);
        [Event(4)]
        public void TriggeredTimer(string name) => WriteEvent(4, "TriggeredTimer", name);
        [Event(5)]
#if NET6_0_OR_GREATER
        [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026:RequiresUnreferencedCode", Justification = "Parameters to this method are primitive and are trimmer safe")]
#endif
        public void RefreshedCredentials(string name, bool succesfully) => WriteEvent(5, "RefreshedCredentials", name, succesfully);
        [Event(6)]
        public void AlreadyRegistered(string name) => WriteEvent(6, "AlreadyRegistered", name);
    }

    public class TimerBasedCredentialRefresher : ICredentialsRefresher
    {
        private ICredentialsProvider? _credentialsProvider;
        private TimerRegistration? _registration;
        private bool _disposed = false;

        public ICredentialsProvider Register(ICredentialsProvider provider, ICredentialsRefresher.NotifyCredentialRefreshedAsync callback)
        {
            // TODO should this be ArgumentException?
            if (provider.ValidUntil is null)
            {
                return provider;
            }

            // TODO should this be ArgumentException?
            if (provider.ValidUntil == default(TimeSpan))
            {
                return provider;
            }

            Reset();

            _credentialsProvider = provider;
            _registration = new TimerRegistration(callback);

            _registration.ScheduleTimer(_credentialsProvider);
            TimerBasedCredentialRefresherEventSource.Log.Registered(provider.Name);

            return _credentialsProvider;
        }

        public bool Unregister(ICredentialsProvider provider)
        {
            if (false == Object.ReferenceEquals(provider, _credentialsProvider))
            {
                return false;
            }

            TimerBasedCredentialRefresherEventSource.Log.Unregistered(provider.Name);
            _registration?.Dispose();

            _credentialsProvider = null;
            _registration = null;

            return true;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }

            try
            {
                Reset();
            }
            finally
            {
                _disposed = true;
            }
        }

        private void Reset()
        {
            if (_credentialsProvider is not null)
            {
                TimerBasedCredentialRefresherEventSource.Log.Unregistered(_credentialsProvider.Name);
                _credentialsProvider = null;
            }

            if (_registration is not null)
            {
                _registration.Dispose();
                _registration = null;
            }
        }

        private class TimerRegistration : IDisposable
        {
            private Timer? _timer;
            private bool _disposed;

            public ICredentialsRefresher.NotifyCredentialRefreshedAsync Callback { get; set; }

            public TimerRegistration(ICredentialsRefresher.NotifyCredentialRefreshedAsync callback)
            {
                Callback = callback;
            }

            public void ScheduleTimer(ICredentialsProvider provider)
            {
                if (provider.ValidUntil == null)
                {
                    throw new ArgumentNullException(nameof(provider.ValidUntil) + " of " + provider.GetType().Name + " was null");
                }

                if (_disposed)
                {
                    return;
                }

                _timer = new Timer();
                _timer.Interval = provider.ValidUntil.Value.TotalMilliseconds * (1.0 - 1 / 3.0);
                _timer.Elapsed += async (o, e) =>
                {
                    TimerBasedCredentialRefresherEventSource.Log.TriggeredTimer(provider.Name);
                    if (_disposed)
                    {
                        // We were waiting and the registration has been disposed in meanwhile
                        _timer.Dispose();
                        return;
                    }

                    try
                    {
                        _timer.Stop();
                        provider.Refresh();
                        await Callback.Invoke(provider.Password != null).ConfigureAwait(false);
                        TimerBasedCredentialRefresherEventSource.Log.RefreshedCredentials(provider.Name, true);
                    }
                    catch (Exception)
                    {
                        await Callback.Invoke(false).ConfigureAwait(false);
                        TimerBasedCredentialRefresherEventSource.Log.RefreshedCredentials(provider.Name, false);
                    }
                    finally
                    {
                        _timer.Start();
                    }
                };

                _timer.AutoReset = true;
                _timer.Enabled = true;
                TimerBasedCredentialRefresherEventSource.Log.ScheduledTimer(provider.Name, _timer.Interval);
            }

            public void Dispose()
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(GetType().FullName);
                }

                try
                {
                    _timer?.Stop();
                    _disposed = true;
                }
                finally
                {
                    _timer?.Dispose();
                    _timer = null;
                }
            }
        }
    }

    class NoOpCredentialsRefresher : ICredentialsRefresher
    {
        public ICredentialsProvider Register(ICredentialsProvider provider, ICredentialsRefresher.NotifyCredentialRefreshedAsync callback)
        {
            return provider;
        }

        public bool Unregister(ICredentialsProvider provider)
        {
            return false;
        }

        public void Dispose()
        {
        }
    }
}
