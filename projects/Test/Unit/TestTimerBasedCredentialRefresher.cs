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
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Xunit;
using Xunit.Abstractions;

namespace Test.Unit
{
    public class MockCredentialsProvider : ICredentialsProvider
    {
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly TimeSpan? _validUntil = TimeSpan.FromSeconds(1);
        private Exception _ex = null;
        private int _refreshCalledTimes = 0;

        public MockCredentialsProvider(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        public MockCredentialsProvider(ITestOutputHelper testOutputHelper, TimeSpan validUntil)
        {
            _testOutputHelper = testOutputHelper;
            _validUntil = validUntil;
        }

        public int RefreshCalledTimes
        {
            get
            {
                return _refreshCalledTimes;
            }
        }

        public string Name => this.GetType().Name;

        public string UserName => "guest";

        public string Password
        {
            get
            {
                if (_ex == null)
                {
                    return "guest";
                }
                else
                {
                    throw _ex;
                }
            }
        }

        public TimeSpan? ValidUntil => _validUntil;

        public void Refresh()
        {
            _refreshCalledTimes++;
        }

        public void PasswordThrows(Exception ex)
        {
            _ex = ex;
        }
    }

    public class TestTimerBasedCredentialsRefresher
    {
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly TimerBasedCredentialRefresher _refresher = new TimerBasedCredentialRefresher();

        public TestTimerBasedCredentialsRefresher(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public void TestRegister()
        {
            Task cb(bool unused) => Task.CompletedTask;
            ICredentialsProvider credentialsProvider = new MockCredentialsProvider(_testOutputHelper);

            Assert.True(credentialsProvider == _refresher.Register(credentialsProvider, cb));
            Assert.True(_refresher.Unregister(credentialsProvider));
        }

        [Fact]
        public void TestDoNotRegisterWhenHasNoExpiry()
        {
            ICredentialsProvider credentialsProvider = new MockCredentialsProvider(_testOutputHelper, TimeSpan.Zero);
            Task cb(bool unused) => Task.CompletedTask;

            _refresher.Register(credentialsProvider, cb);

            Assert.False(_refresher.Unregister(credentialsProvider));
        }

        [Fact]
        public async Task TestRefreshToken()
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                using (CancellationTokenRegistration ctr = cts.Token.Register(() => tcs.TrySetCanceled()))
                {
                    var credentialsProvider = new MockCredentialsProvider(_testOutputHelper, TimeSpan.FromSeconds(1));

                    Task cb(bool arg)
                    {
                        tcs.SetResult(arg);
                        return Task.CompletedTask;
                    }

                    _refresher.Register(credentialsProvider, cb);
                    Assert.True(await tcs.Task);
                    Assert.True(credentialsProvider.RefreshCalledTimes > 0);
                    Assert.True(_refresher.Unregister(credentialsProvider));
                }
            }
        }

        [Fact]
        public async Task TestRefreshTokenUpdateCallback()
        {
            var tcs1 = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var tcs2 = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int cb1CalledTimes = 0;
            int cb2CalledTimes = 0;

            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                using (CancellationTokenRegistration ctr = cts.Token.Register(() => { tcs1.TrySetCanceled(); tcs2.TrySetCanceled(); })) {
                    var credentialsProvider = new MockCredentialsProvider(_testOutputHelper, TimeSpan.FromSeconds(1));

                    Task cb1(bool arg)
                    {
                        cb1CalledTimes++;
                        tcs1.SetResult(arg);
                        return Task.CompletedTask;
                    }

                    Task cb2(bool arg)
                    {
                        cb2CalledTimes++;
                        tcs2.SetResult(arg);
                        return Task.CompletedTask;
                    }

                    _refresher.Register(credentialsProvider, cb1);
                    Assert.True(await tcs1.Task);
                    Assert.True(credentialsProvider.RefreshCalledTimes == 1);
                    Assert.True(cb1CalledTimes == 1);
                    _refresher.Register(credentialsProvider, cb2);
                    Assert.True(await tcs2.Task);
                    Assert.True(credentialsProvider.RefreshCalledTimes == 2);
                    Assert.True(cb2CalledTimes == 1);
                    Assert.True(cb1CalledTimes == 1);

                    Assert.True(_refresher.Unregister(credentialsProvider));
                }
            }
        }

        [Fact]
        public async Task TestRefreshTokenFailed()
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                using (CancellationTokenRegistration ctr = cts.Token.Register(() => tcs.TrySetCanceled()))
                {
                    var credentialsProvider = new MockCredentialsProvider(_testOutputHelper, TimeSpan.FromSeconds(1));

                    Task cb(bool arg)
                    {
                        tcs.SetResult(arg);
                        return Task.CompletedTask;
                    }

                    var ex = new Exception();
                    credentialsProvider.PasswordThrows(ex);

                    _refresher.Register(credentialsProvider, cb);
                    Assert.False(await tcs.Task);
                    Assert.True(credentialsProvider.RefreshCalledTimes > 0);
                    Assert.True(_refresher.Unregister(credentialsProvider));
                }
            }
        }
    }
}
