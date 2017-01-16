// ***********************************************************************
// Solution         : ServiceFabricLearning
// Project          : StateProvider
// File             : RwLock.cs
// Created          : 2017-01-03  1:37 PM
// ***********************************************************************
// <copyright>
//     Copyright © 2016 Kolibre Credit Team. All rights reserved.
// </copyright>
// ***********************************************************************

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace StateProvider
{
    internal class RwLock
    {
        private readonly ReaderWriterLockSlim _rwLock;

        public RwLock()
        {
            _rwLock = new ReaderWriterLockSlim();
        }

        public IDisposable AcquireReadLock()
        {
            return new DisposableReadLock(_rwLock);
        }

        public IDisposable AcquireWriteLock()
        {
            return new DisposableWriteLock(_rwLock);
        }

        #region Nested type: DisposableLockBase

        private abstract class DisposableLockBase : IDisposable
        {
            private bool _isDisposed;

            protected DisposableLockBase(ReaderWriterLockSlim rwLock)
            {
                Lock = rwLock;
                _isDisposed = false;
            }

            protected ReaderWriterLockSlim Lock { get; }

            void IDisposable.Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            [SuppressMessage("ReSharper", "VirtualMemberNeverOverridden.Global")]
            [SuppressMessage("ReSharper", "UnusedParameter.Global")]
            protected virtual void Dispose(bool disposing)
            {
                if (!_isDisposed)
                    OnDispose();
                _isDisposed = true;
            }

            protected abstract void OnDispose();

            ~DisposableLockBase()
            {
                Dispose(false);
            }
        }

        #endregion

        #region Nested type: DisposableReadLock

        private sealed class DisposableReadLock : DisposableLockBase
        {
            public DisposableReadLock(ReaderWriterLockSlim rwLock)
                : base(rwLock)
            {
                Lock.EnterReadLock();
            }

            protected override void OnDispose()
            {
                Lock.ExitReadLock();
            }
        }

        #endregion

        #region Nested type: DisposableWriteLock

        private sealed class DisposableWriteLock : DisposableLockBase
        {
            public DisposableWriteLock(ReaderWriterLockSlim rwLock)
                : base(rwLock)
            {
                Lock.EnterWriteLock();
            }

            protected override void OnDispose()
            {
                Lock.ExitWriteLock();
            }
        }

        #endregion
    }
}