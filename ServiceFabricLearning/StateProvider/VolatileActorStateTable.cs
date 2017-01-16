// ***********************************************************************
// Solution         : ServiceFabricLearning
// Project          : StateProvider
// File             : VolatileActorStateTable.cs
// Created          : 2017-01-03  1:33 PM
// ***********************************************************************
// <copyright>
//     Copyright © 2016 Kolibre Credit Team. All rights reserved.
// </copyright>
// ***********************************************************************

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Fabric;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace StateProvider
{
    internal class VolatileActorStateTable<TType, TKey, TValue>
    {
        private readonly Dictionary<TType, Dictionary<TKey, TableEntry>> _committedEntriesTable;

        /// <summary>
        ///     Operations are only committed in sequence number order. This is needed
        ///     to perform builds correctly - i.e. without sequence number "holes" in
        ///     the copy data. ReplicationContext tracks whether a replication operation is
        ///     1) quorum acked
        ///     2) completed
        ///     A replication operation is only completed when it is quorum acked and there
        ///     are no other operations with lower sequence numbers that are not yet
        ///     quorum acked.
        /// </summary>
        private readonly Dictionary<long, ReplicationContext> _pendingReplicationContexts;

        /// <summary>
        ///     Lists of entries are in non-decreasing sequence number order and used to
        ///     take a snapshot of the current state when performing builds. The sequence numbers
        ///     will not be contiguous if there were deletes.
        /// </summary>
        private readonly LinkedList<ListEntry> _committedEntriesList;

        private readonly LinkedList<ListEntry> _uncommittedEntriesList;
        private readonly RwLock _rwLock;

        public VolatileActorStateTable()
        {
            _committedEntriesTable = new Dictionary<TType, Dictionary<TKey, TableEntry>>();
            _pendingReplicationContexts = new Dictionary<long, ReplicationContext>();
            _committedEntriesList = new LinkedList<ListEntry>();
            _uncommittedEntriesList = new LinkedList<ListEntry>();
            _rwLock = new RwLock();
        }

        public void ApplyUpdates(IEnumerable<ActorStateDataWrapper> actorStateDataList)
        {
            using (_rwLock.AcquireWriteLock())
            {
                foreach (ActorStateDataWrapper actorStateData in actorStateDataList)
                {
                    ApplyUpdate_UnderWriteLock(new LinkedListNode<ListEntry>(new ListEntry(actorStateData, null)));
                }
            }
        }

        public async Task CommitUpdateAsync(long sequenceNumber, Exception ex = null)
        {
            if (sequenceNumber == 0L)
            {
                if (ex != null)
                {
                    throw ex;
                }

                throw new FabricException(FabricErrorCode.SequenceNumberCheckFailed);
            }

            List<ReplicationContext> committedReplicationContexts = new List<ReplicationContext>();
            ReplicationContext replicationContext;
            using (_rwLock.AcquireWriteLock())
            {
                // get replicationContext
                // In PrepareUpdate, _pendingReplicationContexts.Add(sequenceNumber, replicationContext);
                replicationContext = _pendingReplicationContexts[sequenceNumber];

                /*
                    public void SetReplicationComplete(Exception replicationException)
                    {
                        IsReplicationComplete = true;
                        _replicationException = replicationException;
                    }
                */

                // 将 sequenceNumber 对应的 ReplicationContext 设置为完成，但是不一定提交到 _committedEntriesTable 中
                replicationContext.SetReplicationComplete(ex);

                // 当前 commit update 的 ActorStateDataWrapper 和 ReplicationContext 就是 _uncommittedEntriesList 中的第一个时
                if (_uncommittedEntriesList.First?.Value.ActorStateDataWrapper.SequenceNumber == sequenceNumber)
                {
                    // 当 _uncommittedEntriesList.First 是已完成的 ReplicationContext
                    while (_uncommittedEntriesList.First?.Value.IsReplicationComplete == true)
                    {
                        LinkedListNode<ListEntry> first = _uncommittedEntriesList.First;
                        _uncommittedEntriesList.RemoveFirst();

                        // if(replicationContext._replicationException != null) replicationContext.Value.IsFailed = true;
                        // 如果在状态同步的过程中未发生任何异常
                        if (!first.Value.IsFailed)
                        {
                            // 提交更新到 _committedEntriesTable 中
                            ApplyUpdate_UnderWriteLock(first);
                        }

                        /*
                            public void CompleteReplication()
                            {
                                if (PendingReplicationContext == null)
                                {
                                    return;
                                }

                                PendingReplicationContext.CompleteListEntry();   // --_associatedEntryCount;
                                PendingReplicationContext = null;
                            }
                        */
                        first.Value.CompleteReplication();

                        long completedSequenceNumber = first.Value.ActorStateDataWrapper.SequenceNumber;

                        // _pendingReplicationContexts[completedSequenceNumber] is ReplicationContext
                        // replicationContext.IsAllEntriesComplete => _associatedEntryCount == 0L
                        if (_pendingReplicationContexts[completedSequenceNumber].IsAllEntriesComplete)
                        {
                            committedReplicationContexts.Add(_pendingReplicationContexts[completedSequenceNumber]);
                            _pendingReplicationContexts.Remove(completedSequenceNumber);
                        }
                    }
                    replicationContext = null;
                }
            }

            foreach (ReplicationContext committedReplicationContext in committedReplicationContexts)
            {
                // 将 Task 设置为完成
                /*
                    public void MarkAsCompleted()
                    {
                        if (_replicationException != null)
                        {
                            _pendingCommitTaskSource.SetException(_replicationException);
                        }
                        else
                        {
                            _pendingCommitTaskSource.SetResult(null);
                        }
                    }
                */
                committedReplicationContext.MarkAsCompleted();
            }

            if (replicationContext == null)
            {
                return;
            }

            // 如果该 replicationContext 按顺序还需要等待前置的 replicationContext 提交，则在这里挂起
            await replicationContext.WaitForCompletionAsync();
        }

        public IReadOnlyDictionary<TKey, TValue> GetActorStateDictionary(TType type)
        {
            using (_rwLock.AcquireReadLock())
            {
                Dictionary<TKey, TValue> dictionary1 = new Dictionary<TKey, TValue>();
                Dictionary<TKey, TableEntry> dictionary2;
                if (_committedEntriesTable.TryGetValue(type, out dictionary2))
                {
                    foreach (TableEntry tableEntry in dictionary2.Values)
                        dictionary1.Add(tableEntry.ActorStateDataWrapper.Key, tableEntry.ActorStateDataWrapper.Value);
                }
                return dictionary1;
            }
        }

        public long GetHighestCommittedSequenceNumber()
        {
            using (_rwLock.AcquireReadLock())
            {
                if (_committedEntriesList.Count > 0)
                    return _committedEntriesList.Last.Value.ActorStateDataWrapper.SequenceNumber;
                return 0;
            }
        }

        public long GetHighestKnownSequenceNumber()
        {
            using (_rwLock.AcquireReadLock())
            {
                if (_uncommittedEntriesList.Count > 0)
                    return _uncommittedEntriesList.Last.Value.ActorStateDataWrapper.SequenceNumber;
                if (_committedEntriesList.Count > 0)
                    return _committedEntriesList.Last.Value.ActorStateDataWrapper.SequenceNumber;
                return 0;
            }
        }

        public ActorStateEnumerator GetShallowCopiesEnumerator(TType type)
        {
            using (_rwLock.AcquireReadLock())
            {
                List<ActorStateDataWrapper> committedEntriesList = new List<ActorStateDataWrapper>();
                Dictionary<TKey, TableEntry> dictionary;
                if (_committedEntriesTable.TryGetValue(type, out dictionary))
                {
                    foreach (TableEntry tableEntry in dictionary.Values)
                        committedEntriesList.Add(tableEntry.ActorStateDataWrapper);
                }
                return new ActorStateEnumerator(committedEntriesList, new List<ActorStateDataWrapper>());
            }
        }

        /// <summary>
        ///     The use of read/write locks means that the process of creating shallow
        ///     copies will necessarily compete with replication operations. i.e.
        ///     The process of preparing for a copy will block replication.
        /// </summary>
        public ActorStateEnumerator GetShallowCopiesEnumerator(long maxSequenceNumber)
        {
            using (_rwLock.AcquireReadLock())
            {
                List<ActorStateDataWrapper> committedEntriesList = new List<ActorStateDataWrapper>();
                List<ActorStateDataWrapper> uncommittedEntriesList = new List<ActorStateDataWrapper>();
                long num = 0;
                foreach (ListEntry committedEntries in _committedEntriesList)
                {
                    if (committedEntries.ActorStateDataWrapper.SequenceNumber <= maxSequenceNumber)
                    {
                        num = committedEntries.ActorStateDataWrapper.SequenceNumber;
                        committedEntriesList.Add(committedEntries.ActorStateDataWrapper);
                    }
                    else
                        break;
                }
                if (num < maxSequenceNumber)
                {
                    foreach (ListEntry uncommittedEntries in _uncommittedEntriesList)
                    {
                        if (uncommittedEntries.ActorStateDataWrapper.SequenceNumber <= maxSequenceNumber)
                            uncommittedEntriesList.Add(uncommittedEntries.ActorStateDataWrapper);
                        else
                            break;
                    }
                }
                return new ActorStateEnumerator(committedEntriesList, uncommittedEntriesList);
            }
        }

        public IEnumerator<TKey> GetSortedStorageKeyEnumerator(TType type, Func<TKey, bool> filter)
        {
            using (_rwLock.AcquireWriteLock())
            {
                List<TKey> keyList = new List<TKey>();
                Dictionary<TKey, TableEntry> dictionary;
                if (_committedEntriesTable.TryGetValue(type, out dictionary))
                {
                    foreach (KeyValuePair<TKey, TableEntry> keyValuePair in dictionary)
                    {
                        if (filter(keyValuePair.Key))
                            keyList.Add(keyValuePair.Key);
                    }
                    keyList.Sort();
                }
                return keyList.GetEnumerator();
            }
        }

        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
        public void PrepareUpdate(IEnumerable<ActorStateDataWrapper> actorStateDataWrapperList, long sequenceNumber)
        {
            if (sequenceNumber == 0L)
            {
                return;
            }

            foreach (ActorStateDataWrapper stateDataWrapper in actorStateDataWrapperList)
            {
                stateDataWrapper.UpdateSequenceNumber(sequenceNumber);
            }

            using (_rwLock.AcquireWriteLock())
            {
                ReplicationContext replicationContext = new ReplicationContext();
                /*
                    public ReplicationContext()
                    {
                        IsReplicationComplete = false;
                        _replicationException = null;
                        _pendingCommitTaskSource = new TaskCompletionSource<object>();
                        _associatedEntryCount = 0L;
                    }
                */
                foreach (ActorStateDataWrapper stateDataWrapper in actorStateDataWrapperList)
                {
                    _uncommittedEntriesList.AddLast(new ListEntry(stateDataWrapper, replicationContext));
                    /*
                        public ListEntry(ActorStateDataWrapper actorStateDataWrapper, ReplicationContext replicationContext)
                        {
                            ActorStateDataWrapper = actorStateDataWrapper;
                            PendingReplicationContext = replicationContext;
                            PendingReplicationContext?.AssociateListEntry();  // PendingReplicationContext._associatedEntryCount ++;
                        }
                    */
                }

                _pendingReplicationContexts.Add(sequenceNumber, replicationContext);
            }
        }

        public bool TryGetValue(TType type, TKey key, out TValue value)
        {
            using (_rwLock.AcquireReadLock())
            {
                Dictionary<TKey, TableEntry> dictionary;
                bool flag = _committedEntriesTable.TryGetValue(type, out dictionary);
                if (flag)
                {
                    TableEntry tableEntry;
                    flag = dictionary.TryGetValue(key, out tableEntry);
                    value = flag ? tableEntry.ActorStateDataWrapper.Value : default(TValue);
                }
                else
                {
                    value = default(TValue);
                }
                return flag;
            }
        }

        /*
            1. VolatileActorStateProvider.ActorActivatedAsync => VolatileActorStateProvider.ReplicateUpdateAsync => 
            VolatileActorStateProvider.ReplicateStateChangesAsync => VolatileActorStateTable.CommitUpdateAsync => 
            VolatileActorStateTable.ApplyUpdate_UnderWriteLock
        */

        private void ApplyUpdate_UnderWriteLock(LinkedListNode<ListEntry> listNode)
        {
            ActorStateDataWrapper actorStateDataWrapper = listNode.Value.ActorStateDataWrapper;
            TType type = actorStateDataWrapper.Type;
            TKey key = actorStateDataWrapper.Key;
            bool isDelete = actorStateDataWrapper.IsDelete;

            TableEntry tableEntry = new TableEntry(actorStateDataWrapper, listNode);
            Dictionary<TKey, TableEntry> dictionary;
            if (!_committedEntriesTable.TryGetValue(type, out dictionary))
            {
                if (isDelete)
                {
                    return;
                }

                dictionary = new Dictionary<TKey, TableEntry>();
                _committedEntriesTable[type] = dictionary;
            }

            // 从 _committedEntriesList 移除原有的 TableEntry
            TableEntry originalTableEntry;
            if (dictionary.TryGetValue(key, out originalTableEntry))
            {
                _committedEntriesList.Remove(originalTableEntry.ListNode);
            }

            // 从 _committedEntriesTable 移除 TableEntry
            if (isDelete)
            {
                dictionary.Remove(key);
            }
            else
            {
                dictionary[key] = tableEntry;
            }

            //TODO: 意图不明
            if (_committedEntriesList.Last?.Value.ActorStateDataWrapper.IsDelete == true)
            {
                _committedEntriesList.RemoveLast();
            }

            _committedEntriesList.AddLast(listNode);
        }

        #region Nested type: ActorStateDataWrapper

        [DataContract]
        public class ActorStateDataWrapper
        {
            private ActorStateDataWrapper(TType type, TKey key, TValue value)
            {
                Type = type;
                Key = key;
                Value = value;
                IsDelete = false;
                SequenceNumber = 0L;
            }

            private ActorStateDataWrapper(TType type, TKey key)
            {
                Type = type;
                Key = key;
                Value = default(TValue);
                IsDelete = true;
                SequenceNumber = 0L;
            }

            [DataMember]
            public TType Type { get; private set; }

            [DataMember]
            public TKey Key { get; private set; }

            [DataMember]
            public TValue Value { get; private set; }

            [DataMember]
            public bool IsDelete { get; private set; }

            [DataMember]
            public long SequenceNumber { get; private set; }

            public static ActorStateDataWrapper CreateForDelete(TType type, TKey key)
            {
                return new ActorStateDataWrapper(type, key);
            }

            public static ActorStateDataWrapper CreateForUpdate(TType type, TKey key, TValue value)
            {
                return new ActorStateDataWrapper(type, key, value);
            }

            internal void UpdateSequenceNumber(long sequenceNumber)
            {
                SequenceNumber = sequenceNumber;
            }
        }

        #endregion

        #region Nested type: ActorStateEnumerator

        public class ActorStateEnumerator : IReadOnlyCollection<ActorStateDataWrapper>, IEnumerable<ActorStateDataWrapper>, IEnumerable, IEnumerator<ActorStateDataWrapper>, IDisposable, IEnumerator
        {
            private readonly List<ActorStateDataWrapper> _committedEntriesListShallowCopy;
            private readonly List<ActorStateDataWrapper> _uncommittedEntriesListShallowCopy;
            private int _index;
            private ActorStateDataWrapper _current;

            public ActorStateEnumerator(List<ActorStateDataWrapper> committedEntriesList, List<ActorStateDataWrapper> uncommittedEntriesList)
            {
                _committedEntriesListShallowCopy = committedEntriesList;
                _uncommittedEntriesListShallowCopy = uncommittedEntriesList;
                _index = 0;
                _current = null;
            }

            public long CommittedCount
            {
                get { return _committedEntriesListShallowCopy.Count; }
            }

            public long UncommittedCount
            {
                get { return _uncommittedEntriesListShallowCopy.Count; }
            }

            int IReadOnlyCollection<ActorStateDataWrapper>.Count
            {
                get { return _committedEntriesListShallowCopy.Count + _uncommittedEntriesListShallowCopy.Count; }
            }

            ActorStateDataWrapper IEnumerator<ActorStateDataWrapper>.Current
            {
                get { return _current; }
            }

            object IEnumerator.Current
            {
                get { return _current; }
            }

            void IDisposable.Dispose()
            {
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this;
            }

            IEnumerator<ActorStateDataWrapper> IEnumerable<ActorStateDataWrapper>.GetEnumerator()
            {
                return this;
            }

            bool IEnumerator.MoveNext()
            {
                return GetNext() != null;
            }

            void IEnumerator.Reset()
            {
                _index = 0;
                _current = null;
            }

            public ActorStateDataWrapper GetNext()
            {
                int count1 = _committedEntriesListShallowCopy.Count;
                int count2 = _uncommittedEntriesListShallowCopy.Count;
                int index = _index++;
                _current = index >= count1 ? (index >= count2 + count1 ? null : _uncommittedEntriesListShallowCopy[index - count1]) : _committedEntriesListShallowCopy[index];
                return _current;
            }

            public ActorStateDataWrapper PeekNext()
            {
                int count1 = _committedEntriesListShallowCopy.Count;
                int count2 = _uncommittedEntriesListShallowCopy.Count;
                int index = _index;
                return index >= count1 ? (index >= count2 + count1 ? null : _uncommittedEntriesListShallowCopy[index - count1]) : _committedEntriesListShallowCopy[index];
            }
        }

        #endregion

        #region Nested type: ListEntry

        private sealed class ListEntry
        {
            public ListEntry(ActorStateDataWrapper actorStateDataWrapper, ReplicationContext replicationContext)
            {
                ActorStateDataWrapper = actorStateDataWrapper;
                PendingReplicationContext = replicationContext;
                PendingReplicationContext?.AssociateListEntry();
            }

            public ActorStateDataWrapper ActorStateDataWrapper { get; }

            private ReplicationContext PendingReplicationContext { get; set; }

            public bool IsReplicationComplete
            {
                get
                {
                    if (PendingReplicationContext != null)
                        return PendingReplicationContext.IsReplicationComplete;
                    return true;
                }
            }

            public bool IsFailed
            {
                get
                {
                    if (PendingReplicationContext != null)
                    {
                        return PendingReplicationContext.IsFailed;
                    }

                    return false;
                }
            }

            public void CompleteReplication()
            {
                if (PendingReplicationContext == null)
                {
                    return;
                }

                PendingReplicationContext.CompleteListEntry(); // --_associatedEntryCount;
                PendingReplicationContext = null;
            }
        }

        #endregion

        #region Nested type: ReplicationContext

        private sealed class ReplicationContext
        {
            private readonly TaskCompletionSource<object> _pendingCommitTaskSource;
            private Exception _replicationException;
            private long _associatedEntryCount;

            public ReplicationContext()
            {
                IsReplicationComplete = false;
                _replicationException = null;
                _pendingCommitTaskSource = new TaskCompletionSource<object>();
                _associatedEntryCount = 0L;
            }

            public bool IsReplicationComplete { get; private set; }

            public bool IsFailed
            {
                get { return _replicationException != null; }
            }

            public bool IsAllEntriesComplete
            {
                get { return _associatedEntryCount == 0L; }
            }

            public void AssociateListEntry()
            {
                ++_associatedEntryCount;
            }

            public void CompleteListEntry()
            {
                --_associatedEntryCount;
            }

            public void MarkAsCompleted()
            {
                if (_replicationException != null)
                {
                    _pendingCommitTaskSource.SetException(_replicationException);
                }
                else
                {
                    _pendingCommitTaskSource.SetResult(null);
                }
            }

            public void SetReplicationComplete(Exception replicationException)
            {
                IsReplicationComplete = true;
                _replicationException = replicationException;
            }

            public async Task WaitForCompletionAsync()
            {
                await _pendingCommitTaskSource.Task;
            }
        }

        #endregion

        #region Nested type: TableEntry

        private sealed class TableEntry
        {
            public TableEntry(ActorStateDataWrapper actorStateDataWrapper, LinkedListNode<ListEntry> listNode)
            {
                ActorStateDataWrapper = actorStateDataWrapper;
                ListNode = listNode;
            }

            public ActorStateDataWrapper ActorStateDataWrapper { get; }

            public LinkedListNode<ListEntry> ListNode { get; }
        }

        #endregion
    }
}