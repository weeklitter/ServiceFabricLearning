// Decompiled with JetBrains decompiler
// Type: Microsoft.ServiceFabric.Actors.Runtime.VolatileActorStateProvider
// Assembly: Microsoft.ServiceFabric.Actors, Version=5.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35
// MVID: 1D1D0DFF-C148-449A-8CB0-851E17B7D4D2
// Assembly location: C:\Respositories\credit.kolibre.server\packages\Microsoft.ServiceFabric.Actors.2.3.301\lib\net45\Microsoft.ServiceFabric.Actors.dll

using Microsoft.ServiceFabric.Actors.Query;
using Microsoft.ServiceFabric.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Fabric;
using System.Fabric.Common;
using System.Globalization;
using System.IO;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Runtime;
using StateProvider.Properties;

namespace StateProvider
{
  /// <summary>
  /// Provides an implementation of <see cref="T:Microsoft.ServiceFabric.Actors.Runtime.IActorStateProvider" /> where actor state is kept in-memory and is volatile.
  /// </summary>
  [SuppressMessage("ReSharper", "RedundantExtendsListEntry")]
  public class VolatileActorStateProvider : IActorStateProvider, IStateProviderReplica, IStateProvider, VolatileLogicalTimeManager.ISnapshotHandler
  {
    private static readonly ActorStateData s_actorPresenceValue = new ActorStateData(new byte[1]);
    private const string LOGICAL_TIMESTAMP_KEY = "LogicalTimestamp";
    private const int DEFAULT_TRANSIENT_ERROR_RETRY_DELAY_IN_SECONDS = 1;
    private const string TRACE_TYPE = "VolatileActorStateProvider";
    private readonly VolatileActorStateTable<ActorStateType, string, ActorStateData> _stateTable;
    private readonly DataContractSerializer _copyOrReplicationOperationSerializer;
    private readonly VolatileLogicalTimeManager _logicalTimeManager;
    private readonly ActorStateProviderSerializer _actorStateSerializer;
    private readonly object _replicationLock;
    private readonly ActorStateProviderHelper _actorStateProviderHelper;
    private readonly ReplicatorSettings _userDefinedReplicatorSettings;
    private readonly TimeSpan _transientErrorRetryDelay;
    private SecondaryPump _secondaryPump;
    private ActorTypeInformation _actorTypeInformation;
    private FabricReplicator _fabricReplicator;
    private IStateReplicator2 _stateReplicator;
    private ReplicaRole _replicaRole;
    private IStatefulServicePartition _partition;
    private string _traceType;
    private string _traceId;
    private StatefulServiceInitializationParameters _initParams;

    /// <summary>Function called during suspected data-loss.</summary>
    /// <value>A function representing data-loss callback function.</value>
    public Func<CancellationToken, Task<bool>> OnDataLossAsync { private get; set; }

    /// <summary>
    /// Creates an instance of <see cref="T:Microsoft.ServiceFabric.Actors.Runtime.VolatileActorStateProvider" />.
    /// </summary>
    public VolatileActorStateProvider()
      : this(TimeSpan.FromSeconds(1.0))
    {
    }

    /// <summary>
    /// Creates an instance of <see cref="T:Microsoft.ServiceFabric.Actors.Runtime.VolatileActorStateProvider" /> with
    /// specified replicator settings.
    /// </summary>
    /// <param name="replicatorSettings">
    /// A <see cref="T:System.Fabric.ReplicatorSettings" /> that describes replicator settings.
    /// </param>
    public VolatileActorStateProvider(ReplicatorSettings replicatorSettings)
      : this(TimeSpan.FromSeconds(1.0))
    {
      _userDefinedReplicatorSettings = replicatorSettings;
    }

    internal VolatileActorStateProvider(TimeSpan retryDelay)
    {
      _stateTable = new VolatileActorStateTable<ActorStateType, string, ActorStateData>();
      _copyOrReplicationOperationSerializer = CreateCopyOrReplicationOperationSerializer();
      _actorStateSerializer = new ActorStateProviderSerializer();
      _logicalTimeManager = new VolatileLogicalTimeManager((VolatileLogicalTimeManager.ISnapshotHandler) this);
      _secondaryPump = (SecondaryPump) null;
      _replicationLock = new object();
      _replicaRole = ReplicaRole.Unknown;
      _transientErrorRetryDelay = retryDelay;
      _actorStateProviderHelper = new ActorStateProviderHelper((IActorStateProviderInternal) this);
    }

    void IActorStateProvider.Initialize(ActorTypeInformation actorTypeInfo)
    {
      _actorTypeInformation = actorTypeInfo;
    }

      async Task IActorStateProvider.ActorActivatedAsync(ActorId actorId, CancellationToken cancellationToken)
      {
            // 从 _stateTable 找到 ActorStateData 并且返回
            string key = ActorStateProviderHelper.CreateActorPresenceStorageKey(actorId);
          ActorStateData data;
          if (_stateTable.TryGetValue(ActorStateType.Actor, key, out data))
          {
              return;
          }

            // 如果 _stateTable 中不存在该 actorId 对应的 ActorStateData，则初始化该 actorId 对应的 ActorStateData
            await ExecuteWithRetriesAsync(() =>
          {
              cancellationToken.ThrowIfCancellationRequested();
              return ReplicateUpdateAsync(ActorStateType.Actor, key, s_actorPresenceValue);
          }, cancellationToken);
      }

      Task IActorStateProvider.ReminderCallbackCompletedAsync(ActorId actorId, IActorReminder reminder, CancellationToken cancellationToken)
    {
      string reminderCompletedKey = ActorStateProviderHelper.CreateReminderCompletedStorageKey(actorId, reminder.Name);
      ActorStateData actorStateData = new ActorStateData(new ReminderCompletedData(_logicalTimeManager.CurrentLogicalTime, DateTime.UtcNow));
      return _actorStateProviderHelper.ExecuteWithRetriesAsync((Func<Task>) (() =>
      {
        cancellationToken.ThrowIfCancellationRequested();
        return ReplicateUpdateAsync(ActorStateType.Actor, reminderCompletedKey, actorStateData);
      }), cancellationToken);
    }

      Task<T> IActorStateProvider.LoadStateAsync<T>(ActorId actorId, string stateName, CancellationToken cancellationToken)
      {
          if (actorId == null)
          {
              throw new ArgumentNullException(nameof(actorId));
          }

          if (string.IsNullOrEmpty(stateName))
          {
              throw new ArgumentException(Resources.Argument_EmptyOrNullString, nameof(stateName));
          }

          ActorStateData actorStateData;
          if (_stateTable.TryGetValue(ActorStateType.Actor, CreateActorStorageKey(actorId, stateName), out actorStateData))
          {
              return Task.FromResult<T>(_actorStateSerializer.Deserialize<T>(actorStateData.ActorState));
          }

            throw new KeyNotFoundException($"Actor State with name {stateName} was not found.");
      }

      /*
          ActorStateChange
          {   
              string StateName;
              Type Type;
              object Value;
              StateChangeKind ChangeKind;
          }

          StateChangeKind 
          {
              None,
              Add,
              Update,
              Remove
          }
           */
    async Task IActorStateProvider.SaveStateAsync(ActorId actorId, IReadOnlyCollection<ActorStateChange> stateChanges, CancellationToken cancellationToken)
    {
      List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> actorStateDataWrapperList = new List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>();
      foreach (ActorStateChange stateChange in (IEnumerable<ActorStateChange>) stateChanges)
      {
        // $"{ActorIdKind}_{ActorId}_{StateName}"
        string actorStorageKey = CreateActorStorageKey(actorId, stateChange.StateName);
        if (stateChange.ChangeKind == StateChangeKind.Add || stateChange.ChangeKind == StateChangeKind.Update)
        {
          byte[] state = _actorStateSerializer.Serialize<object>(stateChange.Type, stateChange.Value);

        /*
            public static ActorStateDataWrapper CreateForUpdate(TType type, TKey key, TValue value)
            {
                return new ActorStateDataWrapper(type, key, value);
            }

            type = ActorStateType.Actor;
            key = actorStorageKey;
            value = new ActorStateData(state); 

            public ActorStateData(byte[] state)
            {
                ActorState = state;
            }
        */
        actorStateDataWrapperList.Add(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForUpdate(ActorStateType.Actor, actorStorageKey, new ActorStateData(state)));
        }
        else if (stateChange.ChangeKind == StateChangeKind.Remove)
        {
            /*
            public static ActorStateDataWrapper CreateForDelete(TType type, TKey key)
            {
                return new ActorStateDataWrapper(type, key);
            }

            type = ActorStateType.Actor;
            key = actorStorageKey;
            */
            actorStateDataWrapperList.Add(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForDelete(ActorStateType.Actor, actorStorageKey));
        }
      }
        if (actorStateDataWrapperList.Count <= 0)
        {
            return;
        }

      await ExecuteWithRetriesAsync(async () =>
      {
          cancellationToken.ThrowIfCancellationRequested();
          await ReplicateStateChangesAsync(actorStateDataWrapperList);
      }, cancellationToken);
    }

      Task<bool> IActorStateProvider.ContainsStateAsync(ActorId actorId, string stateName, CancellationToken cancellationToken)
      {
          if (actorId == null)
          {
              throw new ArgumentNullException(nameof(actorId));
          }

          if (string.IsNullOrEmpty(stateName))
          {
              throw new ArgumentException(Resources.Argument_EmptyOrNullString, nameof(stateName));
          }

          ActorStateData actorStateData;
          if (_stateTable.TryGetValue(ActorStateType.Actor, CreateActorStorageKey(actorId, stateName), out actorStateData))
          {
              return Task.FromResult(true);
          }
          return Task.FromResult(false);
      }

      async Task IActorStateProvider.RemoveActorAsync(ActorId actorId, CancellationToken cancellationToken)
    {
      List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> actorStateDataWrapperList = new List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>();
      string actorStorageKeyPrefix = CreateActorStorageKeyPrefix(actorId, string.Empty);
      string reminderCompletedKeyPrefix = ActorStateProviderHelper.CreateReminderCompletedStorageKeyPrefix(actorId);
      VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateEnumerator actorStateEnumerator = _stateTable.GetShallowCopiesEnumerator(ActorStateType.Actor);
      VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper actorStateDataWrapper;
      while ((actorStateDataWrapper = actorStateEnumerator.GetNext()) != null)
      {
        if (actorStateDataWrapper.Key == actorStorageKeyPrefix || actorStateDataWrapper.Key.StartsWith(actorStorageKeyPrefix + "_") || actorStateDataWrapper.Key.StartsWith(reminderCompletedKeyPrefix))
          actorStateDataWrapperList.Add(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForDelete(ActorStateType.Actor, actorStateDataWrapper.Key));
      }
      string reminderStorgaeKeyPrefix = CreateReminderStorageKeyPrefix(actorId, string.Empty);
      VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateEnumerator reminderEnumerator = _stateTable.GetShallowCopiesEnumerator(ActorStateType.Reminder);
      while ((actorStateDataWrapper = reminderEnumerator.GetNext()) != null)
      {
        if (actorStateDataWrapper.Key.StartsWith(reminderStorgaeKeyPrefix))
          actorStateDataWrapperList.Add(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForDelete(ActorStateType.Reminder, actorStateDataWrapper.Key));
      }
      string key = ActorStateProviderHelper.CreateActorPresenceStorageKey(actorId);
      actorStateDataWrapperList.Add(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForDelete(ActorStateType.Actor, key));
      if (actorStateDataWrapperList.Count <= 0)
        return;
      await _actorStateProviderHelper.ExecuteWithRetriesAsync((Func<Task>) (async () =>
      {
        cancellationToken.ThrowIfCancellationRequested();
        await ReplicateStateChangesAsync((IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>) actorStateDataWrapperList);
      }), cancellationToken);
    }

    Task<IEnumerable<string>> IActorStateProvider.EnumerateStateNamesAsync(ActorId actorId, CancellationToken cancellationToken)
    {
      List<string> stringList = new List<string>();
      string storageKeyPrefix = CreateActorStorageKeyPrefix(actorId, string.Empty);
      VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateEnumerator copiesEnumerator = _stateTable.GetShallowCopiesEnumerator(ActorStateType.Actor);
      VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper next;
      while ((next = copiesEnumerator.GetNext()) != null)
      {
        if (next.Key == storageKeyPrefix || next.Key.StartsWith(storageKeyPrefix + "_"))
          stringList.Add(ExtractStateName(actorId, next.Key));
      }
      return Task.FromResult<IEnumerable<string>>((IEnumerable<string>) stringList);
    }

    Task<PagedResult<ActorId>> IActorStateProvider.GetActorsAsync(int itemsCount, ContinuationToken continuationToken, CancellationToken cancellationToken)
    {
      return _actorStateProviderHelper.GetStoredActorIds<string>(itemsCount, continuationToken, (Func<IEnumerator<string>>) (() => _stateTable.GetSortedStorageKeyEnumerator(ActorStateType.Actor, (Func<string, bool>) (key => key.StartsWith("@@")))), (Func<string, string>) (storageKey => storageKey), cancellationToken);
    }

    Task IActorStateProvider.SaveReminderAsync(ActorId actorId, IActorReminder reminder, CancellationToken cancellationToken)
    {
      List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> actorStateDataWrapperList = new List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>()
      {
        VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForUpdate(ActorStateType.Reminder, CreateReminderStorageKey(actorId, reminder.Name), new ActorStateData(new ActorReminderData(actorId, reminder, _logicalTimeManager.CurrentLogicalTime))),
        VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForDelete(ActorStateType.Actor, ActorStateProviderHelper.CreateReminderCompletedStorageKey(actorId, reminder.Name))
      };
      return _actorStateProviderHelper.ExecuteWithRetriesAsync((Func<Task>) (() =>
      {
        cancellationToken.ThrowIfCancellationRequested();
        return ReplicateStateChangesAsync((IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>) actorStateDataWrapperList);
      }), cancellationToken);
    }

    Task IActorStateProvider.DeleteReminderAsync(ActorId actorId, string reminderName, CancellationToken cancellationToken)
    {
      List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> actorStateDataWrapperList = new List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>()
      {
        VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForDelete(ActorStateType.Reminder, CreateReminderStorageKey(actorId, reminderName)),
        VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForDelete(ActorStateType.Actor, ActorStateProviderHelper.CreateReminderCompletedStorageKey(actorId, reminderName))
      };
      return _actorStateProviderHelper.ExecuteWithRetriesAsync((Func<Task>) (() =>
      {
        cancellationToken.ThrowIfCancellationRequested();
        return ReplicateStateChangesAsync((IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>) actorStateDataWrapperList);
      }), cancellationToken);
    }

    Task<IActorReminderCollection> IActorStateProvider.LoadRemindersAsync(CancellationToken cancellationToken)
    {
      ActorReminderCollection reminderCollection = new ActorReminderCollection();
      foreach (KeyValuePair<string, ActorStateData> actorState in (IEnumerable<KeyValuePair<string, ActorStateData>>) _stateTable.GetActorStateDictionary(ActorStateType.Reminder))
      {
        ActorReminderData actorReminderData = actorState.Value.ActorReminderData;
        if (actorReminderData != null)
        {
          string completedStorageKey = ActorStateProviderHelper.CreateReminderCompletedStorageKey(actorReminderData.ActorId, actorReminderData.Name);
          ActorStateData actorStateData = (ActorStateData) null;
          ReminderCompletedData reminderCompletedData = (ReminderCompletedData) null;
          if (_stateTable.TryGetValue(ActorStateType.Actor, completedStorageKey, out actorStateData))
            reminderCompletedData = actorStateData.ReminderLastCompletedData;
          reminderCollection.Add(actorReminderData.ActorId, (IActorReminderState) new ActorReminderState(actorReminderData, _logicalTimeManager.CurrentLogicalTime, reminderCompletedData));
        }
      }
      return Task.FromResult<IActorReminderCollection>((IActorReminderCollection) reminderCollection);
    }

      private Task<PagedResult<ActorId>> GetStoredActorIds<T>(int itemsCount, ContinuationToken continuationToken, Func<IEnumerator<T>> getEnumeratorFunc, Func<T, string> getStorageKeyFunc, CancellationToken cancellationToken)
      {
            long num1 = continuationToken == null ? 0L : long.Parse((string)continuationToken.Marker);
            long num2 = 0;
            List<ActorId> actorIdList = new List<ActorId>();
            PagedResult<ActorId> result = new PagedResult<ActorId>();
            IEnumerator<T> enumerator = getEnumeratorFunc();
            bool flag = enumerator.MoveNext();
          if (!flag)
          {
              return Task.FromResult(result);
          }

            while (num2 < num1)
            {
                cancellationToken.ThrowIfCancellationRequested();
                flag = enumerator.MoveNext();
                ++num2;
                if (!flag)
                {
                    return Task.FromResult(result);
                }
            }

            while (flag)
            {
                cancellationToken.ThrowIfCancellationRequested();
                string presenceStorageKey1 = getStorageKeyFunc(enumerator.Current);
                ActorId presenceStorageKey2 = ActorStateProviderHelper.GetActorIdFromPresenceStorageKey(presenceStorageKey1);
                if (presenceStorageKey2 != (ActorId)null)
                    actorIdList.Add(presenceStorageKey2);
                else
                    ActorTrace._source.WriteWarningWithId(this._owner.TraceType, this._owner.TraceId, string.Format("Failed to parse ActorId from storage key: {0}", (object)presenceStorageKey1), new object[0]);
                flag = enumerator.MoveNext();
                ++num2;
                if (actorIdList.Count == itemsCount)
                {
                    result.Items = (IEnumerable<ActorId>)actorIdList.AsReadOnly();
                    if (flag)
                        result.ContinuationToken = new ContinuationToken((object)num2.ToString());
                    return Task.FromResult<PagedResult<ActorId>>(result);
                }
            }
            result.Items = (IEnumerable<ActorId>)actorIdList.AsReadOnly();
            return Task.FromResult<PagedResult<ActorId>>(result);
        }


      void IStateProviderReplica.Initialize(StatefulServiceInitializationParameters initializationParameters)
      {
          _initParams = initializationParameters;
          _traceType = "VolatileActorStateProvider";
          _traceId = ActorTrace.GetTraceIdForReplica(initializationParameters.PartitionId, initializationParameters.ReplicaId);
      }

      Task<IReplicator> IStateProviderReplica.OpenAsync(ReplicaOpenMode openMode, IStatefulServicePartition partition, CancellationToken cancellationToken)
    {
      _fabricReplicator = partition.CreateReplicator((IStateProvider) this, GetReplicatorSettings());
      _stateReplicator = _fabricReplicator.StateReplicator2;
      _partition = partition;
      _secondaryPump = new SecondaryPump(_partition, _stateTable, (IStateReplicator) _stateReplicator, _copyOrReplicationOperationSerializer, _logicalTimeManager, _traceId);
      return Task.FromResult<IReplicator>((IReplicator) _fabricReplicator);
    }

    async Task IStateProviderReplica.ChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
    {
      switch (newRole)
      {
        case ReplicaRole.Primary:
          _logicalTimeManager.Start();
          await _secondaryPump.WaitForPumpCompletionAsync();
          break;
        case ReplicaRole.IdleSecondary:
          _logicalTimeManager.Stop();
          _secondaryPump.StartCopyAndReplicationPump();
          break;
        case ReplicaRole.ActiveSecondary:
          _logicalTimeManager.Stop();
          ActorStateData data;
          if (_stateTable.TryGetValue(ActorStateType.LogicalTimestamp, "LogicalTimestamp", out data) && data.LogicalTimestamp.HasValue)
            _logicalTimeManager.CurrentLogicalTime = data.LogicalTimestamp.Value;
          if (_replicaRole == ReplicaRole.Primary)
          {
            _secondaryPump.StartReplicationPump();
            break;
          }
          break;
      }
      _replicaRole = newRole;
    }

    Task IStateProviderReplica.CloseAsync(CancellationToken cancellationToken)
    {
      return _secondaryPump.WaitForPumpCompletionAsync();
    }

    void IStateProviderReplica.Abort()
    {
    }

    Task IStateProviderReplica.BackupAsync(Func<BackupInfo, CancellationToken, Task<bool>> backupCallback)
    {
      throw new NotImplementedException(string.Format((IFormatProvider) CultureInfo.CurrentCulture, Actors.SR.ErrorMethodNotSupported, new object[2]
      {
        (object) "Backup",
        (object) GetType()
      }));
    }

    Task IStateProviderReplica.BackupAsync(BackupOption option, TimeSpan timeout, CancellationToken cancellationToken, Func<BackupInfo, CancellationToken, Task<bool>> backupCallback)
    {
      throw new NotImplementedException(string.Format((IFormatProvider) CultureInfo.CurrentCulture, Actors.SR.ErrorMethodNotSupported, new object[2]
      {
        (object) "Backup",
        (object) GetType()
      }));
    }

    Task IStateProviderReplica.RestoreAsync(string backupFolderPath)
    {
      throw new NotImplementedException(string.Format((IFormatProvider) CultureInfo.CurrentCulture, Actors.SR.ErrorMethodNotSupported, new object[2]
      {
        (object) "Restore",
        (object) GetType()
      }));
    }

    Task IStateProviderReplica.RestoreAsync(string backupFolderPath, RestorePolicy restorePolicy, CancellationToken cancellationToken)
    {
      throw new NotImplementedException(string.Format((IFormatProvider) CultureInfo.CurrentCulture, Actors.SR.ErrorMethodNotSupported, new object[2]
      {
        (object) "Restore",
        (object) GetType()
      }));
    }

    IOperationDataStream IStateProvider.GetCopyContext()
    {
      return (IOperationDataStream) null;
    }

    IOperationDataStream IStateProvider.GetCopyState(long upToSequenceNumber, IOperationDataStream copyContext)
    {
      lock (_replicationLock)
      {
        long local0 = _stateTable.GetHighestKnownSequenceNumber();
        if (local0 < upToSequenceNumber)
        {
          InvalidOperationException local1 = new InvalidOperationException(string.Format((IFormatProvider) CultureInfo.InvariantCulture, Actors.SR.ErrorHighestSequenceNumberLessThanUpToSequenceNumber, new object[2]
          {
            (object) local0,
            (object) upToSequenceNumber
          }));
          ActorTrace._source.WriteErrorWithId("VolatileActorStateProvider", _traceId, "IStateProvider.GetCopyState failed with unexpected error: {0}", new object[1]
          {
            (object) local1.ToString()
          });
          throw local1;
        }
      }
      ReplicatorSettings replicatorSettings = _stateReplicator.GetReplicatorSettings();
      if (!replicatorSettings.MaxReplicationMessageSize.HasValue)
      {
        InvalidOperationException operationException = new InvalidOperationException(string.Format((IFormatProvider) CultureInfo.CurrentCulture, Actors.SR.ErrorReplicatorSettings, new object[1]
        {
          (object) "MaxReplicationMessageSize"
        }));
        ActorTrace._source.WriteErrorWithId("VolatileActorStateProvider", _traceId, "IStateProvider.GetCopyState failed with unexpected error: {0}", new object[1]
        {
          (object) operationException.ToString()
        });
        throw operationException;
      }
      return (IOperationDataStream) new CopyStateEnumerator(_stateTable.GetShallowCopiesEnumerator(upToSequenceNumber), _copyOrReplicationOperationSerializer, upToSequenceNumber, replicatorSettings.MaxReplicationMessageSize.Value / 2L);
    }

    long IStateProvider.GetLastCommittedSequenceNumber()
    {
      return _stateTable.GetHighestCommittedSequenceNumber();
    }

    Task<bool> IStateProvider.OnDataLossAsync(CancellationToken cancellationToken)
    {
      return Task.FromResult<bool>(false);
    }

    Task IStateProvider.UpdateEpochAsync(Epoch epoch, long previousEpochLastSequenceNumber, CancellationToken cancellationToken)
    {
      return (Task) Task.FromResult<bool>(true);
    }

    async Task VolatileLogicalTimeManager.ISnapshotHandler.OnSnapshotAsync(TimeSpan currentLogicalTime)
    {
      try
      {
        await ReplicateUpdateAsync(ActorStateType.LogicalTimestamp, "LogicalTimestamp", new ActorStateData(currentLogicalTime));
      }
      catch (Exception ex)
      {
      }
    }

    internal static DataContractSerializer CreateCopyOrReplicationOperationSerializer()
    {
      return new DataContractSerializer(typeof (CopyOrReplicationOperation));
    }

    private ReplicatorSettings GetReplicatorSettings()
    {
      if (_userDefinedReplicatorSettings != null)
        return _userDefinedReplicatorSettings;
      _initParams.CodePackageActivationContext.ConfigurationPackageModifiedEvent += new EventHandler<PackageModifiedEventArgs<ConfigurationPackage>>(OnConfigurationPackageModified);
      return LoadReplicatorSettings();
    }

    private ReplicatorSettings LoadReplicatorSettings()
    {
      return ActorStateProviderHelper.GetActorReplicatorSettings(_initParams.CodePackageActivationContext, _actorTypeInformation.ImplementationType);
    }

    private void OnConfigurationPackageModified(object sender, PackageModifiedEventArgs<ConfigurationPackage> e)
    {
      try
      {
        _stateReplicator.UpdateReplicatorSettings(LoadReplicatorSettings());
      }
      catch (FabricElementNotFoundException ex)
      {
        ActorTrace._source.WriteErrorWithId("VolatileActorStateProvider", _traceId, "FabricElementNotFoundException while loading replicator settings from configuration.", new object[1]{ (object) ex });
        _partition.ReportFault(FaultType.Transient);
      }
      catch (FabricException ex)
      {
        ActorTrace._source.WriteErrorWithId("VolatileActorStateProvider", _traceId, "FabricException while loading replicator security settings from configuration.", new object[1]{ (object) ex });
        _partition.ReportFault(FaultType.Transient);
      }
      catch (ArgumentException ex)
      {
        ActorTrace._source.WriteWarningWithId("VolatileActorStateProvider", _traceId, "ArgumentException while updating replicator settings from configuration.", new object[1]{ (object) ex });
        _partition.ReportFault(FaultType.Transient);
      }
    }

      private Task ReplicateUpdateAsync(ActorStateType type, string key, ActorStateData data)
      {
          return ReplicateStateChangesAsync(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper.CreateForUpdate(type, key, data));
      }

      private Task ReplicateStateChangesAsync(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper actorStateDataWrapper)
      {
          return ReplicateStateChangesAsync(new[] { actorStateDataWrapper });
      }

        /*
        1. VolatileActorStateProvider.ActorActivatedAsync => VolatileActorStateProvider.ReplicateUpdateAsync => 
        VolatileActorStateProvider.ReplicateStateChangesAsync => VolatileActorStateTable.CommitUpdateAsync


        */
        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
    private async Task ReplicateStateChangesAsync(IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> actorStateDataWrapperList)
    {
      Exception replicationException = null;
      OperationData operationData = SerializeToOperationData(_copyOrReplicationOperationSerializer, new CopyOrReplicationOperation(actorStateDataWrapperList));
      Task replicationTask;
      long sequenceNumber;

      lock (_replicationLock)
      {
        replicationTask = _stateReplicator.ReplicateAsync(operationData, CancellationToken.None, out sequenceNumber);
        _stateTable.PrepareUpdate(actorStateDataWrapperList, sequenceNumber);
      }

      try
      {
        await replicationTask;
      }
      catch (Exception ex)
      {
        replicationException = ex;
      }

      await _stateTable.CommitUpdateAsync(sequenceNumber, replicationException);
    }

    internal static OperationData SerializeToOperationData(DataContractSerializer serializer, CopyOrReplicationOperation copyOrReplicationOperation)
    {
      using (MemoryStream memoryStream = new MemoryStream())
      {
        XmlDictionaryWriter binaryWriter = XmlDictionaryWriter.CreateBinaryWriter((Stream) memoryStream);
        serializer.WriteObject(binaryWriter, (object) copyOrReplicationOperation);
        binaryWriter.Flush();
        return new OperationData(memoryStream.ToArray());
      }
    }

    private static string CreateReminderStorageKey(ActorId actorId, string reminderName)
    {
      return string.Format((IFormatProvider) CultureInfo.InvariantCulture, "{0}_{1}", new object[2]
      {
        (object) actorId.GetStorageKey(),
        (object) reminderName
      });
    }

    private static string CreateReminderStorageKeyPrefix(ActorId actorId, string reminderNamePrefix)
    {
      return CreateReminderStorageKey(actorId, reminderNamePrefix);
    }

    private static string CreateActorStorageKey(ActorId actorId, string stateName)
    {
      if (string.IsNullOrEmpty(stateName))
        return actorId.GetStorageKey();
      return string.Format((IFormatProvider) CultureInfo.InvariantCulture, "{0}_{1}", new object[2]
      {
        (object) actorId.GetStorageKey(),
        (object) stateName
      });
    }

    private static string CreateActorStorageKeyPrefix(ActorId actorId, string stateNamePrefix)
    {
      return CreateActorStorageKey(actorId, stateNamePrefix);
    }

    private static string ExtractStateName(ActorId actorId, string storageKey)
    {
      string storageKeyPrefix = CreateActorStorageKeyPrefix(actorId, string.Empty);
      if (storageKey == storageKeyPrefix)
        return string.Empty;
      return storageKey.Substring(storageKeyPrefix.Length + 1);
    }

      private Task ExecuteWithRetriesAsync(Func<Task> func, CancellationToken userCancellationToken)
      {
          return ExecuteWithRetriesAsync<object>(async () =>
          {
              await func();
              return null;
          }, userCancellationToken);
      }

      private async Task<TResult> ExecuteWithRetriesAsync<TResult>(Func<Task<TResult>> func, CancellationToken userCancellationToken)
        {
            TResult result;
            while (true)
            {
                try
                {
                    result = await func();
                    break;
                }
                catch (FabricTransientException)
                {
                }
                catch (FabricNotPrimaryException)
                {
                    if (_replicaRole != ReplicaRole.Primary)
                    {
                        throw;
                    }
                }
                catch (OperationCanceledException)
                {
                    if (userCancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }

                    if (_replicaRole != ReplicaRole.Primary)
                    {
                        throw new FabricNotPrimaryException();
                    }
                }

                await Task.Delay(_transientErrorRetryDelay, userCancellationToken);
            }
            return result;
        }

        internal enum ActorStateType
    {
      LogicalTimestamp,
      Actor,
      Reminder,
    }

    [DataContract]
    internal class CopyOrReplicationOperation
    {
      [DataMember]
      private readonly IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> _actorStateDataWrapperList;

      public IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> ActorStateDataWrapperList
      {
        get
        {
          return _actorStateDataWrapperList;
        }
      }

      public CopyOrReplicationOperation(IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> dataWrapperList)
      {
        _actorStateDataWrapperList = dataWrapperList;
      }
    }

    [DataContract]
    internal sealed class ActorStateData
    {
      [DataMember]
      public TimeSpan? LogicalTimestamp { get; private set; }

      [DataMember]
      public byte[] ActorState { get; private set; }

      [DataMember]
      public ActorReminderData ActorReminderData { get; private set; }

      [DataMember]
      public ReminderCompletedData ReminderLastCompletedData { get; private set; }

      public ActorStateData(TimeSpan logicalTimestamp)
      {
        LogicalTimestamp = new TimeSpan?(logicalTimestamp);
      }

      public ActorStateData(byte[] state)
      {
        ActorState = state;
      }

      public ActorStateData(ActorReminderData reminderData)
      {
        ActorReminderData = reminderData;
      }

      public ActorStateData(ReminderCompletedData reminderCompletedData)
      {
        ReminderLastCompletedData = reminderCompletedData;
      }

      public long EstimateDataLength()
      {
        return (long) ((!LogicalTimestamp.HasValue ? 0 : 8) + (ActorState == null ? 0 : ActorState.Length)) + (ActorReminderData == null ? 0L : ActorReminderData.EstimateDataLength()) + (ReminderLastCompletedData == null ? 0L : ReminderLastCompletedData.EstimateDataLength());
      }
    }

    internal sealed class CopyStateEnumerator : IOperationDataStream
    {
      private readonly DataContractSerializer _copyOperationSerializer;
      private readonly long _maxSequenceNumber;
      private readonly long _maxDataLength;
      /// <summary>
      /// This LinkedList represents actor state data grouped by sequence number in increasing
      /// order of sequence number. Each entry in the LinkedList contains sequence number and
      /// all the ActorStateDataWrapper entries which belong to that sequence number.
      /// This grouping is required to maintain replication boundary during copy operation
      /// to build a replica.
      /// </summary>
      private readonly LinkedList<CopyStateData> _copyStateList;

      public CopyStateEnumerator(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateEnumerator actorStateDataEnumerator, DataContractSerializer copyOperationSerializer, long maxSequenceNumber, long maxDataLength)
      {
        _copyOperationSerializer = copyOperationSerializer;
        _maxSequenceNumber = maxSequenceNumber;
        _maxDataLength = maxDataLength;
        _copyStateList = new LinkedList<CopyStateData>();
        GroupActorStateDataBySequenceNumber(actorStateDataEnumerator);
      }

      Task<OperationData> IOperationDataStream.GetNextAsync(CancellationToken cancellationToken)
      {
        if (_copyStateList.Count == 0)
          return Task.FromResult<OperationData>((OperationData) null);
        List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> stateDataWrapperList = new List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>();
        long num1 = 0;
        long num2 = _copyStateList.First.Value.GetEstimatedDataLength();
        do
        {
          CopyStateData copyStateData = _copyStateList.First.Value;
          _copyStateList.RemoveFirst();
          if (copyStateData.SequenceNumber <= _maxSequenceNumber)
          {
            stateDataWrapperList.AddRange((IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>) copyStateData.ActorStateDataWrapperList);
            num1 += num2;
          }
          num2 = 0L;
          if (_copyStateList.Count != 0 && _copyStateList.First.Value.SequenceNumber <= _maxSequenceNumber)
            num2 = _copyStateList.First.Value.GetEstimatedDataLength();
        }
        while (num2 > 0L && num1 + num2 <= _maxDataLength);
        return Task.FromResult<OperationData>(SerializeToOperationData(_copyOperationSerializer, new CopyOrReplicationOperation((IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>) stateDataWrapperList)));
      }

      private void GroupActorStateDataBySequenceNumber(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateEnumerator actorStateEnumerator)
      {
        while (actorStateEnumerator.PeekNext() != null)
        {
          CopyStateData copyStateData = new CopyStateData(actorStateEnumerator.PeekNext().SequenceNumber);
          VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper stateDataWrapper;
          do
          {
            copyStateData.ActorStateDataWrapperList.Add(actorStateEnumerator.GetNext());
            stateDataWrapper = actorStateEnumerator.PeekNext();
          }
          while (stateDataWrapper != null && stateDataWrapper.SequenceNumber == copyStateData.SequenceNumber);
          _copyStateList.AddLast(copyStateData);
        }
      }

      private sealed class CopyStateData
      {
        private readonly long _sequenceNumber;
        private readonly List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> _actorStateDataWrapperList;

        public long SequenceNumber
        {
          get
          {
            return _sequenceNumber;
          }
        }

        public List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> ActorStateDataWrapperList
        {
          get
          {
            return _actorStateDataWrapperList;
          }
        }

        public CopyStateData(long sequenceNumber)
        {
          _sequenceNumber = sequenceNumber;
          _actorStateDataWrapperList = new List<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper>();
        }

        public long GetEstimatedDataLength()
        {
          long num = 0;
          foreach (VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper stateDataWrapper in ActorStateDataWrapperList)
            num += (long) (4 + stateDataWrapper.Key.Length * 2) + (stateDataWrapper.IsDelete ? 0L : stateDataWrapper.Value.EstimateDataLength()) + 8L;
          return num;
        }
      }
    }

    internal sealed class SecondaryPump
    {
      private readonly IStatefulServicePartition _partition;
      private readonly string _traceId;
      private readonly VolatileActorStateTable<ActorStateType, string, ActorStateData> _stateTable;
      private readonly IStateReplicator _stateReplicator;
      private readonly DataContractSerializer _copyOrReplicationOperationSerializer;
      private readonly VolatileLogicalTimeManager _logicalTimeManager;
      private Task _pumpTask;

      public SecondaryPump(IStatefulServicePartition partition, VolatileActorStateTable<ActorStateType, string, ActorStateData> stateTable, IStateReplicator stateReplicator, DataContractSerializer copyOrReplicationOperationSerializer, VolatileLogicalTimeManager logicalTimeManager, string traceId)
      {
        _partition = partition;
        _stateTable = stateTable;
        _stateReplicator = stateReplicator;
        _copyOrReplicationOperationSerializer = copyOrReplicationOperationSerializer;
        _logicalTimeManager = logicalTimeManager;
        _pumpTask = (Task) null;
        _traceId = traceId;
      }

      public void StartCopyAndReplicationPump()
      {
        _pumpTask = PumpLoop(true);
      }

      public void StartReplicationPump()
      {
        _pumpTask = PumpLoop(false);
      }

      public async Task WaitForPumpCompletionAsync()
      {
        if (_pumpTask == null)
          return;
        await _pumpTask;
      }

      private async Task PumpLoop(bool isCopy)
      {
        try
        {
          ActorTrace._source.WriteInfoWithId("VolatileActorStateProvider", _traceId, "Starting PumpLoop. isCopy: {0}", new object[1]
          {
            (object) isCopy
          });
          IOperationStream operationStream = GetOperationStream(isCopy);
          bool donePumping = false;
          IOperation operation;
          do
          {
            operation = await operationStream.GetOperationAsync(CancellationToken.None);
            if (operation != null)
            {
              using (MemoryStream memoryStream = new MemoryStream(operation.Data[0].Array))
                DeserializeAndApply(XmlDictionaryReader.CreateBinaryReader((Stream) memoryStream, XmlDictionaryReaderQuotas.Max), operation, isCopy);
              operation.Acknowledge();
            }
            else
            {
              ActorTrace._source.WriteInfoWithId("VolatileActorStateProvider", _traceId, "PumpLoop (isCopy: {0}) processed operation NULL.", new object[1]
              {
                (object) isCopy
              });
              if (isCopy)
              {
                operationStream = GetOperationStream(false);
                isCopy = false;
              }
              else
                donePumping = true;
            }
          }
          while (operation != null || !donePumping);
        }
        catch (Exception ex)
        {
          ActorTrace._source.WriteErrorWithId("VolatileActorStateProvider", _traceId, "PumpLoop failed: {0}", new object[1]
          {
            (object) ex.ToString()
          });
          _partition.ReportFault(FaultType.Transient);
        }
      }

      private IOperationStream GetOperationStream(bool isCopy)
      {
        if (!isCopy)
          return _stateReplicator.GetReplicationStream();
        return _stateReplicator.GetCopyStream();
      }

      private void DeserializeAndApply(XmlDictionaryReader binaryReader, IOperation operation, bool isCopy)
      {
        object obj = _copyOrReplicationOperationSerializer.ReadObject(binaryReader);
        CopyOrReplicationOperation replicationOperation = obj as CopyOrReplicationOperation;
        if (replicationOperation != null)
        {
          IEnumerable<VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper> stateDataWrapperList = replicationOperation.ActorStateDataWrapperList;
          if (!isCopy)
          {
            foreach (VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper dataWrapper in stateDataWrapperList)
            {
              dataWrapper.UpdateSequenceNumber(operation.SequenceNumber);
              UpdateLogicalTimestamp(dataWrapper);
            }
          }
          _stateTable.ApplyUpdates(stateDataWrapperList);
        }
        else
          throw new InvalidOperationException(string.Format((IFormatProvider) CultureInfo.InvariantCulture, Actors.SR.ErrorCasting, new object[2]
          {
            (object) obj.GetType(),
            (object) typeof (CopyOrReplicationOperation)
          }));
      }

      private void UpdateLogicalTimestamp(VolatileActorStateTable<ActorStateType, string, ActorStateData>.ActorStateDataWrapper dataWrapper)
      {
        if (dataWrapper.IsDelete || !dataWrapper.Value.LogicalTimestamp.HasValue)
          return;
        _logicalTimeManager.CurrentLogicalTime = dataWrapper.Value.LogicalTimestamp.Value;
      }
    }
  }
}
