// ***********************************************************************
// Solution         : ServiceFabricLearning
// Project          : StateProvider
// File             : ActorStateProviderHelper.cs
// Created          : 2017-01-03  11:05 AM
// ***********************************************************************
// <copyright>
//     Copyright © 2016 Kolibre Credit Team. All rights reserved.
// </copyright>
// ***********************************************************************

using System;
using System.Globalization;
using Microsoft.ServiceFabric.Actors;

namespace StateProvider
{
    internal sealed class ActorStateProviderHelper
    {
        internal static string CreateActorPresenceStorageKey(ActorId actorId)
        {
            if (actorId == null)
            {
                throw new ArgumentNullException(nameof(actorId));
            }

            return string.Format(CultureInfo.InvariantCulture, "{0}_{1}", "@@", actorId.GetStorageKey());
        }

        internal static ActorId GetActorIdFromPresenceStorageKey(string presenceStorageKey)
        {
            return ActorId.TryGetActorIdFromStorageKey(presenceStorageKey.Substring("@@".Length + 1));
        }
    }
}