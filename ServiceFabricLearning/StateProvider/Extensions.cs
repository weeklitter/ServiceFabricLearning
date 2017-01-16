// ***********************************************************************
// Solution         : ServiceFabricLearning
// Project          : StateProvider
// File             : Extensions.cs
// Created          : 2017-01-03  12:33 PM
// ***********************************************************************
// <copyright>
//     Copyright © 2016 Kolibre Credit Team. All rights reserved.
// </copyright>
// ***********************************************************************

using System.Globalization;
using Microsoft.ServiceFabric.Actors;

namespace StateProvider
{
    internal static class Extensions
    {
        internal static string GetStorageKey(this ActorId actorId)
        {
            switch (actorId.Kind)
            {
                case ActorIdKind.Long:
                    return string.Format(CultureInfo.InvariantCulture, "{0}_{1}", actorId.Kind.ToString(), actorId.GetLongId());
                case ActorIdKind.Guid:
                    return string.Format(CultureInfo.InvariantCulture, "{0}_{1}", actorId.Kind.ToString(), actorId.GetGuidId());
                case ActorIdKind.String:
                    return string.Format(CultureInfo.InvariantCulture, "{0}_{1}", actorId.Kind.ToString(), actorId.GetStringId());
                default:
                    return null;
            }
        }
    }
}