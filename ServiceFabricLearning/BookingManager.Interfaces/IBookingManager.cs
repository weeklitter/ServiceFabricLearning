// ***********************************************************************
// Solution         : ServiceFabricLearning
// Project          : BookingManager.Interfaces
// File             : IBookingManager.cs
// Created          : 2017-01-16  1:59 PM
// ***********************************************************************
// <copyright>
//     Copyright © 2016 Kolibre Credit Team. All rights reserved.
// </copyright>
// ***********************************************************************

using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;

namespace BookingManager.Interfaces
{
    /// <summary>
    ///     Ticket 的预订管理器。
    /// </summary>
    public interface IBookingManager : IActor
    {
        /// <summary>
        ///     获取可用的剩余票量。
        /// </summary>
        Task<int> GetAvailableAmountAsync(CancellationToken cancellationToken);
    }
}