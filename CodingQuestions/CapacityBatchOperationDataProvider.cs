//---------------------------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//---------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Azure.AISC.Scheduler.Regional.Capacity
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.AISC.Scheduler.Global.Model;
    using Microsoft.Azure.AISC.StoreProvider.Table;

    internal sealed class CapacityBatchOperationDataProvider
    {
        private static readonly IEnumerable<Type> BatchEntityTypes =
            new Type[] { typeof(FederationCapacity), typeof(CapacityChange) };

        /// <summary>
        /// Creates capacity change and updates the corresponding instance type series capacity atomically.
        /// </summary>
        /// <param name="entityStoreProvider">Entity store provider</param>
        /// <param name="capacityChange">Capacity change to create</param>
        /// <param name="federationCapacity">Federation capacity update</param>
        public static async Task ApplyCapacityReservationAsync(
            IEntityStoreProvider entityStoreProvider,
            CapacityChange capacityChange,
            FederationCapacity federationCapacity)
        {
            IEntityBatchOperation batchOperation =
                entityStoreProvider.CreateBatch(CapacityBatchOperationDataProvider.BatchEntityTypes);
            batchOperation.AddCreateEntityOperation(capacityChange);
            batchOperation.AddUpdateEntityOperation(federationCapacity);

            await entityStoreProvider.ExecuteBatchOperationAsync(batchOperation, CancellationToken.None);
        }

        /// <summary>
        /// Deletes capacity change and updates the corresponding instance type series capacity atomically.
        /// </summary>
        /// <param name="entityStoreProvider">Entity store provider</param>
        /// <param name="capacityChange">Capacity change to delete</param>
        /// <param name="federationCapacity">Federation capacity update</param>
        public static async Task RemoveCapacityReservationAsync(
            IEntityStoreProvider entityStoreProvider,
            CapacityChange capacityChange,
            FederationCapacity federationCapacity)
        {
            IEntityBatchOperation batchOperation =
                entityStoreProvider.CreateBatch(CapacityBatchOperationDataProvider.BatchEntityTypes);
            batchOperation.AddDeleteEntityOperation(capacityChange);
            batchOperation.AddUpdateEntityOperation(federationCapacity);

            await entityStoreProvider.ExecuteBatchOperationAsync(batchOperation, CancellationToken.None);
        }
    }
}
