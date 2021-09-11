//---------------------------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//---------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Azure.AISC.Scheduler.Common.Capacity
{
    using System.Threading;
    using System.Threading.Tasks;

    internal interface IFederationCapacityAllocator
    {
        Task<CapacityReservationResult> ReserveAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            ulong instanceTypeCount,
            CancellationToken cancellationToken);

        Task<bool> CommitReservationAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken);

        Task<bool> AbortReservationAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken);

        Task<bool> ReleaseAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            ulong instanceTypeCount,
            CancellationToken cancellationToken);

        Task<bool> CommitReleaseAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken);

        Task<bool> AbortReleaseAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken);
    }
}
