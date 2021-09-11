//---------------------------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//---------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Azure.AISC.Scheduler.Regional.Capacity
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.AISC.Common;
    using Microsoft.Azure.AISC.Common.Retry;
    using Microsoft.Azure.AISC.Common.Utils;
    using Microsoft.Azure.AISC.Common.Utils.Async;
    using Microsoft.Azure.AISC.Common.Utils.Configuration;
    using Microsoft.Azure.AISC.Common.Utils.Retry;
    using Microsoft.Azure.AISC.Diagnostics.Abstractions;
    using Microsoft.Azure.AISC.Scheduler.Common;
    using Microsoft.Azure.AISC.Scheduler.Common.Capacity;
    using Microsoft.Azure.AISC.Scheduler.Common.Telemetry;
    using Microsoft.Azure.AISC.Scheduler.Global.Model;
    using Microsoft.Azure.AISC.StoreProvider;
    using Microsoft.Azure.AISC.StoreProvider.Table;
    using Microsoft.Extensions.Logging;

    internal sealed class FederationCapacityAllocator : IFederationCapacityAllocator
    {
        private readonly IEntityStoreProvider storeProvider;
        private readonly InstanceTypeCache instanceTypeCache;
        private readonly IConfigurationProvider configurationProvider;
        private readonly IMetricsEmitter metricsEmitter;
        private readonly ILogger logger;
        private readonly int backoffDuration;
        private readonly int maxRetryCount;
        private string currentServiceLocation;

        public FederationCapacityAllocator(
            IEntityStoreProvider storeProvider,
            IConfigurationProvider configProvider,
            InstanceTypeCache instanceTypeCache,
            IMetricsEmitter metricsEmitter,
            ILogger logger)
        {
            storeProvider.MustNotBeNull(nameof(storeProvider));
            instanceTypeCache.MustNotBeNull(nameof(instanceTypeCache));
            configProvider.MustNotBeNull(nameof(configProvider));
            metricsEmitter.MustNotBeNull(nameof(metricsEmitter));
            logger.MustNotBeNull(nameof(logger));

            this.storeProvider = storeProvider;
            this.instanceTypeCache = instanceTypeCache;
            this.configurationProvider = configProvider;
            this.metricsEmitter = metricsEmitter;
            this.logger = logger;
            this.backoffDuration =
                configProvider.GetValue<int>(Constants.ConfigurationKeys.BatchOperationBackoffDurationInMilliSeconds);
            this.maxRetryCount =
                configProvider.GetValue<int>(Constants.ConfigurationKeys.BatchOperationMaxRetryCount);
            this.currentServiceLocation =
                configProvider.GetValue<string>(Constants.ConfigurationKeys.CurrentServiceLocation);
        }

        public async Task<CapacityReservationResult> ReserveAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            ulong instanceTypeCount,
            CancellationToken cancellationToken)
        {
            resourceId.MustNotBeNullOrEmpty(nameof(resourceId));
            location.MustNotBeNullOrEmpty(nameof(location));
            instanceTypeName.MustNotBeNullOrEmpty(nameof(instanceTypeName));
            federationId.MustNotBeNullOrEmpty(nameof(federationId));

            return await BackoffRetryUtility<CapacityReservationResult>.ExecuteAsync(
                async () =>
                {
                    return await this.ReserveCapacityAsync(
                        resourceId,
                        location,
                        instanceTypeName,
                        federationId,
                        instanceTypeCount,
                        cancellationToken);
                },
                new AggregateWithPreconditionFailedRandomBackoffRetryPolicy(
                    TimeSpan.FromMilliseconds(this.backoffDuration), this.maxRetryCount),
                this.logger,
                cancellationToken);
        }

        public async Task<bool> AbortReservationAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken)
        {
            return await this.AbortCapacityChangeWithRetryAsync(
                resourceId,
                location,
                instanceTypeName,
                federationId,
                cancellationToken);
        }

        public async Task<bool> CommitReservationAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken)
        {
            return await this.CommitCapacityChangeAsync(
                resourceId,
                location,
                instanceTypeName,
                federationId,
                cancellationToken);
        }

        public async Task<bool> ReleaseAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            ulong instanceTypeCount,
            CancellationToken cancellationToken)
        {
            resourceId.MustNotBeNullOrEmpty(nameof(resourceId));
            location.MustNotBeNullOrEmpty(nameof(location));
            instanceTypeName.MustNotBeNullOrEmpty(nameof(instanceTypeName));
            federationId.MustNotBeNullOrEmpty(nameof(federationId));

            return await BackoffRetryUtility<bool>.ExecuteAsync(
                async () =>
                {
                    return await this.ReleaseCapacityAsync(
                        resourceId,
                        location,
                        instanceTypeName,
                        federationId,
                        instanceTypeCount,
                        cancellationToken);
                },
                new AggregateWithPreconditionFailedRandomBackoffRetryPolicy(
                    TimeSpan.FromMilliseconds(this.backoffDuration), this.maxRetryCount),
                this.logger,
                cancellationToken);
        }

        public async Task<bool> CommitReleaseAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken)
        {
            return await this.CommitCapacityChangeAsync(
                resourceId,
                location,
                instanceTypeName,
                federationId,
                cancellationToken);
        }

        public async Task<bool> AbortReleaseAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken)
        {
            return await this.AbortCapacityChangeWithRetryAsync(
                resourceId,
                location,
                instanceTypeName,
                federationId,
                cancellationToken);
        }

        private async Task<CapacityReservationResult> ReserveCapacityAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            ulong instanceTypeCount,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            InstanceType instanceType = await this.instanceTypeCache.GetAsync(instanceTypeName, cancellationToken);

            //convert from instance type count to capacity count
            ulong capacityToReserve = instanceTypeCount * instanceType.CapacityCount;
            long capacityChange = (long)capacityToReserve;

            if (await this.CheckIfCapacityChangeAlreadyExistAsync(
                resourceId,
                location,
                instanceTypeName,
                federationId,
                capacityChange,
                cancellationToken))
            {
                string statusMessage = RegionalSchedulerResources.CapacityReservationIsApproved(
                    resourceId,
                    capacityToReserve,
                    instanceType.CapacityId,
                    federationId,
                    location);

                this.logger.LogInformation(statusMessage);

                return new CapacityReservationResult(CapacityReservationStatus.Approved, statusMessage);
            }

            FederationCapacity federationCapacity =
                await this.storeProvider.TryReadEntityAsync<FederationCapacity>(
                    FederationCapacity.FormatPartitionKey(location, instanceType.CapacityId),
                    federationId.ToLowerInvariant(),
                    EntityLocationRequestProperties.FromLocation(location),
                    cancellationToken);

            if (federationCapacity == null)
            {
                this.logger.LogInformation(
                    $"Requested instance type capacity with capacity id: {instanceType.CapacityId} " +
                    $"does not exist in the location: {location} in federation: {federationId}.");

                string statusMessage = RegionalSchedulerResources.CapacityReservationIsNotApprovedUnAvailable(
                    resourceId,
                    capacityToReserve,
                    instanceType.CapacityId,
                    federationId,
                    location);

                return new CapacityReservationResult(CapacityReservationStatus.NotApproved, statusMessage);
            }

            if (!federationCapacity.IsAvailableForAllocation)
            {
                string statusMessage = RegionalSchedulerResources.CapacityReservationIsNotApprovedUnAvailable(
                    resourceId,
                    capacityToReserve,
                    instanceType.CapacityId,
                    federationId,
                    location);

                this.logger.LogInformation($"{statusMessage}, Current federation capacity is: {federationCapacity}");

                return new CapacityReservationResult(CapacityReservationStatus.NotApproved, statusMessage);
            }

            if (capacityToReserve <= this.GetAvailableCapacity(federationCapacity))
            {
                federationCapacity.UsedCount += capacityToReserve;

                CapacityChange capacityReservation = new CapacityChange(location)
                {
                    ResourceId = resourceId,
                    CapacityId = instanceType.CapacityId,
                    FederationId = federationId,
                    Capacity = capacityChange,
                };

                await CapacityBatchOperationDataProvider.ApplyCapacityReservationAsync(
                    this.storeProvider,
                    capacityReservation,
                    federationCapacity);

                string statusMessage = RegionalSchedulerResources.CapacityReservationIsApproved(
                    resourceId,
                    capacityToReserve,
                    instanceType.CapacityId,
                    federationId,
                    location);

                this.logger.LogInformation(
                    $"{statusMessage}, Federation capacity after approval is: {federationCapacity}");

                await this.WriteCapacityMetricsAsync(federationCapacity, cancellationToken);

                return new CapacityReservationResult(CapacityReservationStatus.Approved, statusMessage);
            }
            else if (
                (capacityToReserve + federationCapacity.BufferCount) > federationCapacity.TotalCount)
            {
                string statusMessage = RegionalSchedulerResources.CapacityReservationIsNotApproved(
                    resourceId,
                    capacityToReserve,
                    instanceType.CapacityId,
                    federationId,
                    location,
                    federationCapacity.TotalCount);

                this.logger.LogInformation($"{statusMessage}, Current federation capacity is: {federationCapacity}");

                return new CapacityReservationResult(CapacityReservationStatus.NotApproved, statusMessage);
            }
            else
            {
                string statusMessage = RegionalSchedulerResources.CapacityReservationIsWaiting(
                    resourceId,
                    capacityToReserve,
                    instanceType.CapacityId,
                    federationId,
                    location,
                    this.GetAvailableCapacity(federationCapacity));

                this.logger.LogInformation($"{statusMessage}, Current federation capacity is: {federationCapacity}");

                return new CapacityReservationResult(CapacityReservationStatus.Waiting, statusMessage);
            }
        }

        private async Task<bool> CommitCapacityChangeAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken)
        {
            resourceId.MustNotBeNullOrEmpty(nameof(resourceId));
            location.MustNotBeNullOrEmpty(nameof(location));
            instanceTypeName.MustNotBeNullOrEmpty(nameof(instanceTypeName));
            federationId.MustNotBeNullOrEmpty(nameof(federationId));
            cancellationToken.ThrowIfCancellationRequested();

            InstanceType instanceType = await this.instanceTypeCache.GetAsync(instanceTypeName, cancellationToken);

            CapacityChange capacityChange = await this.storeProvider.TryReadEntityAsync<CapacityChange>(
                CapacityChange.FormatPartitionKey(location, instanceType.CapacityId),
                CapacityChange.FormatKey(resourceId),
                EntityLocationRequestProperties.FromLocation(location),
                cancellationToken);

            if (capacityChange != null)
            {
                if (capacityChange.FederationId == federationId)
                {
                    await this.storeProvider.DeleteEntityAsync(capacityChange, cancellationToken);

                    this.logger.LogInformation($"Commit capacity change {capacityChange}");

                    return true;
                }

                throw new InvalidOperationException(
                    $"Requested federationId: {federationId} to commit capacity change" +
                    $" does not match with the existing record: {capacityChange}");
            }

            this.logger.LogInformation($"Could not find capacity change to commit for " +
                $"{resourceId} at {location} with {instanceTypeName} in {federationId}");

            return false;
        }

        private async Task<bool> AbortCapacityChangeWithRetryAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken)
        {
            resourceId.MustNotBeNullOrEmpty(nameof(resourceId));
            location.MustNotBeNullOrEmpty(nameof(location));
            instanceTypeName.MustNotBeNullOrEmpty(nameof(instanceTypeName));
            federationId.MustNotBeNullOrEmpty(nameof(federationId));
            cancellationToken.ThrowIfCancellationRequested();

            return await BackoffRetryUtility<bool>.ExecuteAsync(
                async () =>
                {
                    return await this.AbortCapacityChangeAsync(
                        resourceId,
                        location,
                        instanceTypeName,
                        federationId,
                        cancellationToken);
                },
                new AggregateWithPreconditionFailedRandomBackoffRetryPolicy(
                    TimeSpan.FromMilliseconds(this.backoffDuration), this.maxRetryCount),
                this.logger,
                cancellationToken);
        }

        private async Task<bool> AbortCapacityChangeAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            InstanceType instanceType = await this.instanceTypeCache.GetAsync(instanceTypeName, cancellationToken);

            CapacityChange capacityChange = await this.storeProvider.TryReadEntityAsync<CapacityChange>(
                CapacityChange.FormatPartitionKey(location, instanceType.CapacityId),
                CapacityChange.FormatKey(resourceId),
                EntityLocationRequestProperties.FromLocation(location),
                cancellationToken);

            if (capacityChange != null)
            {
                if (capacityChange.FederationId == federationId)
                {
                    FederationCapacity federationCapacity =
                        await this.storeProvider.TryReadEntityAsync<FederationCapacity>(
                            FederationCapacity.FormatPartitionKey(location, instanceType.CapacityId),
                            FederationCapacity.FormatRowKey(federationId),
                            EntityLocationRequestProperties.FromLocation(location),
                            cancellationToken);

                    if (federationCapacity == null)
                    {
                        throw new InvalidOperationException(
                            $"Requested instance type series capacity with capacity id: {instanceType.CapacityId} " +
                            $"does not exist at location: {location} to abort the capacity change: {capacityChange}");
                    }

                    if (capacityChange.Capacity >= 0)
                    {
                        federationCapacity.UsedCount -= (ulong)capacityChange.Capacity;
                    }
                    else
                    {
                        federationCapacity.UsedCount += (ulong)(-capacityChange.Capacity);
                    }

                    this.logger.LogInformation($"Abort capacity change {capacityChange}, " +
                        $"federation capacity is {federationCapacity}");

                    await CapacityBatchOperationDataProvider.RemoveCapacityReservationAsync(
                        this.storeProvider,
                        capacityChange,
                        federationCapacity);

                    await this.WriteCapacityMetricsAsync(federationCapacity, cancellationToken);

                    return true;
                }

                throw new InvalidOperationException(
                    $"Requested federationId: {federationId} to abort capacity change" +
                    $" does not match with the existing capacity change: {capacityChange}");
            }

            this.logger.LogInformation($"Could not find capacity change to abort for " +
                $"{resourceId} at {location} with {instanceTypeName} in {federationId}");

            return false;
        }

        private async Task<bool> CheckIfCapacityChangeAlreadyExistAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            long capacityToChange,
            CancellationToken cancellationToken)
        {
            InstanceType instanceType = await this.instanceTypeCache.GetAsync(instanceTypeName, cancellationToken);

            CapacityChange existingChange = await this.storeProvider.TryReadEntityAsync<CapacityChange>(
                CapacityChange.FormatPartitionKey(location, instanceType.CapacityId),
                CapacityChange.FormatKey(resourceId),
                EntityLocationRequestProperties.FromLocation(location),
                cancellationToken);

            if (existingChange != null)
            {
                if (existingChange.FederationId == federationId &&
                    existingChange.Capacity == (long)capacityToChange)
                {
                    return true;
                }

                throw new InvalidOperationException(
                    $"Requested federationId: {federationId} or capacity to change: {capacityToChange} " +
                    $"do not match with the existing change: {existingChange}");
            }

            return false;
        }

        private async Task<bool> ReleaseCapacityAsync(
            string resourceId,
            string location,
            string instanceTypeName,
            string federationId,
            ulong instanceTypeCount,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            InstanceType instanceType = await this.instanceTypeCache.GetAsync(instanceTypeName, cancellationToken);

            //convert from instance type count to capacity count
            ulong capacityToRelease = instanceTypeCount * instanceType.CapacityCount;
            long capacityChange = -1 * (long)capacityToRelease;

            if (await this.CheckIfCapacityChangeAlreadyExistAsync(
                resourceId,
                location,
                instanceTypeName,
                federationId,
                capacityChange,
                cancellationToken))
            {
                return true;
            }

            FederationCapacity federationCapacity =
                await this.storeProvider.TryReadEntityAsync<FederationCapacity>(
                    FederationCapacity.FormatPartitionKey(location, instanceType.CapacityId),
                    federationId.ToLowerInvariant(),
                    EntityLocationRequestProperties.FromLocation(location),
                    cancellationToken);

            if (federationCapacity == null)
            {
                throw new InvalidOperationException(
                    $"Requested instance type series capacity with capacity id: {instanceType.CapacityId} " +
                    $"does not exist in the location: {location} in federation: {federationId}.");
            }

            if (federationCapacity.UsedCount < capacityToRelease)
            {
                throw new InvalidOperationException(
                    $"Capacity to release:{capacityToRelease} is > used " +
                    $"capacity:{federationCapacity.UsedCount} for federation: {federationId}.");
            }

            federationCapacity.UsedCount -= capacityToRelease;

            CapacityChange capacityRelease = new CapacityChange(location)
            {
                ResourceId = resourceId,
                CapacityId = instanceType.CapacityId,
                FederationId = federationId,
                Capacity = capacityChange, // To release amount is used as a negative value.
            };
            capacityRelease.SetEntityLocation(location);

            this.logger.LogInformation($"Release capacity for {resourceId} for {capacityToRelease} units of " +
                $"{instanceType.CapacityId} in {federationId}, new federation capacity is {federationCapacity}");

            await CapacityBatchOperationDataProvider.ApplyCapacityReservationAsync(
                this.storeProvider,
                capacityRelease,
                federationCapacity);

            await this.WriteCapacityMetricsAsync(federationCapacity, cancellationToken);

            return true;
        }

        private ulong GetAvailableCapacity(FederationCapacity federationCapacity)
        {
            if (federationCapacity.TotalCount < federationCapacity.BufferCount)
            {
                this.logger.LogCritical($"Federation:{federationCapacity.FederationId} has " +
                    $"total capacity:{federationCapacity.TotalCount} < buffer capacity" +
                    $":{federationCapacity.BufferCount}");

                return 0;
            }

            ulong totalCapacity = federationCapacity.TotalCount - federationCapacity.BufferCount;
            if (totalCapacity < federationCapacity.UsedCount)
            {
                this.logger.LogWarning(
                    $"Total capacity is less than the used capacity. This can be due to a capacity adjustment and " +
                    $"it will get corrected as existing jobs complete. " +
                    $"Federation capacity: {federationCapacity} capacity units");

                return 0;
            }

            return totalCapacity - federationCapacity.UsedCount;
        }
    }
}
