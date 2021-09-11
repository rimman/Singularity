//---------------------------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//---------------------------------------------------------------------------------------------------------------------
namespace Microsoft.Azure.AISC.Scheduler.Global.Model
{
    using System.Collections.Generic;
    using Microsoft.Azure.AISC.Common.Utils;
    using Microsoft.Azure.AISC.StoreProvider.Table;
    using Newtonsoft.Json;
    using static Microsoft.Azure.AISC.StoreProvider.EntityStoreConstants;

    // Models capacity of a Federation (a.k.a., cluster in a datacenter)
    internal sealed class FederationCapacity : CosmosStoreEntity
    {
        private const string EntityType = "FederationCapacity";

        [JsonConstructor]
        public FederationCapacity()
        {
        }

        public FederationCapacity(string federationLocation, string federationId, string capacityId)
            : base(federationLocation, jsonExtensionBag: null)
        {
            federationLocation.MustNotBeNullOrEmpty(nameof(federationLocation));
            federationId.MustNotBeNullOrEmpty(nameof(federationId));
            capacityId.MustNotBeNullOrEmpty(nameof(capacityId));

            this.FederationLocation = federationLocation;
            this.FederationId = federationId;
            this.CapacityId = capacityId;
        }

        [JsonProperty(CosmosDBStoreConstants.PropertyNames.Id)]
        public override string Id
        {
            get
            {
                return FederationCapacity.FormatRowKey(this.FederationId);
            }

            set
            {
            }
        }

        [StorePartitionKeyProperty]
        [JsonProperty(CosmosDBStoreConstants.PropertyNames.PartitionKey)]
        public override string PartitionKey
        {
            get
            {
                return FederationCapacity.FormatPartitionKey(this.FederationLocation, this.CapacityId);
            }

            set
            {
            }
        }

        [JsonProperty("CapacityId")]
        public string CapacityId
        {
            get;
            private set;
        }

        [JsonProperty("UsedCount")]
        public ulong UsedCount
        {
            get;
            set;
        }

        [JsonProperty("TotalCount")]
        public ulong TotalCount
        {
            get;
            set;
        }

        [JsonProperty("FederationLocation")]
        public string FederationLocation
        {
            get;
            private set;
        }

        [JsonProperty("FederationId")]
        public string FederationId
        {
            get;
            private set;
        }

        [JsonProperty("BufferCount")]
        public ulong BufferCount
        {
            get;
            set;
        }

        [JsonProperty("IsAvailableForAllocation")]
        public bool IsAvailableForAllocation
        {
            get;
            set;
        }

        [JsonProperty("CapabilityValueByName")]
        public Dictionary<string, List<string>> CapabilityValueByName
        {
            get;
            set;
        }

        [JsonProperty("DerivedCapacities")]
        public List<DerivedCapacity> DerivedCapacities
        {
            get;
            private set;
        }

        public override string StoreEntityType => FederationCapacity.EntityType;

        public static string FormatPartitionKey(string location, string capacityId)
        {
            location.MustNotBeNullOrEmpty(nameof(location));
            capacityId.MustNotBeNullOrWhiteSpace(nameof(capacityId));

            return (location + CosmosDBStoreConstants.CompositeKeyDelimiter + capacityId)
                    .ToLowerInvariant();
        }

        public static string FormatRowKey(string federationId)
        {
            federationId.MustNotBeNullOrEmpty(nameof(federationId));

            return federationId.ToLowerInvariant();
        }

        public override string ToString()
        {
            return "["
                + $"{nameof(this.CapacityId)}: {this.CapacityId}|"
                + $"{nameof(this.FederationLocation)}: {this.FederationLocation}|"
                + $"{nameof(this.FederationId)}: {this.FederationId}|"
                + $"{nameof(this.TotalCount)}: {this.TotalCount}|"
                + $"{nameof(this.UsedCount)}: {this.UsedCount}|"
                + $"{nameof(this.BufferCount)}: {this.BufferCount}|"
                + $"{nameof(this.IsAvailableForAllocation)}: {this.IsAvailableForAllocation}|"
                + $"{nameof(this.DerivedCapacities)}: {JsonConvert.SerializeObject(this.DerivedCapacities)}|"
                + "]";
        }
    }
}
