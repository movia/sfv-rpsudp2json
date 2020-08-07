using System;
using Microsoft.Azure.Cosmos.Table;

using VehicleTracker.Contracts;

namespace RpsUdpToJson
{
    static class VehicleJourneyAssignmentExtensions
    {
        public static DynamicTableEntity ToTableEntity(this VehicleJourneyAssignment vehicleJourneyAssignment)
        {
            var item = new DynamicTableEntity("1", vehicleJourneyAssignment.VehicleRef);
            item.Properties.Add("VehicleRef", new EntityProperty(vehicleJourneyAssignment.VehicleRef));
            item.Properties.Add("JourneyRef", new EntityProperty(vehicleJourneyAssignment.JourneyRef));
            item.Properties.Add("ValidFromUtc", new EntityProperty(vehicleJourneyAssignment.ValidFromUtc));

            if (vehicleJourneyAssignment.InvalidFromUtc != null)
                item.Properties.Add("InvalidFromUtc", new EntityProperty(vehicleJourneyAssignment.InvalidFromUtc.Value));

            return item;
        }

        public static VehicleJourneyAssignment ToVehicleJourneyAssignment(this DynamicTableEntity item)
        {
            var vehicleJourneyAssignment = new VehicleJourneyAssignment
            {
                JourneyRef = item.Properties["JourneyRef"].StringValue,
                VehicleRef = item.Properties["VehicleRef"].StringValue,
                ValidFromUtc = item.Properties["ValidFromUtc"].DateTime ?? DateTime.MinValue
            };

            if (item.Properties.TryGetValue("InvalidFromUtc", out var invalidFromUtc))
                vehicleJourneyAssignment.InvalidFromUtc = invalidFromUtc.DateTime;

            return vehicleJourneyAssignment;
        }
    }
}
