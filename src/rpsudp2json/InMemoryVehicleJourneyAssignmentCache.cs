using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using VehicleTracker.Contracts;

namespace RpsUdpToJson
{
    public class InMemoryVehicleJourneyAssignmentCache : IVehicleJourneyAssignmentCache
    {
        private readonly ConcurrentDictionary<string, VehicleJourneyAssignment> vehicleJourneyAssignmentCache;

        public InMemoryVehicleJourneyAssignmentCache()
        {
            vehicleJourneyAssignmentCache = new ConcurrentDictionary<string, VehicleJourneyAssignment>(StringComparer.OrdinalIgnoreCase);
        }

        public bool TryGet(string vehicleRef, [MaybeNullWhen(false)] out VehicleJourneyAssignment? vehicleJourneyAssignment)
        {
            if (vehicleRef == null)
            {
                vehicleJourneyAssignment = null;
                return false;
            }

            return vehicleJourneyAssignmentCache.TryGetValue(vehicleRef, out vehicleJourneyAssignment);
        }

        public void Put(VehicleJourneyAssignment vehicleJourneyAssignment)
        {
            vehicleJourneyAssignmentCache.AddOrUpdate(vehicleJourneyAssignment.VehicleRef, vehicleJourneyAssignment, (_, existing) => existing.ValidFromUtc > vehicleJourneyAssignment.ValidFromUtc ? existing : vehicleJourneyAssignment);
        }

        public bool TryGetByJourney(string journeyRef, out IEnumerable<VehicleJourneyAssignment> vehicleJourneyAssignments)
        {
            throw new NotImplementedException();
        }
    }
}
