using System;
using System.Globalization;
using Newtonsoft.Json;

namespace VehicleTracker.Contracts
{
    [Serializable]
    public class VehiclePositionEvent
    {
        [JsonProperty("eventType")]
        public string EventType => "vehiclePosition";

        [JsonProperty("vehiclePosition")]
        public VehiclePosition VehiclePosition { get; set; }
        
        public override string ToString()
        {
            return $"vehiclePosition [{VehiclePosition}]";
        }
    }
}
