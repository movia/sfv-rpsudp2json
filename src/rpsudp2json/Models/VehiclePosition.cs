using System;
using System.Globalization;
using Newtonsoft.Json;

namespace VehicleTracker.Contracts
{
    [Serializable]
    public class VehiclePosition
    {
        [JsonProperty("vehicleRef")]
        public string VehicleRef { get; set; }

        [JsonProperty("position")]
        public Position Position { get; set; }

        [JsonProperty("bearing", NullValueHandling = NullValueHandling.Ignore)]
        public double? Bearing { get; set; }

        [JsonProperty("speed", NullValueHandling = NullValueHandling.Ignore)]
        public double? Speed { get; set; }

        [JsonProperty("accuracy", NullValueHandling = NullValueHandling.Ignore)]
        public double? Accuracy { get; set; }

        [JsonProperty("timestamp")]
        public DateTime Timestamp { get; set; }

        [JsonProperty("journeyRef", NullValueHandling = NullValueHandling.Ignore)]
        public string JourneyRef { get; set; }

        [JsonProperty("sourceJourneyRef", NullValueHandling = NullValueHandling.Ignore)]
        public string SourceJourneyRef { get; set; }

        public override string ToString()
        {
            return $"{VehicleRef} @ {Position}";
        }
    }
}
