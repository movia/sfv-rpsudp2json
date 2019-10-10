using System;
using System.Globalization;
using Newtonsoft.Json;

namespace VehicleTracker.Contracts
{
    [Serializable]
    public class Position
    {
        [JsonProperty("latitude")]
        public double Latitude { get; set; }

        [JsonProperty("longitude")]
        public double Longitude { get; set; }

        public override string ToString()
        {
            return $"{Latitude.ToString("f6", CultureInfo.InvariantCulture)},{Longitude.ToString("f6", CultureInfo.InvariantCulture)}";
        }

    }
}
