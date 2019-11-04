using System;
using System.IO;
using System.Text;

using Microsoft.Extensions.Logging;

using VehicleTracker.Contracts;

namespace RpsUdpToJson
{
    public class UdpConverter
    {
        ILogger<UdpConverter> logger;

        public UdpConverter(ILogger<UdpConverter> logger)
        {
            this.logger = logger;
        }

        public  bool TryParseVehiclePosition(byte[] bytes, out VehiclePosition vehiclePositon)
        {
            try
            {
                vehiclePositon = new VehiclePosition();

                using (var stream = new MemoryStream(bytes))
                using (var reader = new BinaryReader(stream))
                {

                    /* MessageType */
                    reader.ReadByte();

                    /* Priority */
                    reader.ReadByte();

                    /* UnitIdentity */
                    string.Format("{0:x2}{1:x2}{2:x2}{3:x2}{4:x2}{5:x2}{6:x2}{7:x2}", reader.ReadByte(), reader.ReadByte(), reader.ReadByte(), reader.ReadByte(), reader.ReadByte(), reader.ReadByte(), reader.ReadByte(), reader.ReadByte());

                    /* SequenceNumber */
                    reader.ReadUInt16();

                    // Time - ms since UTC midnight */
                    var timestamp = reader.ReadUInt32();
                    var sendDateTime = DateTime.UtcNow.Date.AddDays(timestamp > 86400000 / 2 && DateTime.UtcNow.TimeOfDay.TotalMilliseconds < 86400000 / 2 ? -1 : 0).AddMilliseconds(timestamp);
                    vehiclePositon.Timestamp = sendDateTime;

                    vehiclePositon.Position = new Position();

                    // Latitude	Single	-90,00000 - +90,00000	IEEE 32-bits floating point number with single precision.
                    var lat = reader.ReadSingle();
                    if (!float.IsNaN(lat) && -90f <= lat && lat <= +90 && Math.Abs(lat) > 0.00001f)
                        vehiclePositon.Position.Latitude = lat;

                    // Longitude	Single	-180,00000 - +180,00000
                    var lng = reader.ReadSingle();
                    if (!float.IsNaN(lng) && -180f <= lng && lng <= +180 && Math.Abs(lng) > 0.00001f)
                        vehiclePositon.Position.Longitude = lng;

                    // 8	24	Yes	Speed	UInt16	0,00  655.36 m/s	Speed in steps of 0,01 m/s
                    vehiclePositon.Speed = reader.ReadUInt16() * 0.01;


                    // 9	26	Yes	Direction	UInt16	0,00  359.99 	Direction in steps of 0.01
                    vehiclePositon.Bearing = reader.ReadUInt16() * 0.01;


                    // 10	28	Yes	GPS-quality	Byte	Quality for GPS.	See 4.2.6.
                    var gpsQualityByte = reader.ReadByte();
                    //PositionOutputBuffer.GpsFixType = (byte)(gpsQualityByte & (byte)0xF);
                    vehiclePositon.Accuracy = (byte)((gpsQualityByte >> 4) & (byte)0xF);

                    // 11	29	Some	Signals	Byte	4 signals of 2 bits each	See 4.2.7.
                    var signalsByte = reader.ReadByte();
                    //PositionOutputBuffer.InService = (byte)((signalsByte >> 6) & 0x03);
                    //PositionOutputBuffer.StopRequested = (byte)((signalsByte >> 4) & 0x03);
                    //PositionOutputBuffer.DoorReleased = (byte)((signalsByte >> 2) & 0x03);
                    //PositionOutputBuffer.PowerOn = (byte)((signalsByte >> 0) & 0x03);

                    // 12	30	No	Distance	UInt32	Vehicle distance in steps of 5 meters	See 4.2.8
                    var distance = reader.ReadUInt32();
                    //if (distance > 0)
                    //    PositionOutputBuffer.Distance = distance * 5;
                    //else
                    //    PositionOutputBuffer.Distance_IsNull = true;

                    // L13	34	Yes	Length of field 13	Byte	0-255	Number of bytes.        
                    var vehicleIdLength = reader.ReadByte();

                    // 13		Yes	Vehicle id	String	Identity of vehicle.	Must be a vehicle fixed unique value, e.g. BUS FMS VI field.
                    if (vehicleIdLength > 0)
                        vehiclePositon.VehicleRef = Encoding.ASCII.GetString(reader.ReadBytes(vehicleIdLength));

                    // L14		No	Length of field 14	Byte	0-255	Number of bytes.
                    var driverIdLength = reader.ReadByte();

                    // 14		No	Driver id	String	Identity of current driver.	Optional. Could be BUS FMS DI, or unique id from other available source in vehicle.
                    if (driverIdLength > 0)
                        Encoding.ASCII.GetString(reader.ReadBytes(driverIdLength));


                    // L15		Yes	Length of field 15	Byte	0-255	Number of bytes.
                    var serviceJourneyIdLength = reader.ReadByte();

                    // 15		Yes	Service Journey Id	String	See 4.2.11.
                    if (serviceJourneyIdLength > 0)
                        vehiclePositon.SourceJourneyRef = Encoding.ASCII.GetString(reader.ReadBytes(serviceJourneyIdLength));

                    // L16		No	Length of field 16	Byte	0-255	Number of bytes.
                    var accountIdLength = reader.ReadByte();

                    // 16		No	Account Id	String	Identity of operator.	Optional.
                    if (accountIdLength > 0)
                        Encoding.ASCII.GetString(reader.ReadBytes(accountIdLength));

                    return true;
                }
            }
            catch (Exception ex)
            {
                vehiclePositon = null;
                logger.LogTrace(ex, $"Failed to parse message: '{BitConverter.ToString(bytes).Replace("-", "")}'");
                return false;
            }
        }

    }
}
