// Code: http://cdn2.hubspot.net/hubfs/438089/notebooks/spark2.0/IoTDeviceGeoIPDS2.0.html

/**
json schema
{
  "device_id": 198164, "device_name": "sensor-pad-198164owomcJZ", "ip": "80.55.20.25", 
  "cca2": "PL", "cca3": "POL", "cn": "Poland", "latitude": 53.080000, "longitude": 18.620000, 
  "scale": "Celsius", "temp": 21, "humidity": 65, "battery_level": 8, "c02_level": 1408, 
  "lcd": "red", "timestamp" :1458081226051 
}
*/

case class DeviceIoTData (
  battery_level: Long, c02_level: Long, cca2: String, cca3: String, 
  cn: String, device_id: Long, device_name: String, humidity: Long, 
  ip: String, latitude: Double, lcd: String, longitude: Double, 
  scale:String, temp: Long, timestamp: Long
)

val ds = spark.read
  .json(s"dbfs:/mnt/$MountName/iot/iot_devices.json")
   .as[DeviceIoTData]
ds.count()
ds.take(10).foreach(println(_))
val dsTempDS = ds.filter(d => {d.temp > 30 && d.humidity > 70})

// Use Dataset APIs for filtering: take(10) returns an Array[DeviceIoTData]; 
// using a foreach() method on the Array collection, I print each item.
dsTempDS.take(10).foreach(println(_))

// filter dataset using where(), filter() = where()
val dsTemp = ds
  .where($"temp" > 25)
  .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
val dsTemp2 = ds
  .filter(d=> {d.temp > 25} )
  .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
ds
  .select($"battery_level", $"c02_level", $"device_name")
  .where($"battery_level" > 6)
  .sort($"c02_level")
  .show

//Apply higher-level Dataset API methods such as groupBy() and avg(). 
//In order words, take all temperatures readings > 25, along with their corrosponding devices' humidity, 
//groupBy ccca3 country code, and compute averages. 
//Plot the resulting Dataset.
ds.filter(d => {d.temp > 25})
  .map(d => (d.temp, d.humidity, d.cca3))
  .groupBy($"_3")
  .avg()
  
// Table and SQL, Visualization
ds.createOrReplaceTempView("iot_device_data")

%sql select cca3, count(distinct device_id) as device_id 
from iot_device_data 
group by cca3 
order by device_id desc limit 100

%sql select cca3, c02_level from iot_device_data where c02_level > 1400 order by c02_level desc


// Converting Dataset to RDDs
val eventsRDD = ds
  .select($"device_name",$"cca3", $"c02_level")
  .where($"c02_level" > 1300)
  .rdd
  .take(10)
  
 eventsRDD: Array[org.apache.spark.sql.Row] = Array([sensor-pad-2n2Pea,NOR,1473], 
 [device-mac-36TWSKiT,ITA,1556], [sensor-pad-8xUD6pzsQI,JPN,1536], [sensor-pad-10BsywSYUF,USA,1470], 
 [meter-gauge-11dlMTZty,ITA,1544], [sensor-pad-14QL93sBR0j,NOR,1346], [sensor-pad-16aXmIJZtdO,USA,1425], 
 [meter-gauge-17zb8Fghhl,USA,1466], [meter-gauge-19eg1BpfCO,USA,1531], [sensor-pad-22oWV2D,JPN,1522])





