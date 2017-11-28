import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date

case class Readings (did:String, readings:Array[(Array[(String,String,Double,Double,String)],Long)])
case class ExplodedReadings (did: String, readings:(Array[(String,String,Double,Double,String)],Long))
case class FlattenedReadingsInput (did:String, cid:Array[String], clientOS:Array[String], rssi:Array[Double], snRatio:Array[Double], ssid:Array[String], ts:Long)
case class FlattenedReadings (did:String, cid:String, clientOS:String, rssi:Double, snRatio:Double, ssid:String, ts:Long)

val schema = StructType(Array(
                                StructField("did",StringType,true),
                                StructField("readings",ArrayType(StructType(Array(
                                        StructField("clients",ArrayType(StructType(Array(
                                                StructField("cid",StringType,true),
                                                StructField("clientOS",StringType,true),
                                                StructField("rssi",DoubleType,true),
                                                StructField("snRatio",DoubleType,true),
                                                StructField("ssid",StringType,true))),true),true),
                                        StructField("ts",LongType,true))),true),true)))

object LoadReadings {

        val storagePath = "masterData/wifi-data"
        //SERVER:
        //val storagePath = "masterData/wifi-data"

        val spark = SparkSession.builder.
                                        appName("MyApp").
                                        getOrCreate


        import spark.implicits._

        def loader (path:String): Dataset[Readings] = {
                spark.read
                         .schema(schema)
                         .json(path)
                         .as[Readings]
        }

        def fullFlatten(df:Dataset[FlattenedReadingsInput]) : Dataset[FlattenedReadings] = {
                df.flatMap(row => {
                val seq = for( i <- 0 until row.cid.size) yield {
                        FlattenedReadings(row.did, row.cid(i), row.clientOS(i), row.rssi(i), row.snRatio(i), row.ssid(i), row.ts)
                }
                seq.toSeq
                })
    }

        def flattenDF (df:Dataset[Readings]): Dataset[FlattenedReadingsInput] = {
                val expDF = df.withColumn("readings", explode(col("readings"))).as[ExplodedReadings]
                expDF
                        .select($"did",$"readings.clients.cid",$"readings.clients.clientOS",$"readings.clients.rssi",$"readings.clients.snRatio",$"readings.clients.ssid",$"readings.ts")
                        .drop("readings")
                        .as[FlattenedReadingsInput]
        }

        //Test method, can be called in spark-shell the following way: LoadReadings.test1("8-10-2017.json").show
        def test1 (path:String): Dataset[FlattenedReadings] = {
                val json = loader(path)
                val flat = flattenDF(json)
                fullFlatten(flat).where($"did" === "ed28082926b85c506e3fdb630b6a8bf7")
        }


        def storeMasterData (path:String): Unit = {
                val json = loader(path)
                val flat = flattenDF(json)
                fullFlatten(flat).repartition(1).write.format("com.databricks.spark.csv").mode("append").save(storagePath)             
        }

      	def loop: Unit = {
      		val arrayOfPaths = Array("json/devices/1-10-2017.json",
									"json/devices/10-10-2017.json",
									"json/devices/11-10-2017.json",
									"json/devices/12-10-2017.json",
									"json/devices/13-10-2017.json",
									"json/devices/14-10-2017.json",
									"json/devices/15-10-2017.json",
									"json/devices/16-10-2017.json",
									"json/devices/17-10-2017.json",
									"json/devices/18-10-2017.json",
									"json/devices/19-10-2017.json",
									"json/devices/2-10-2017.json",
									"json/devices/20-10-2017.json",
									"json/devices/21-10-2017.json",
									"json/devices/22-10-2017.json",
									"json/devices/23-10-2017.json",
									"json/devices/24-10-2017.json",
                                    "json/devices/25-10-2017.json",
									"json/devices/3-10-2017.json",
									"json/devices/30-9-2017.json",
									"json/devices/4-10-2017.json",
									"json/devices/5-10-2017.json",
									"json/devices/6-10-2017.json",
									"json/devices/7-10-2017.json",
									"json/devices/8-10-2017.json",
									"json/devices/9-10-2017.json");
            /*val arrayOfPaths = Array("/user/group4/project2/json/devices/1-10-2017.json",
                                    "/user/group4/project2/json/devices/10-10-2017.json",
                                    "/user/group4/project2/json/devices/11-10-2017.json",
                                    "/user/group4/project2/json/devices/12-10-2017.json",
                                    "/user/group4/project2/json/devices/13-10-2017.json",
                                    "/user/group4/project2/json/devices/14-10-2017.json",
                                    "/user/group4/project2/json/devices/15-10-2017.json",
                                    "/user/group4/project2/json/devices/16-10-2017.json",
                                    "/user/group4/project2/json/devices/17-10-2017.json",
                                    "/user/group4/project2/json/devices/18-10-2017.json",
                                    "/user/group4/project2/json/devices/19-10-2017.json",
                                    "/user/group4/project2/json/devices/2-10-2017.json",
                                    "/user/group4/project2/json/devices/20-10-2017.json",
                                    "/user/group4/project2/json/devices/21-10-2017.json",
                                    "/user/group4/project2/json/devices/22-10-2017.json",
                                    "/user/group4/project2/json/devices/23-10-2017.json",
                                    "/user/group4/project2/json/devices/24-10-2017.json",
                                    "/user/group4/project2/json/devices/3-10-2017.json",
                                    "/user/group4/project2/json/devices/30-9-2017.json",
                                    "/user/group4/project2/json/devices/4-10-2017.json",
                                    "/user/group4/project2/json/devices/5-10-2017.json",
                                    "/user/group4/project2/json/devices/6-10-2017.json",
                                    "/user/group4/project2/json/devices/7-10-2017.json",
                                    "/user/group4/project2/json/devices/8-10-2017.json",
                                    "/user/group4/project2/json/devices/9-10-2017.json");*/

      		//val arrayOfPaths2 = Array("masterData/wifi-data2.csv","masterData/wifi-data3.csv");

      		for ( a <- 0 to (arrayOfPaths.length-1)){
      			storeMasterData(arrayOfPaths(a))		
      		}
      		//peterMasterData(arrayOfPaths(counter))	
      	}
           
    }
