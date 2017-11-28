import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date

case class Bookings (name:String, startDate:String, endDate:String, startTime:String, endTime:String, room:String, lecturers:String, programme:String)
//case class BookingsInclUnix (name:String, startDate:String, endDate:String, startTime:String, endTime:String, room:String, lecturers:String, programme:String, startAsUnix: Long, endAsUnix: Long)

val schema = StructType(Array(
                                StructField("name",StringType,true),
                                StructField("startDate",StringType,true),
                                StructField("endDate",StringType,true),
                                StructField("startTime",StringType,true),
                                StructField("endTime",StringType,true),
                                StructField("room",StringType,true),
                                StructField("type",StringType,true),
                                StructField("lecturers",StringType,true),
                                StructField("programme",StringType,true)))
                               
object LoadBookings {

	val spark = SparkSession.builder.
					appName("MyApp").
					getOrCreate



	import spark.implicits._

	val storagePath = "masterData/booking-data"
      // TODO: json folder as sourcePath for loader (and remove parameter for storeMasterData then)

	def loader (path:String): Dataset[Bookings] = {
		spark.read
			 .schema(schema)
			 .json(path)
			 .as[Bookings]
	}


	 def storeMasterData (path:String): Unit = {
				val json = loader(path)
				/*
                        FOR (OPTIONAL 4th view)
                        val roomInclUnixDates = json.withColumn("startAsUnix", unix_timestamp($"startDate", "yyyy-MM-dd") + unix_timestamp($"startTime", "hh:mm"))
								    .withColumn("endAsUnix", unix_timestamp($"endDate", "yyyy-MM-dd")+unix_timestamp($"endTime", "hh:mm"))
                                                    .show*/
				val flatRoom = json
						      .dropDuplicates
							.withColumn("room", explode(split($"room", "[,]"))).as[Bookings]
				val flatPeople = flatRoom
							.withColumn("lecturers", explode(split($"lecturers", "[,]"))).as[Bookings]
                flatPeople.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("append").save(storagePath)             
        }

      def loop: Unit = {
      	val arrayOfPaths = Array("json/bookings/rooms-2017-09-30.json",
      						"json/bookings/rooms-2017-10-01.json",
      						"json/bookings/rooms-2017-10-02.json",
      						"json/bookings/rooms-2017-10-03.json",
      						"json/bookings/rooms-2017-10-04.json",
      						"json/bookings/rooms-2017-10-05.json",
      						"json/bookings/rooms-2017-10-06.json",
      						"json/bookings/rooms-2017-10-07.json",
      						"json/bookings/rooms-2017-10-08.json",
							 "json/bookings/rooms-2017-10-09.json",
							 "json/bookings/rooms-2017-10-10.json",
							 "json/bookings/rooms-2017-10-11.json",
							 "json/bookings/rooms-2017-10-12.json",
							 "json/bookings/rooms-2017-10-13.json",
							 "json/bookings/rooms-2017-10-14.json",
							 "json/bookings/rooms-2017-10-15.json",
							 "json/bookings/rooms-2017-10-16.json",
							 "json/bookings/rooms-2017-10-17.json",
							 "json/bookings/rooms-2017-10-18.json",
							 "json/bookings/rooms-2017-10-19.json",
							 "json/bookings/rooms-2017-10-20.json",
							 "json/bookings/rooms-2017-10-21.json",
							 "json/bookings/rooms-2017-10-22.json",
							 "json/bookings/rooms-2017-10-23.json",
							 "json/bookings/rooms-2017-10-24.json",
							 "json/bookings/rooms-2017-10-25.json");

      		for ( a <- 0 to (arrayOfPaths.length-1)){
      			storeMasterData(arrayOfPaths(a))		
      		}
      }
           
}
