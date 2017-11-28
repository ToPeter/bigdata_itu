import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date


object BatchData {

	type Device = (String,String,String,String,String,String,Long)
	type Meta = (String,String)
	//type Booking = (String, String, String, String, String, String, String, String, Long, Long)

	val devicesCsvPath = "masterData/wifi-data/"
	val metaCsvPath = "masterData/meta-data/"
	//val bookingCsvPath = "masterData/booking-data/"

    val view1 = "generatedViews/view1"
    val view2 = "generatedViews/view2"
    val view3 = "generatedViews/view3"

	val spark = SparkSession.builder.
				appName("MyApp").
				master("local").
				getOrCreate

	import spark.implicits._

	def devicesCsvLoader (path:String): Dataset[Device] = {
		spark.read
			 .schema(Encoders.product[Device].schema)
			 .csv(path)
			 .withColumnRenamed("_1","did")
			 .withColumnRenamed("_2","cid")
			 .withColumnRenamed("_3","clientOS")
			 .withColumnRenamed("_4","rssi")
			 .withColumnRenamed("_5","snRatio")
			 .withColumnRenamed("_6","ssid")
			 .withColumnRenamed("_7","ts")
			 .as[Device]
	}

	def metaCsvLoader (path:String): Dataset[Meta] = {
		spark.read
			 .schema(Encoders.product[Meta].schema)
			 .csv(path)
			 .withColumnRenamed("_1","did")
			 .withColumnRenamed("_2","location")
			 .as[Meta]
	}

	def bokkingsCSVLoader(path:String): Dataset[Booking] = {
		spark.read
			 .schema(Encoders.product[Booking].schema)
			 .csv(path)
			 .withColumnRenamed("_1","name")
			 .withColumnRenamed("_2","startDate")
			 .withColumnRenamed("_3","endDate")
			 .withColumnRenamed("_4","startTime")
			 .withColumnRenamed("_5","endTime")
			 .withColumnRenamed("_6","room")
			 .withColumnRenamed("_7","lecturers")
			 .withColumnRenamed("_8","programme")
			 .withColumnRenamed("_9","startAsUnix")
			 .withColumnRenamed("_9","endAsUnix")
			 .as[Booking]
	}

	def routersInfo(deviceData: Dataset[Device], metaData: Dataset[Meta]): Unit = {

		var aggregatedPerHour = deviceData
								.groupBy($"did", window((from_unixtime($"ts", "yyyy-MM-dd HH:mm:ss.SSSS")), "1 week"))
								.agg(countDistinct("cid"), count("cid"))
								.withColumnRenamed("count(cid)", "Total no of accesses")
								.withColumnRenamed("count(DISTINCT cid)", "Total no of unique clients")
								
		aggregatedPerHour
			.join(metaData, "did")
			.withColumn("From", date_format($"window.start", "yyyy.MM.dd, EEE"))
			.withColumn("To", date_format($"window.end", "yyyy.MM.dd , EEE"))
			.select("did","location", "From", "To", "Total no of unique clients", "Total no of accesses")
			.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(view1)

	}

	def peopleInAudPerHour (deviceData: Dataset[Device], metaData: Dataset[Meta]): Unit = {

		//filter on meta data of Auditoriums by location column
		val metaOfAuditoriums = metaData
								.filter(lower($"location").contains("aud1") || lower($"location").contains("aud2"))
		//filter on wifi-data of auditoriums and replace did with room (e.g. 3 different dids exists for aud1)
		val aggregatedByRoom = 	deviceData
								.join(metaOfAuditoriums, "did")
								.withColumn("location", 
										when(lower($"location")
											.contains("aud1"), "Aud1")
										otherwise("Aud2"))
								.drop($"did")

		//group the data per room for each hour
		aggregatedByRoom
		.groupBy($"location", window((from_unixtime($"ts", "yyyy-MM-dd HH:mm:ss.SSSS")), "1 hour"))
		.agg(countDistinct("cid"))
		.withColumn("Date", date_format($"window.start", "yyyy.MM.dd , EEE"))
		.withColumn("From", date_format($"window.start", "h:mm a"))
		.withColumn("To", date_format($"window.end", "h:mm a"))
		.select("location","Date", "From", "To", "count(DISTINCT cid)")
		.withColumnRenamed("#devices", "count(DISTINCT cid)")
		.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(view3)
	}


	def saveQualityPerRouter(deviceData: Dataset[Device], metaData: Dataset[Meta]): Unit = {
                        
        // groups per did and hour and calculates average of it. it also adds a weekday column 
        val quality = devicesCsvLoader(devicesCsvPath)
			            .groupBy($"did", window((from_unixtime($"ts", "yyyy-MM-dd HH:mm:ss.SSSS")), "1 hour"))
			            .agg(mean("rssi"), mean("snRatio"), count("cid"))
			            .withColumnRenamed("avg(rssi)", "avg_rssi")
			            .withColumnRenamed("avg(snRatio)", "avg_snRatio")
			            .withColumnRenamed("count(cid)", "Total no of accesses")
			            .withColumn("weekday", date_format($"window.start", "E") )
			            .withColumn("startHour", date_format($"window.start", "HH"))
			            .withColumn("endHour", date_format($"window.end", "HH"))
			            .drop($"window")
            
        //groups the different data based on the weekday (and time) and calculates averag
        // e.g. AVG-values for did on mondays between 1pm and 2pm
        quality
        	.groupBy($"did", $"weekday", $"startHour", $"endHour")
        	.agg(mean("avg_rssi").cast(ShortType), mean("avg_snRatio").cast(ShortType), mean("Total no of accesses").cast(ShortType))
        	.join(metaData, "did")
        	.withColumnRenamed("CAST(avg(avg_rssi) AS SMALLINT)", "avg_rssi")
			.withColumnRenamed("CAST(avg(avg_snRatio) AS SMALLINT)", "avg_snRatio")
			.withColumnRenamed("CAST(avg(Total no of accesses) AS SMALLINT)", "Avg. Total no of accesses")
       		.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(view2)
    }

    /* IN PROGRESS (OPTIONAL 4th BATCH VIEW
    def getHardWorkingStudentsPerLecture(deviceData: Dataset[Device], metaData: Dataset[Meta], bookingData: Dataset[Booking]): Unit = {
    		val aggregatedByRoom = 	deviceData
								.join(metaData, "did")
								.withColumn("location", 
										when(lower($"location")
											.contains("aud1"), "Aud 1")
										when(lower($"location")
											.contains("aud2"), "Aud 2")
										when(lower($"location")
											.contains("aud3"), "Aud 3")
										when(lower($"location")
											.contains("aud4"), "Aud 4")
								.drop($"did")

			//based on if a cid was in a room during lecture time	(outcome distinct lecture, cid combinations)				
			val allExistingCidLectureCombinations = aggregatedByRoom
													.join(bookingData, aggregatedByRoom("location").contains(bookingData("room") && aggregatedByRoom("ts") >= bookingData("startAsUnix") && aggregatedByRoom("ts") <= bookingData("endAsUnix"))
													.select("name", "cid")
													.dropDuplicates()

			val numberOfConnectionPerCID = deviceData
											.groupBy($"cid")
											.agg(countDistinct("ts"))
											.withColumnRenamed("count(DISTINCT ts)", "Total no of connections")
											.select("cid", "Total no of connections")

			val connectionsPerStudentsOfLecture = allExistingCidLectureCombinations
													.join(numberOfConnectionPerCID, "cid")
													.groupBy("name")
													.agg(sum("Total no of connections"))
													.show()
    }*/


	def main : Unit = {
		var devicesData = devicesCsvLoader(devicesCsvPath)
		var metaData = metaCsvLoader(metaCsvPath)
		var bookingData = bokkingsCSVLoader(bookingCsvPath)
		routersInfo(devicesData, metaData)
		peopleInAudPerHour(devicesData, metaData)
		saveQualityPerRouter(devicesData, metaData)
		//getHardWorkingStudentsPerLecture(devicesData, metaData, bookingData)
	}

}