// ******* THIS IS THE 3RD VIEW *********** //

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.split
import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.functions._


import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date

case class DataRead (timestep_time:Int, vehicle_id:Long, vehicle_x:Long, vehicle_y:Long, vehicle_z:Long, vehicle_angle:Long, vehicle_type:String, vehicle_speed:Long, vehicle_pos:Long, vehicle_lane:String, vehicle_slope:Long,
                     person_id:Long, person_x:Long, person_y:Long, person_z:Long, person_angle:Long, person_speed:Long, person_pos:Long, person_edge:String, person_slope:Long)


object BatchViews {
    
    // Changed to server dir
    val path = "99ts.csv"
    
    type personType = (Float, String, String, String, String, String, String, String)
    type vehicleType = (Float, String, String, String, String, String, String, String, String)
    
    val vehicleCsvPath = "masterData/vehicle/"
    val personCsvPath = "masterData/person/"

    val spark = SparkSession.builder.appName("MyApp").master("local").getOrCreate
    
    import spark.implicits._
    
    
    def vehicleLoader (path:String): Dataset[vehicleType] = {
        
        spark.read
            .format("com.databricks.spark.csv")
            .schema(Encoders.product[vehicleType].schema)
            .csv(path)
            .withColumnRenamed("_1","timestep_time")
            .withColumnRenamed("_2","vehicle_id")
            .withColumnRenamed("_3","vehicle_x")
            .withColumnRenamed("_4","vehicle_y")
            .withColumnRenamed("_5","vehicle_angle")
            .withColumnRenamed("_6","vehicle_type")
            .withColumnRenamed("_7","vehicle_speed")
            .withColumnRenamed("_8","vehicle_pos")
            .withColumnRenamed("_9","vehicle_lane")
            .as[vehicleType]
    }
    
    
    def personLoader (path:String): Dataset[personType] = {
        
        spark.read
            .format("com.databricks.spark.csv")
            .schema(Encoders.product[personType].schema)
            .csv(path)
            .withColumnRenamed("_1","timestep_time")
            .withColumnRenamed("_2","person_id")
            .withColumnRenamed("_3","person_x")
            .withColumnRenamed("_4","person_y")
            .withColumnRenamed("_5","person_angle")
            .withColumnRenamed("_6","person_speed")
            .withColumnRenamed("_7","person_pos")
            .withColumnRenamed("_8","person_edge")
            .as[personType]
    }
    
    
    def getVehicleLane (edge: String) : String = {
        var split = edge.split("_")
        return split(0) + "_" + split(1)
    }
    
    
    def test2 : Unit = {
        personLoader(personCsvPath).show
    }



    def creeateZipperView : Unit  = {
        
        val df = vehicleLoader(vehicleCsvPath)
        
        val df2 = df.groupBy("vehicle_lane").agg(count("vehicle_lane").as("nb_cars"))
        
        // Splitting the lane column to obtain all the edges ids and the lanes indexes in two separated columns
        
        val get_last = udf((xs: Seq[String]) => Try(xs.last).toOption)
        
        
        val cars_per_lane = df2.withColumn("_start", lit(0: Int))
            .withColumn("_length", length(col("vehicle_lane")) - 2)
            .withColumn("edge_id", col("vehicle_lane").substr(col("_start"), col("_length")))
            .withColumn("lane_index" , get_last(split(col("vehicle_lane"), "_")))
            .withColumn("direction", when(col("edge_id").startsWith("-"), lit(0: Int)).otherwise(lit(1: Int)))
            .withColumn("edge_id", when(col("edge_id").startsWith("-"), split(col("edge_id"),"-")(1)).otherwise(col("edge_id")))
            .orderBy(col("edge_id"))
            .drop("_lane")
            .drop("_start")
            .drop("_length")
        
        
        val edges3Lanes = cars_per_lane.groupBy("edge_id").agg(count("edge_id").as("nb_lanes"))
            .filter($"nb_lanes" > 2)
        
        //removing the edges that have less than 3 lanes
        val finalTable = cars_per_lane.join(edges3Lanes, cars_per_lane("edge_id") === edges3Lanes("edge_id"))
            .drop(edges3Lanes("edge_id")).show
    }

}