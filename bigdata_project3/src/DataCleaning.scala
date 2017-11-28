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


object DataCleaning {
    
    // Changed to server dir
    val path = "hdfs:/sumo-data/FCDOutput.csv";
    //val path = "99ts.csv"

    type vehicleType0 = (Float, String, String, String, String, String, String, String, String, String, String)
    type personType0 = (Float, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)
    
    type personType = (Float, String, String, String, String, String, String, String)
    type vehicleType = (Float, String, String, String, String, String, String, String, String)
    
    
    val spark = SparkSession.builder.appName("MyApp").master("local").getOrCreate
    
    import spark.implicits._
    

    def vehicleLoader (path:String): Dataset[vehicleType] = {
        
        spark.read
            .format("com.databricks.spark.csv")
            .schema(Encoders.product[vehicleType0].schema)
            .option("header", "true")
            .option("delimiter", ";")
            .csv(path)
            .withColumnRenamed("_1","timestep_time")
            .withColumnRenamed("_2","vehicle_id")
            .withColumnRenamed("_3","vehicle_x")
            .withColumnRenamed("_4","vehicle_y")
            .drop("_5")
            .withColumnRenamed("_6","vehicle_angle")
            .withColumnRenamed("_7","vehicle_type")
            .withColumnRenamed("_8","vehicle_speed")
            .withColumnRenamed("_9","vehicle_pos")
            .withColumnRenamed("_10","vehicle_lane")
            .drop("_11")
            .filter(col("vehicle_id").isNotNull)
            .as[vehicleType]
    }
    
    
    def personLoader (path:String): Dataset[personType] = {
        
        spark.read
            .format("com.databricks.spark.csv")
            .schema(Encoders.product[personType0].schema)
            .option("header", "true")
            .option("delimiter", ";")
            .csv(path)
            .drop("_2").drop("_3").drop("_4").drop("_5")
            .drop("_6").drop("_7").drop("_8").drop("_9").drop("_10").drop("_11")
            .withColumnRenamed("_1","timestep_time")
            .withColumnRenamed("_12","person_id")
            .withColumnRenamed("_13","person_x")
            .withColumnRenamed("_14","person_y")
            .drop("_15")
            .withColumnRenamed("_16","person_angle")
            .withColumnRenamed("_17","person_speed")
            .withColumnRenamed("_18","person_pos")
            .withColumnRenamed("_19","person_edge")
            .drop("_20")
            
            .filter(col("person_id").isNotNull)
            .as[personType]
    }
    
    
    def storeVehicleMasterData: Unit = {
        val json = vehicleLoader(path)
        json.write.format("com.databricks.spark.csv").save("masterData/vehicle")
    }
    
    
    def storePersonMasterData: Unit = {
        val json = personLoader(path)
        json.write.format("com.databricks.spark.csv").save("masterData/person")
    }
    
}