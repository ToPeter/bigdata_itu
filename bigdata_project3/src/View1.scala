import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date

//val path = "99ts.csv"
val path = "hdfs:/sumo-data/FCDOutput.csv"
val spark = SparkSession.builder.appName("MyApp").master("local").getOrCreate  
import spark.implicits._

object Vehicle {

    type vehicleType = (Double, Integer, Double, Double, Double, String, Double, Double, String)

    

     def loader (path:String): Dataset[vehicleType] = {
        
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



    def testMasterData : Unit  = {
            val csv = loader(path)
            csv.show           
    }

    // check no of cars
    def nrPerHour : Unit = {
        loader(path)
            //.filter($"vehicle_id".isNotNull)
            .groupBy($"timestep_time")
            .agg(countDistinct("vehicle_id"))
            .orderBy($"timestep_time".asc)
            .show(100, true) // we have 100 timpestamps in 99ts.csv file
    }

    def nrPerLanes : Unit = {
        loader(path)
            .groupBy($"vehicle_lane")
            .agg(countDistinct("vehicle_id"))
            .orderBy(countDistinct("vehicle_id").desc)
            .show(100, true) 
    }

    def countTs : Unit = {
        loader(path).select(countDistinct("timestep_time")).show
        
    }

    def splitLane : Unit = {
        print(":1801667997_1".split("_"))
        var split = ":1801667997_1_0".split("_")
        print(split(0) + "_" + split(1))
        // split dupa underscore
        // return append primele 2
    }

    def getVehicleEdge (edge: String) : String = {
        var split = edge.split("_")
        return split(0) + "_" + split(1)
    }
    
    def generateView1 : Unit  = {
        
        val df = loader(path)
        // Removing null rows (i.e rows for people)
        //val df = csv.filter(csv("vehicle_id").isNotNull)
                
        // Getting the edge ids
        df.withColumn("_start", lit(0: Int))
            .withColumn("_length", length(col("vehicle_lane")) - 2)
            .withColumn("edge_id", col("vehicle_lane").substr(col("_start"), col("_length")))
            .withColumn("edge_id", when(col("edge_id").startsWith("-"), split(col("edge_id"),"-")(1)).otherwise(col("edge_id")))
            .drop("_start")
            .drop("_length")
            .groupBy("edge_id").agg(count("edge_id"))
            .orderBy(count("edge_id").desc)
            .repartition(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save("views/view1")
            //.show
        
    }
}

