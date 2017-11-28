import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date

case class Meta (did:String, location:String)


val schema = StructType(Array(
                                StructField("did",StringType,true),
                                StructField("location",StringType,true)
                            ))

object LoadMeta {

    val spark = SparkSession.builder.appName("MyApp").getOrCreate

    import spark.implicits._

    def loader (path:String): Dataset[Meta] = {
            spark.read
                     .schema(schema)
                     .json(path)
                     .as[Meta]
    }


    def storeMetaMasterData (path:String): Unit = {
            val json = loader(path)
            val cleaned = json.withColumn("location", when($"location" === "change_me", "unknown").otherwise($"location"))
            cleaned.write.format("com.databricks.spark.csv").save("meta")
    }

       
}
