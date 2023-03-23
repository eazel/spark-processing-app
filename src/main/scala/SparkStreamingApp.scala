import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, lit, monotonically_increasing_id, to_date}
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, StringType, StructField, StructType}
import com.databricks.spark.redshift._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import scala.util.matching.Regex


object SparkStreamingApp {
  private def convertArtworkDimension(artwork_dimension: String): String = {
    val dimension_cm_pattern: Regex = "(\\b([A-Za-z' \\.]* ?)?(?:.*?)(\\d*(?:[.,]\\d+)?)\\s*x\\s*(\\d*(?:[.,]\\d+)?)(?:(?:\\s*x\\s*)(\\d*(?:[.,]\\d+)?))?\\s*(cm|mm|m)\\b)+".r

    val dimension: String = dimension_cm_pattern findFirstMatchIn artwork_dimension match {
      case Some(_) =>
        var result: String = ""
        val matcher = dimension_cm_pattern.findAllIn(artwork_dimension)
        for (i <- 2 to matcher.groupCount) {
          val currentGroup = matcher.group(i)
          val prevGroup = matcher.group(i - 1)
          val prevWasDigit = prevGroup != null && prevGroup.matches("\\d+(?:[.,]\\d+)?")
          if (prevWasDigit && currentGroup != null && !currentGroup.matches("\\b(?:cm|m(?:m)?)\\b")) {
            result = result + " x " + currentGroup
          } else {
            result = result + currentGroup
          }
        }
        result.trim().replaceAll("null", "")
      case None => "null"
    }
    dimension
  }

  private def convertAuctionRecord(row: Row) = {
    val key = row.getAs[String]("key")
    val value = row.getAs[String]("value")
    println("key: " + key)
    println("value: " + value)

    val json = parse(value)
    implicit val formats: DefaultFormats = DefaultFormats
    val mapObj = json.extract[Map[String, Any]]
    key match {
      case "mutualart" =>
        val outMap = mapObj.-("auction_sale", "after", "timestamp", "artwork_link")
        val stringOutMap = outMap.mapValues(v => {
          if (v != null) { v.toString.replaceAll("\n", "") }
          else { "null" }
        })
        (
          "mutualart",
          stringOutMap("artfacts_artist_id"),
          stringOutMap("mutualart_artist_id"),
          stringOutMap("artist_name"),
          stringOutMap("artwork_title"),
          stringOutMap("artwork_date_created"),
          stringOutMap("artwork_medium"),
          stringOutMap("artwork_size"),
          stringOutMap("artwork_edition"),
          stringOutMap("estimate_price"),
          stringOutMap("realized_price"),
          stringOutMap("lot"),
          stringOutMap("auction_venue"),
          stringOutMap("sale_date"),
          "null",
          "null",
          "null",
          "null"
        )
      case "artsy" =>
        val outMap = mapObj.-("bought_in", "currency", "auction_id", "timestamp")
        val stringOutMap = outMap.mapValues(v => {
          if (v != null) { v.toString.replaceAll("\n", "") }
          else { "null" }
        })

        (
          "artsy",
          stringOutMap("artfacts_artist_id"),
          stringOutMap("artsy_artist_id"),
          stringOutMap("artfacts_artist_name"),
          stringOutMap("title"),
          stringOutMap("created_year"),
          stringOutMap("medium"),
          stringOutMap("dimension"),
          "null",
          stringOutMap("estimate_price"),
          stringOutMap("realized_price"),
          stringOutMap("lot_number"),
          stringOutMap("organization"),
          stringOutMap("sale_date"),
          "null",
          stringOutMap("sale_title"),
          stringOutMap("location"),
          convertArtworkDimension(stringOutMap("dimension"))
        )

      case _ => throw new IllegalArgumentException(s"Unknown source format: $key")
    }
  }

  def main(args: Array[String]): Unit = {
    val logger = LogManager.getLogger(this.getClass.getName)

    val spark = SparkSession.builder()
      .appName("SparkStreamingApp")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("info")
    import spark.implicits._

    val kafkaServer = "ec2-3-36-52-200.ap-northeast-2.compute.amazonaws.com:9092"
    val topic = "test_topic2"

    val redshiftSchema = StructType(
      Seq(
        StructField("record_source", StringType),
        StructField("artfacts_artist_id", IntegerType),
        StructField("source_artist_id", StringType),
        StructField("artfacts_artist_name", StringType),
        StructField("title", StringType),
        StructField("artwork_created_date", StringType),
        StructField("medium", StringType),
        StructField("dimension", StringType),
        StructField("edition", StringType),
        StructField("estimate_price", StringType),
        StructField("realized_price", StringType),
        StructField("lot", StringType),
        StructField("auction_sale_organization", StringType),
        StructField("sale_date", StringType),
        StructField("category", StringType),
        StructField("auction_sale_title", StringType),
        StructField("location", StringType),
        StructField("converted_dimension", StringType),
        StructField("id", IntegerType)
      )
    )

    val redshiftDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], redshiftSchema)

    // Read the Kafka topic into a DataFrame
    val kafkaDF = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val keyValueDF = kafkaDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val converted = keyValueDF
      .filter(row => {
        val key = row.getAs[String]("key")
        key != null && !key.matches("[0-9]+")
      })
      .map(convertAuctionRecord)

    var convertedDF = converted.toDF(
      "record_source",
      "artfacts_artist_id",
      "source_artist_id",
      "artfacts_artist_name",
      "title",
      "artwork_created_date",
      "medium",
      "dimension",
      "edition",
      "estimate_price",
      "realized_price",
      "lot",
      "auction_sale_organization",
      "sale_date",
      "category",
      "auction_sale_title",
      "location",
      "converted_dimension"
    )

    convertedDF = convertedDF.withColumn("id", monotonically_increasing_id())

    val unitedDF = redshiftDF.union(convertedDF)
    unitedDF.show()

    // println("Transform the data to match the Redshift table schema")

    //    Class.forName("com.amazon.redshift.jdbc42.Driver")
    //
    //    val sqlContext = new SQLContext(spark.sparkContext)
    //
    //    // Write the transformed data to Redshift using the JDBC driver
    //    val testDf = sqlContext.read
    //      .format("io.github.spark_redshift_community.spark.redshift")
    //      .option("url", redshift_jdbc_url)
    //      .option("query", "select now()")
    //      .option("tempdir", "s3n://eazel-emr-spark/query_temp/")
    //      .option("forward_spark_s3_credentials", "true")
    //      //      .option("user", "chaeeun")
    //      //      .option("password", "chaeeunRedshift")
    //      .load()
    //
    //    // Write the transformed data to Redshift using the JDBC driver
    //    val writer = finalDF.write
    //      .format("io.github.spark_redshift_community.spark.redshift")
    //      .option("url", redshift_jdbc_url)
    //      .option("dbtable", "merged_auction_record")
    //      .option("tempdir", "s3n://eazel-emr-spark/query_temp/")
    //      .option("forward_spark_s3_credentials", "true")
    ////      .option("user", "chaeeun")
    ////      .option("password", "chaeeunRedshift")
    //      .mode(SaveMode.Append)
    //      .save()
    ////      .option("batchsize", "100")
    ////      .option("isolationLevel", "NONE")
    //
    //    println("Write the transformed data to Redshift using the JDBC driver")
    //
    ////      val writer2 = finalDF.write
    ////        .format("com.databricks.spark.redshift")
    ////        .option("url", redshift_jdbc_url)
    ////        .option("dbtable", "merged_auction_record")
    //////        .option("tempdir", "s3://<s3-bucket>/path/to/temp/dir/")
    ////        .mode("append")
    ////        .save()
    //////      writer.start().awaitTermination()

    spark.sparkContext.stop()
  }
}