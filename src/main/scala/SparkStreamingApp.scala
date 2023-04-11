import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.matching.Regex


object SparkStreamingApp {
  private def convertToUSDPattern(priceString: String): String = {
    val pricePattern = "\\d{1,3}(,\\d{3})*( - \\d{1,3}(,\\d{3})*)?".r
    val prices = priceString.split("-").flatMap(pricePattern.findAllIn)
    if (prices.isEmpty) {
      priceString
    } else {
      val formattedPrices = prices.map(p => {
        val digits = p.replaceAll(",", "")
        val formattedDigits = digits.reverse.grouped(3).mkString(",").reverse
        s"US$$$formattedDigits"
      }).mkString(" - ")
      s"$formattedPrices"
    }
  }

  private def parseLotNumber(lot: String): String = {
    val lotPattern = "\\d+".r
    lotPattern.findAllIn(lot).mkString("")
  }

  private def convertSaleDateToYMD(dateString: String): String = {
    val dates = dateString.split(" - ")

    val formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val formatter2 = DateTimeFormatter.ofPattern("dd MMM yyyy", Locale.ENGLISH)
    val formatter3 = DateTimeFormatter.ofPattern("MMM dd, yyyy", Locale.ENGLISH)
    val formatter4 = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val parsedStartDate = try {
      LocalDate.parse(dates(0).split(' ').map(_.capitalize).mkString(" "), formatter1)
    } catch {
      case _: Throwable =>
        try {
          LocalDate.parse(dates(0).split(' ').map(_.capitalize).mkString(" "), formatter2)
        } catch {
          case _: Throwable =>
            LocalDate.parse(dates(0).split(' ').map(_.capitalize).mkString(" "), formatter3)
        }
    }
    val startDate = parsedStartDate.format(formatter4)
    var endDate = ""
    if (dates.length >= 2) {
      val parseEndDate = try {
        LocalDate.parse(dates(1).split(' ').map(_.capitalize).mkString(" "), formatter1)
      } catch {
        case _: Throwable =>
          try {
            LocalDate.parse(dates(1).split(' ').map(_.capitalize).mkString(" "), formatter2)
          } catch {
            case _: Throwable =>
              LocalDate.parse(dates(1).split(' ').map(_.capitalize).mkString(" "), formatter3)
          }
      }

      endDate = parseEndDate.format(formatter4)
    }

    if (endDate != "") {
      endDate
    } else {
      startDate
    }
  }

  private def convertArtworkDimension(artwork_dimension: String): String = {
    val dimension_cm_pattern: Regex = """(\b([A-Za-z' \.]* ?)?(?:.*?)(\d*(?:[.,]\d+)?)\s*[x×by]\s*(\d*(?:[.,]\d+)?)(?:(?:\s*[x×by]\s*)(\d*(?:[.,]\d+)?))?\s*(cm|mm|m)\b)+""".r

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
        result.trim().replaceAll("null", "").replaceAll(",", ".")
      case None => "null"
    }
    dimension
  }

  private def convertAuctionRecord(row: Row) = {
    val key = row.getAs[String]("key")
    val value = row.getAs[String]("value")

    val json = parse(value)
    implicit val formats: DefaultFormats = DefaultFormats
    val mapObj = json.extract[Map[String, Any]]
    key match {
      case "mutualart" =>
        val stringOutMap = mapObj.mapValues(v => {
          if (v != null && v.toString.nonEmpty) { v.toString.replaceAll("\n", "") }
          else { "null" }
        })
//        println("artwork_dimension: " + stringOutMap("artwork_size") + ", " + convertArtworkDimension(stringOutMap("artwork_size")))

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
          convertToUSDPattern(stringOutMap("estimate_price")),
          convertToUSDPattern(stringOutMap("realized_price")),
          parseLotNumber(stringOutMap("lot")),
          stringOutMap("auction_venue"),
          convertSaleDateToYMD(stringOutMap("sale_date")),
          "null",
          stringOutMap("auction_sale"),
          "null",
          convertArtworkDimension(stringOutMap("artwork_size"))
        )
      case "artsy" =>
        val stringOutMap = mapObj.mapValues(v => {
          if (v != null && v.toString.nonEmpty) { v.toString.replaceAll("\n", "") }
          else { "null" }
        })
//        println("artwork_dimension: " + stringOutMap("dimension") + ", " + convertArtworkDimension(stringOutMap("dimension")))
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
          parseLotNumber(stringOutMap("lot_number")),
          stringOutMap("organization"),
          convertSaleDateToYMD(stringOutMap("sale_date")),
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
      .getOrCreate()

    spark.sparkContext.setLogLevel("debug")
    import spark.implicits._

    val kafkaServer = "kafka-01:9092"
    val topic = "test_topic2"
    val redshift_jdbc_url = "jdbc:redshift://default.911700376290.ap-northeast-2.redshift-serverless.amazonaws.com:5439/dev?user=chaeeun&password=chaeeunRedshift"

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
        StructField("converted_dimension", StringType)
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

    val convertedDF = converted.toDF(
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

    val unitedDF = redshiftDF.union(convertedDF)
    unitedDF.show(100)

    println("Transform the data to match the Redshift table schema")

    val sqlContext = new SQLContext(spark.sparkContext)

//    val testDf = sqlContext.read
//      .format("io.github.spark_redshift_community.spark.redshift")
//      .option("url", redshift_jdbc_url)
//      .option("query", "select now()")
//      .option("tempdir", "s3n://eazel-emr-spark/query_temp/")
//      .option("forward_spark_s3_credentials", "true")
//      .load()

//    // Write the unionDF to Redshift using the JDBC driver
//    unitedDF.write
//      .format("io.github.spark_redshift_community.spark.redshift")
//      .option("url", redshift_jdbc_url)
//      .option("dbtable", "merged_auction_record")
//      .option("tempdir", "s3://eazel-emr-spark/query_temp/")
//      .option("forward_spark_s3_credentials", "true")
//      .mode(SaveMode.Append)
//      .save()

    //    val joinedDF: DataFrame = sqlContext.read
//      .format("io.github.spark_redshift_community.spark.redshift")
//      .option("url", redshift_jdbc_url)
//      .option("query", "select * from ")
//      .option("tempdir", "s3n://eazel-emr-spark/query_temp/")
//      .load()


    spark.sparkContext.stop()
  }
}