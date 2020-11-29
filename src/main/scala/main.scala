import java.io._
import scala.collection.immutable._
import io.circe.generic.auto._, io.circe.syntax._
import io.circe.{ Encoder, Json }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
  
case class AccessLog(
      ip: String, 
      ident: String, 
      user: String, 
      datetime: String, 
      request: String, 
      status: String, 
      size: String, 
      referer: String, 
      userAgent: String, 
      unk: String
)
case class countReport(
      access: Map[String, Long]
)

object report {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("scalaSparkProject.com")
    .getOrCreate()

  import spark.implicits._


  /** Function defined to create a report from the logs data. the function
    * find all the dates having too big number of connection (> 20000) and for
    * each date:
    * - compute the total number of connexion to each URI
    * - compute all the address that access to these URI and the number of time
    *   they accessit on that day
    * @param gzPath is the path poiting to access.log.gz
    * @param outputPath is the path pointing to the output file that is a report
    * in json format with one json report line per date
    */
  def createReport(
      gzPath: String,
      outputPath: String
  ): Unit = {
    val logs = spark.read.text(gzPath)    

    val R = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r

    def toAccessLog(params: List[String]) = AccessLog(
      params(0), 
      params(1), 
      params(2), 
      params(3), 
      params(4), 
      params(5), 
      params(6), 
      params(7), 
      params(8), 
      params(9)
    )

    val logAsString = logs.map(_.getString(0))
    val dsParsed = logAsString.flatMap(x => R.unapplySeq(x))    
    val ds = dsParsed.map(toAccessLog _)
    val dsWithTime = ds.withColumn("datetime", to_timestamp(ds("datetime"), "dd/MMM/yyyy:HH:mm:ss X"))    
    val REQ_EX = "([^ ]+)[ ]+([^ ]+)[ ]+([^ ]+)".r
    val s = "POST /administrator/index.php HTTP/1.1"
    REQ_EX.unapplySeq(s)

    val dsExtended = dsWithTime
      .withColumn("method", regexp_extract(dsWithTime("request"), REQ_EX.toString, 1))
      .withColumn("uri", regexp_extract(dsWithTime("request"), REQ_EX.toString, 2))
      .withColumn("http", regexp_extract(dsWithTime("request"), REQ_EX.toString, 3))
      .drop("request")

    dsExtended.cache()
    dsExtended.createOrReplaceTempView("AccessDataExt")


    val sql = """select count(*) as count, cast(datetime as date) as date from AccessDataExt group by date  HAVING count(*) > 20000 order by count desc"""
    def findDatesHavingMoreThan20kConnections: Array[java.sql.Date] = spark.sql(sql).select("date").map(_.getDate(0)).collect()
    val theDates = findDatesHavingMoreThan20kConnections
    val currentDate = theDates(0)


    def numberOfAccessByUri(currentDate: java.sql.Date) = spark
    .sql("select uri, cast(datetime as date) as date, count(*) as countaccess from AccessDataExt group by date, uri order by countaccess desc")
    .filter(col("date")===currentDate).drop("date")

    def numberOfAccessByIp(currentDate: java.sql.Date) = spark
    .sql("select ip, cast(datetime as date) as date, count(*) as countaccess from AccessDataExt group by date, ip order by countaccess desc")
    .filter(col("date")===currentDate).drop("date")

    def reportUriByDate(currentDate: java.sql.Date) = countReport(numberOfAccessByUri(currentDate)
    .collect
    .map(r => (r.getString(0), r.getLong(1))).toMap)

    def reportIpByDate(currentDate: java.sql.Date) = countReport(numberOfAccessByIp(currentDate)
    .collect
    .map(r => (r.getString(0), r.getLong(1))).toMap)


    import scala.util.parsing.json.JSONObject
    val reportAsSeq = theDates.map(date => (date.toString(),reportUriByDate(date),reportIpByDate(date)))
    val reportJson = reportAsSeq.asJson.noSpaces.toString()

    // write to the file and close it
    val file = new PrintWriter(new File(outputPath))
    file.write(reportJson + "\n")
    file.close()
    
  }

  def main(args: Array[String]) {
    createReport("access.log.gz","logReport.json")
    spark.close()
  }
}