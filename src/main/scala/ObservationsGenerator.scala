import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, size, expr, from_unixtime, input_file_name, lit, sum}
import org.apache.spark.sql.functions.udf

object ObservationsGenerator {

  def getStats(df: DataFrame, name: String) = {

    val unique_records = df.distinct().count()
    val stats = df.describe()
    println(s"COMPUTING STATS FOR ${name}")
    stats
      .withColumn("uniq_cnt", lit(unique_records))
      .show()
  }

  /**
   * give a string fmt= (web_id,)
   * extract web_ids
   * @param str
   */
  def extractWebIds(str: String): String = {
    if (str != null)
      str.replaceAll("[\\(|\\)]","").split(",")(0)
    else
      null
  }

  def extractWebIdsUdf = udf[String,String]( extractWebIds)

  def parseLoans(df:DataFrame): DataFrame = {
    df
      .withColumnRenamed("id","loan_id")
      .drop("_c0") //dropping row number
      .withColumn("webvisit_parsed", extractWebIdsUdf(col("webvisit_id")))
  }

  def saveData(df: DataFrame) = {
    df
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header","true")
      .csv("./locals/output/observations.csv")

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("observation-generator").setMaster("local[*]")

    val spark = SparkSession.
      builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()


    val customer = spark.
      read.
      json("./locals/data/customers.json")
      .withColumnRenamed("id","user_id")

    customer.show()

    val loans = spark
      .read
      .option("inferschema","true")
      .option("header","true")
      .csv("./locals/data/loan-*")

    loans.show()

    val loans_parsed = parseLoans(loans)

    loans_parsed.show()

    val visits = spark
      .read
      .option("inferschema","true")
      .option("header","true")
      .csv("./locals/data/visits.csv")
      .withColumnRenamed("id", "visit_id")
      .drop("_c0") //dropping row number

    visits.show()

   val cust_loans =  loans_parsed
     .join(customer,"user_id")
     .withColumn("loan_ts_parsed", from_unixtime(col("timestamp")))
     .drop("timestamp")



   val observations =  cust_loans
     .join(visits, cust_loans("webvisit_parsed") === visits("visit_id"), "left")
     .drop("webvisit_parsed")
     .drop("webvisit_id")

    getStats(loans, "loans")
    getStats(visits,"visits")
    getStats(customer, "customer")
    getStats(cust_loans, "customer_loans")
    getStats(observations, "observations")

    observations.show()

    saveData(observations)

      }
}
