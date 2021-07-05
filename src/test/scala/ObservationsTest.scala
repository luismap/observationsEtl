import org.apache.spark.sql.{Row, SparkSession}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("ObservationsTest")
      .getOrCreate()
  }

}


class ObservationsTest extends AnyFunSpec with SparkSessionTestWrapper with DataFrameComparer {

  import spark.implicits._
  import ObservationsGenerator.{extractWebIds, extractWebIdsUdf}

  import org.apache.spark.sql.types._

  describe(".extractWebIds") {
    it("for (123,) returns 123 ") {
      assert(extractWebIds("(123,)") == "123")
    }

    it("for null returns null ") {
      assert(extractWebIds(null) == null)
    }

    it("for (45, should return 45") {
      assert(extractWebIds("(45,") == "45")
    }

  }


  it("webids dataframe") {

    val sourceDF = Seq(
      "(112,)",
      null,
      "(278,)"
    ).toDF("webids")

    val actualDF = sourceDF
      .withColumn("webvisit_parsed", extractWebIdsUdf(col("webids")))

    val expectedSchema = StructType(
      StructField("webids", StringType, true) ::
        StructField("webvisit_parsed", StringType, true) ::
        Nil
    )

    val expectedData = Seq(
      Row("(112,)", "112"),
      Row(null, null),
      Row("(278,)", "278")

    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema
    )

    assertSmallDataFrameEquality(actualDF, expectedDF)

  }
}
