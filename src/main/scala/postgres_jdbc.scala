/**
  * Created by wdc on 2017/4/17.
  */
import java.sql.{Connection, DriverManager, ResultSet}
import java.sql.DriverManager

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import java.util.Properties

import org.apache.spark.sql.types._

object postgres_jdbc {
  val conn_str = "jdbc:postgresql://192.168.199.152:5432/lottery?user=cwlgp&password=cwl12345"

  def getPGData1(): DataFrame ={
    val spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///").getOrCreate()  //非windows环境可以不用config
    val table = "personas_final_0411"
    val connectionProperties = new Properties()
    connectionProperties.setProperty("dbtable", "sales_forecast_2009");// 设置表
    connectionProperties.setProperty("user", "cwlgp");// 设置用户名
    connectionProperties.setProperty("password", "cwl12345");// 设置密码
    val schema = StructType(Array(
      StructField("name", StringType,nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField("age", DoubleType,nullable = true),
      StructField("income", DoubleType, nullable = true),
      StructField("job", StringType,nullable = true),
      StructField("address", StringType, nullable = true),
      StructField("education", StringType,nullable = true),
      StructField("marriage", StringType,nullable = true),
      StructField("family_size", DoubleType,nullable = true),
      StructField("kids_num", DoubleType,nullable = true),
      StructField("login_date", StringType,nullable = true),
      StructField("login_time", StringType,nullable = true),
      StructField("stay_time", DoubleType,nullable = true),
      StructField("bounce_rate", DoubleType,nullable = true),
      StructField("active_degree", DoubleType,nullable = true),
      StructField("amount", DoubleType,nullable = true),
      StructField("trade_time", StringType,nullable = true),
      StructField("game_id", StringType,nullable = true),
      StructField("payment", StringType,nullable = true),
      StructField("risk", StringType,nullable = true),
      StructField("reputation", StringType,nullable = true),
      StructField("id", IntegerType,nullable = true)))
    val df = spark.read.schema(schema).jdbc(conn_str, table, connectionProperties)
    return df
  }

  def main(args: Array[String]) {
    val df = getPGData1()
    df.show()
    df.printSchema()
  }
}
