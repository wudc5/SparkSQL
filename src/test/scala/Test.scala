import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, IntegerType, StringType}
/**
  * Created by Administrator on 2017/4/15.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///").getOrCreate()  //非windows环境可以不用config
//    val schema = StructType(Array(
//      StructField("name", StringType,nullable = true),
//      StructField("gender", StringType, nullable = true),
//      StructField("age", DoubleType,nullable = true),
//      StructField("income", DoubleType, nullable = true),
//      StructField("job", StringType,nullable = true),
//      StructField("address", StringType, nullable = true),
//      StructField("education", StringType,nullable = true),
//      StructField("marriage", StringType,nullable = true),
//      StructField("family_size", DoubleType,nullable = true),
//      StructField("kids_num", DoubleType,nullable = true),
//      StructField("login_date", StringType,nullable = true),
//      StructField("login_time", StringType,nullable = true),
//      StructField("stay_time", DoubleType,nullable = true),
//      StructField("bounce_rate", DoubleType,nullable = true),
//      StructField("active_degree", DoubleType,nullable = true),
//      StructField("amount", DoubleType,nullable = true),
//      StructField("trade_time", StringType,nullable = true),
//      StructField("game_id", StringType,nullable = true),
//      StructField("payment", StringType,nullable = true),
//      StructField("risk", StringType,nullable = true),
//      StructField("reputation", StringType,nullable = true),
//      StructField("id", IntegerType,nullable = true)))

//    val data_df = spark.read.option("sep","|").option("header","true").schema(schema).csv(args(0))
//    data_df.show()
//    data_df.printSchema()
//    data_df.select("id").show()

    val df_pg = postgres_jdbc.getPGData1()
    df_pg.show()
    val data_df = df_pg.drop("name","login_date","login_time", "trade_time","id")

//    val stringIndexer = new StringIndexer().setInputCol("job").setOutputCol("jobIndexed").fit(data_df)
//    val indexed_test_df = stringIndexer.transform(data_df)
//    val oneHotEncoder = new OneHotEncoder().setInputCol("jobIndexed").setOutputCol("jobEncoder")
//    val ohe_df = oneHotEncoder.transform(indexed_test_df)
//    ohe_df.show()
//
    spark.udf.register("trans_gender", (gender:String)=>{gender match{
      case "男" => 0.0
      case "女" => 1.0
    }})

    spark.udf.register("trans_edu", (education:String)=>{education match{
      case "高中" => 0.0
      case "大专" => 1.0
      case "本科" => 2.0
      case "硕士" => 3.0
      case "博士" => 4.0
    }})
//
//    ohe_df.createOrReplaceTempView("data")
//
//    val final_df = spark.sql("select *, trans_gender(gender) indexed_gender, trans_edu(edu) indexed_edu from data")
//    val accembler = new VectorAssembler().setInputCols(Array("age", "jobEncoder", "indexed_gender", "indexed_edu")).setOutputCol("features")
//    val training_df = accembler.transform(final_df)
//    training_df.show()
//    val kmeans=new KMeans().setK(2)
//    val model = kmeans.fit(training_df)
//    model.clusterCenters.foreach(println)

    spark.stop()
  }
}
