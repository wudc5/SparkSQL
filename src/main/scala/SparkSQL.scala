import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
  * Created by Administrator on 2017/4/15.
  */
object SparkSQL {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///").getOrCreate()  //非windows环境可以不用config
    val test_df = spark.read.option("header","true").csv(args(0))
    test_df.show()
    test_df.filter("id='1'").show()
    test_df.printSchema()
    val stringIndexer = new StringIndexer().setInputCol("gender").setOutputCol("genderIndexed").fit(test_df)
    val indexed_test_df = stringIndexer.transform(test_df)
    val oneHotEncoder = new OneHotEncoder().setInputCol("genderIndexed").setOutputCol("genderEncoder")
    oneHotEncoder.transform(indexed_test_df).show()

    val schema = StructType(Array(StructField("lat", DoubleType,nullable = true),
      StructField("lon", DoubleType, nullable=true)))
    val location_df = spark.read.option("header","true").schema(schema).csv(args(1))
    location_df.show()
    location_df.printSchema()
    val accembler = new VectorAssembler().setInputCols(Array("lat","lon")).setOutputCol("features")
    val training_df = accembler.transform(location_df)
    training_df.show()
    val kmeans=new KMeans().setK(2)
    val model = kmeans.fit(training_df)
    model.clusterCenters.foreach(println)
    spark.stop()
  }

}
