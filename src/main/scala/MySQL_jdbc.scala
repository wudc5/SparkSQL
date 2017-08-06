import java.sql.DriverManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import java.util.Properties
import java.sql.{Connection, DriverManager, ResultSet};
/**
  * Created by wdc on 2017/4/18.
  */
object MySQL_jdbc {
  def main(args: Array[String]): Unit = {
    val conn_str = "jdbc:mysql://localhost:3306/lottery?user=root&password=123456&serverTimezone=UTC"

    // Load the driver
    classOf[com.mysql.jdbc.Driver]

    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs = statement.executeQuery("SELECT * FROM personas LIMIT 15")

      // Iterate Over ResultSet
      while (rs.next) {
        println(rs.getString("name"))
      }
    }
    finally {
      conn.close
    }
  }
}
