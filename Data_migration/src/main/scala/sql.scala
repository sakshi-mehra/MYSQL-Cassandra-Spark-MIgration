import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
//import sqlContext.implicits._
import org.apache.spark.sql.SparkSession
object sql extends App{

  import org.apache.spark

  val conf:SparkConf = new SparkConf().setAppName("Histogram").setMaster("local")
  val sc:SparkContext = new SparkContext(conf)
  val sqlcontext = new SQLContext(sc)
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
import spark.implicits._

  val jdbcDF = sqlcontext.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3307/seller_connect").option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "order_items")
    .option("user", "sp_read")
    .option("password", "sp_read@123")
    .load()
//  println(jdbcDF.rdd.partitions.length)
//  jdbcDF.write
//    .option("header", "true")
//    .option("delimiter", ",")
//    .format("csv")
//    .save("/home/sakshi")
//  println(jdbcDF.columns)
  jdbcDF.printSchema()
  jdbcDF.createOrReplaceTempView("orders")
  val sqlDF = spark.sql("SELECT * FROM orders where seller_id ='AYF9FF7SFXD1U'")
  sqlDF.show()
}
//.option("query", "select * from order_items where asin='B00IA704TW'")
//