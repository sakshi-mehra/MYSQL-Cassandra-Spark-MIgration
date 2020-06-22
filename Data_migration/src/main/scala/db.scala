import java.sql.{DriverManager, ResultSet}
import java.util.UUID

import org.apache.spark.sql.functions.year
import org.apache.spark.sql.functions.{current_date, current_timestamp, date_format}
import org.apache.spark.sql.functions.to_date
import org.apache.spark._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
import org.apache.spark
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode, SparkSession}
//import _root_.sql.sqlcontext
import org.apache.spark.sql.functions.split

object db extends App{
  val conf = new SparkConf().set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext("local[2]", "MigrateMySQLToCassandra", conf)
  val sqlcontext= new SQLContext(sc)
  val mysqlJdbcString: String = s"jdbc:mysql://localhost:3307/seller_connect?user=sp_read&password=sp_read@123"
//  println(mysqlJdbcString)
 Class.forName("com.mysql.jdbc.Driver").newInstance


  //connecting to SQL
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  import spark.implicits._

  val order_items = sqlcontext.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3307/seller_connect").option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "order_items")
    .option("user", "sp_read")
    .option("password", "sp_read@123")
    .load()

//  order_items.printSchema()
  order_items.createOrReplaceTempView("order_items")
  val orders = sqlcontext.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3307/seller_connect").option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "orders")
    .option("user", "sp_read")
    .option("password", "sp_read@123")
    .load()
//  orders.printSchema()
  orders.createOrReplaceTempView("orders")
  val ordersDF = spark.sql("SELECT id,seller_id,purchase_date," +
    "city,country_code,created_at,currency_code,deleted_at," +
    "fulfillment_channel,is_business_order,is_premium_order,is_prime,is_replacement_order," +
    "last_update_date,marketplace_id,number_of_items_shipped,number_of_items_unshipped, " +
    "order_status,order_type,payment_method,postal_code,sales_channel," +
    "shipment_service_level_category,ship_service_level,state_or_region,updated_at" +
    " FROM orders where seller_id ='AYF9FF7SFXD1U'")
  val order_itemsDF = spark.sql("SELECT order_id,amazon_order_id,amount," +
    "quantity_ordered FROM order_items where seller_id ='AYF9FF7SFXD1U'")
  val sqlDF= ordersDF.join(order_itemsDF,ordersDF("id")===order_itemsDF("order_id"),"inner")
//  val sqlDF= spark.sql("SELECT orders.seller_id,orders.purchase_date,order_items.amazon_order_id," +
//  "order_items.amount,orders.city,orders.country_code,orders.created_at,orders.currency_code,orders.deleted_at," +
//  "orders.fulfillment_channel,orders.is_business_order,orders.is_premium_order,orders.is_prime,orders.is_replacement_order," +
//  "orders.last_update_date,orders.marketplace_id,orders.number_of_items_shipped, orders.number_of_items_unshipped," +
//  "orders.order_status,orders.order_type,orders.payment_method,orders.postal_code,order_items.quantity_ordered," +
//  "orders.sales_channel,orders.shipment_service_level_category,orders.ship_service_level,orders.state_or_region," +
//  "orders.updated_at" +
//  " FROM orders,order_items where order_items.order_id=orders.id and orders.seller_id ='AYF9FF7SFXD1U' ")


//  sqlDF.show()
//removing columns from DF
final val dropList = Seq("id","order_id","amount_gift_wrap_price", "currency_code_promotion_discount","currency_code_item_tax",
  "currency_code_shipping_discount","currency_code_shipping_tax","currency_code_gift_wrap_price","order_item_id",
  "quantity_shipped","currency_code_shipping_price","amount_shipping_price","amount_shipping_discount",
  "title","currency_code_gift_wrap_tax","amount_gift_wrap_tax","condition_id","amount_shipping_tax",
  "seller_sku","condition_subtype_id","amount_item_tax","amount_promotion_discount","asin")


  val filteredDF = sqlDF.select(sqlDF.columns .filter(colName => !dropList.contains(colName)) .map(colName => new Column(colName)): _*)

  filteredDF.createOrReplaceTempView("filteredDF")
//filteredDF.select(filteredDF.columns.foreach(x=> if(x.equals("purchase_date")){
//  spark.sql(select split(x,'-')[0] as purchase_year from x)
//})
//  val py=filteredDF.withColumn("purchase_year",$year(filteredDF.col("purchase_date").date_format)
//    spark.sql("select SPLIT(purchase_date,'-')[0] as purchase_year from filteredDF").select("purchase_year").collect())
//      .toDF("purchase_year")
//  filteredDF.withColumn("purchase_year", sc.parallelize(py["purchase_year"]))
//  filteredDF.col("purchase_date")
//  filteredDF.withColumn("purchase_year",
//val rdd=sc.parallelize(filteredDF("purchase_date"))
//  toDF(filteredDF).select("purchase_date",date_format("purchase_date", "yyyy MM dd"))
//print(filteredDF.withColumn("purchase_year", ())
//  filteredDF.show()

  filteredDF.withColumn("purchase_date",to_date($"purchase_date"))
//  println(year(filteredDF.select("purchase_date")))
  val finalDF=filteredDF.withColumn("purchase_year",year(filteredDF.col("purchase_date")))
finalDF.show()
  finalDF.printSchema()
  (finalDF.columns.foreach(x=> println(x)))




  //    val customerEvents = new JdbcRDD(sc, () => { DriverManager.getConnection(mysqlJdbcString)},
//    "select * from customer_events ce, staff, store where ce.store = store.store_name and ce.staff = staff.name " +
//      "and ce.id >= ? and ce.id <= ?", 1, 50, 1,
//    (r: ResultSet) => {
//      (r.getString("customer"),
//        r.getTimestamp("time"),
//        UUID.randomUUID(),
//        r.getString("event_type"),
//        r.getString("store_name"),
//        r.getString("location"),
//        r.getString("store_type"),
//        r.getString("staff"),
//        r.getString("job_title")
//      )
//    })
//println(customerEvents)
//  customerEvents.saveToCassandra("test", "customer_events",
//    SomeColumns("customer_id", "time", "id", "event_type", "store_name", "store_type", "store_location", "staff_name", "staff_title"))
  finalDF.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "orderss", "keyspace" -> "amazon_us"))
  .save()
  import org.apache.spark.sql._
//  finalDF.save("org.apache.spark.sql.cassandra",SaveMode.Overwrite,options = Map( "c_table" -> "test1", "keyspace" -> "yana_test"))
}








