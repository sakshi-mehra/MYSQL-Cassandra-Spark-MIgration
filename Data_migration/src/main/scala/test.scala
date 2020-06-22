import db.filteredDF
import org.apache.spark.sql.types.TimestampType

object test {
  filteredDF.withColumn("purchase_year",filteredDF.col("purchase_date").cast(TimestampType))

}
