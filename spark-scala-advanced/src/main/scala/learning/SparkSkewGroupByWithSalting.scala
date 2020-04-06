package learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Optimize aggregation with skew data in Spark
  * Well, it does, to mitigate this you could run group by operation twice.
  * Firstly with salted key, then remove salting and group again.
  * The second grouping will take partially aggregated data,
  * thus significantly reduce skew impact.
  */
object SparkSkewGroupByWithSalting {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val custDF = Seq(("cust1",1,200.0),("cust2",1,200.0),("cust3",2,700.0),
      ("cust1",2,700.0),("cust4",3,550.0),("cust1",4,880.0),("cust1",3,550.0),
      ("cust1",5,270.5),("cust5",1,200.0),("cust1",6,875.0),("cust6",5,270.5))
      .toDF("custid","itemid","price")

    val saltedDF = custDF.withColumn("salt_key", (rand()*5).cast("int"))
    saltedDF.show

    // apply first group by with salted key
    // then again apply group by removing the salted key
    val resultDF = saltedDF.groupBy("salt_key","custid")
      .agg(sum("price").as("price"))
      .groupBy("custid")
      .agg(sum("price"))

    resultDF.show
  }

}
