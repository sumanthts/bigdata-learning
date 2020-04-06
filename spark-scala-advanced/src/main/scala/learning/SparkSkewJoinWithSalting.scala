package learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Optimize skew join in Spark using Salting technique
  */
object SparkSkewJoinWithSalting {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val usersDF = Seq(("user1"), ("user2"), ("user3")).toDF("id")
    val ordersDF = Seq((1L, "user1"), (2L, "user2"), (3L, "user3"), (4L, "user1"), (5L, "user1")
      , (6L, "user1"), (7L, "user1")).toDF("order_id", "user_id")

    // 1) Take a bigger dataset and add a column with some randomness
    //    Here I'm simply adding a number between 0 and 2
    val ordersWithSaltedCol = ordersDF.withColumn("order_join_key", concat($"user_id",
      floor(rand(seed = 2) * 2))
    )

    //ordersWithSaltedCol.printSchema()
    ordersWithSaltedCol.show()

    // 2) Later add a new column to cover all random possibilities.
    //    Create one line for each possibility. Here I'm using explode function.
    val usersWithSaltedCol = usersDF.withColumn("salt", lit((0 to 2).toArray))
      //array(lit(0), lit(1), lit(2))) or typedLit((0 to 2).toArray)
      .withColumn("user_salt", explode($"salt"))
      .withColumn("user_join_key", concat($"id", $"user_salt"))

    usersWithSaltedCol.show()

    // 3) Make a join with the salted column
    val result = usersWithSaltedCol.join(ordersWithSaltedCol, $"user_join_key" === $"order_join_key")

    result.show()
  }

}
