package learning

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSCDType2App {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master("local")
      .getOrCreate()

    // read the new source data
    val df_source = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/source_data.csv")
    df_source.show()
//	+------+--------------+
//	|src_id|      src_attr|
//	+------+--------------+
//	|     1|  Hello World!|
//	|     2|Hello PySpark!|
//	|     4|  Hello Scala!|
//	+------+--------------+


    // read the existing target table data
    val df_target = spark.read
      .parquet("src/main/resources/target_table")
    df_target.show()
//	+---+----------------+----------+----------+--------------+-----------+
//	| id|            attr|is_current|is_deleted|effective_date|expiry_date|
//	+---+----------------+----------+----------+--------------+-----------+
//	|  1|          Hello!|     false|     false|    2018-01-01| 2018-12-31|
//	|  1|    Hello World!|      true|     false|    2019-01-01| 9999-12-31|
//	|  2|    Hello Spark!|      true|     false|    2019-02-01| 9999-12-31|
//	|  3|Hello Old World!|      true|     false|    2019-02-01| 9999-12-31|
//	+---+----------------+----------+----------+--------------+-----------+

	
    //val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //val max_date = dateFormat.parse("9999-12-31")
    //val current_date = dateFormat.format(java.util.Date)

    // prepare source for merge by adding effective and expiry dates
    val df_source_new = df_source.withColumn("src_effec_date", date_format(current_date(),"yyyy-MM-dd"))
      .withColumn("src_exp_date", date_format(lit("9999-12-31"), "yyyy-MM-dd"))
//	+------+--------------+--------------+------------+
//	|src_id|      src_attr|src_effec_date|src_exp_date|
//	+------+--------------+--------------+------------+
//	|     1|  Hello World!|    2020-01-27|  9999-12-31|
//	|     2|Hello PySpark!|    2020-01-27|  9999-12-31|
//	|     4|  Hello Scala!|    2020-01-27|  9999-12-31|
//	+------+--------------+--------------+------------+
	  
	  
    // FULL Merge, join on key column and also expiry date column to make only join to the latest records
    val df_merge = df_target.join(df_source_new, (df_source_new("src_id") === df_target("id")) &&
      (df_source_new("src_exp_date") === df_target("expiry_date")), "outer")
//	+----+----------------+----------+----------+--------------+-----------+------+--------------+--------------+------------+
//	|  id|            attr|is_current|is_deleted|effective_date|expiry_date|src_id|      src_attr|src_effec_date|src_exp_date|
//	+----+----------------+----------+----------+--------------+-----------+------+--------------+--------------+------------+
//	|   1|          Hello!|     false|     false|    2018-01-01| 2018-12-31|  null|          null|          null|        null|
//	|   1|    Hello World!|      true|     false|    2019-01-01| 9999-12-31|     1|  Hello World!|    2020-01-27|  9999-12-31|
//	|   3|Hello Old World!|      true|     false|    2019-02-01| 9999-12-31|  null|          null|          null|        null|
//	|null|            null|      null|      null|          null|       null|     4|  Hello Scala!|    2020-01-27|  9999-12-31|
//	|   2|    Hello Spark!|      true|     false|    2019-02-01| 9999-12-31|     2|Hello PySpark!|    2020-01-27|  9999-12-31|
//	+----+----------------+----------+----------+--------------+-----------+------+--------------+--------------+------------+
	  
	  
    // Derive new column to indicate the action
    val df_merge1 = df_merge.withColumn("action", when(df_merge("attr") =!= df_merge("src_attr"), "UPDATE")
      .when(df_merge("src_id").isNull && df_merge("is_current"), "DELETE")
      .when(df_merge("id").isNull, "INSERT")
      .otherwise("NOACTION"))
//	+----+----------------+----------+----------+--------------+-----------+------+--------------+--------------+------------+--------+
//	|  id|            attr|is_current|is_deleted|effective_date|expiry_date|src_id|      src_attr|src_effec_date|src_exp_date|  action|
//	+----+----------------+----------+----------+--------------+-----------+------+--------------+--------------+------------+--------+
//	|   1|          Hello!|     false|     false|    2018-01-01| 2018-12-31|  null|          null|          null|        null|NOACTION|
//	|   1|    Hello World!|      true|     false|    2019-01-01| 9999-12-31|     1|  Hello World!|    2020-01-27|  9999-12-31|NOACTION|
//	|   3|Hello Old World!|      true|     false|    2019-02-01| 9999-12-31|  null|          null|          null|        null|  DELETE|
//	|null|            null|      null|      null|          null|       null|     4|  Hello Scala!|    2020-01-27|  9999-12-31|  INSERT|
//	|   2|    Hello Spark!|      true|     false|    2019-02-01| 9999-12-31|     2|Hello PySpark!|    2020-01-27|  9999-12-31|  UPDATE|
//	+----+----------------+----------+----------+--------------+-----------+------+--------------+--------------+------------+--------+


    // For records that contain no action
    val df_merge_p1 = df_merge1.where("action = 'NOACTION'").select((df_target.columns.map(c => col(c))):_*)
//	+---+------------+----------+----------+--------------+-----------+
//	| id|        attr|is_current|is_deleted|effective_date|expiry_date|
//	+---+------------+----------+----------+--------------+-----------+
//	|  1|      Hello!|     false|     false|    2018-01-01| 2018-12-31|
//	|  1|Hello World!|      true|     false|    2019-01-01| 9999-12-31|
//	+---+------------+----------+----------+--------------+-----------+


    // For records that needs insert only
    val df_merge_p2 = df_merge1.where("action = 'INSERT'")
      .select(col("src_id").as("id"),col("src_attr").as("attr"),lit(true).as("is_current")
      ,lit(false).as("is_deleted"),col("src_effec_date").as("effective_date")
      ,col("src_exp_date").as("expiry_date"))
//	+---+------------+----------+----------+--------------+-----------+
//	| id|        attr|is_current|is_deleted|effective_date|expiry_date|
//	+---+------------+----------+----------+--------------+-----------+
//	|  4|Hello Scala!|      true|     false|    2020-01-27| 9999-12-31|
//	+---+------------+----------+----------+--------------+-----------+


    // For records that needs to be deleted
    val df_merge_p3 = df_merge1.where("action = 'DELETE'").select((df_target.columns.map(c => col(c))):_*)
      .withColumn("is_current", lit(false))
      .withColumn("is_deleted", lit(true))
//	+---+----------------+----------+----------+--------------+-----------+
//	| id|            attr|is_current|is_deleted|effective_date|expiry_date|
//	+---+----------------+----------+----------+--------------+-----------+
//	|  3|Hello Old World!|     false|      true|    2019-02-01| 9999-12-31|
//	+---+----------------+----------+----------+--------------+-----------+


    // For records that needs to be expired and then inserted
    val df_merge_p4_1 = df_merge1.where("action = 'UPDATE'").select(col("src_id").as("id")
      ,col("src_attr").as("attr"),lit(true).as("is_current"),lit(false).as("is_deleted")
      ,col("src_effec_date").as("effective_date"),col("src_exp_date").as("expiry_date"))
//	+---+--------------+----------+----------+--------------+-----------+
//	| id|          attr|is_current|is_deleted|effective_date|expiry_date|
//	+---+--------------+----------+----------+--------------+-----------+
//	|  2|Hello PySpark!|      true|     false|    2020-01-27| 9999-12-31|
//	+---+--------------+----------+----------+--------------+-----------+


    val df_merge_p4_2 = df_merge1.where("action = 'UPDATE'").select(col("id")
      ,col("attr"),lit(false).as("is_current"),lit(false).as("is_deleted")
      ,col("effective_date"),date_sub(col("src_effec_date"),1).as("expiry_date"))
//	+---+------------+----------+----------+--------------+-----------+
//	| id|        attr|is_current|is_deleted|effective_date|expiry_date|
//	+---+------------+----------+----------+--------------+-----------+
//	|  2|Hello Spark!|     false|     false|    2019-02-01| 2020-01-26|
//	+---+------------+----------+----------+--------------+-----------+


    val df_merge_final = df_merge_p1.union(df_merge_p2)
      .union(df_merge_p3)
      .union(df_merge_p4_1)
      .union(df_merge_p4_2)

    df_merge_final.printSchema()
//	root
//	 |-- id: integer (nullable = true)
//	 |-- attr: string (nullable = true)
//	 |-- is_current: boolean (nullable = true)
//	 |-- is_deleted: boolean (nullable = true)
//	 |-- effective_date: string (nullable = true)
//	 |-- expiry_date: string (nullable = true)
 
 
    df_merge_final.show
//	+---+----------------+----------+----------+--------------+-----------+
//	| id|            attr|is_current|is_deleted|effective_date|expiry_date|
//	+---+----------------+----------+----------+--------------+-----------+
//	|  1|          Hello!|     false|     false|    2018-01-01| 2018-12-31|
//	|  1|    Hello World!|      true|     false|    2019-01-01| 9999-12-31|
//	|  4|    Hello Scala!|      true|     false|    2020-01-27| 9999-12-31|
//	|  3|Hello Old World!|     false|      true|    2019-02-01| 9999-12-31|
//	|  2|  Hello PySpark!|      true|     false|    2020-01-27| 9999-12-31|
//	|  2|    Hello Spark!|     false|     false|    2019-02-01| 2020-01-26|
//	+---+----------------+----------+----------+--------------+-----------+

  }

}
