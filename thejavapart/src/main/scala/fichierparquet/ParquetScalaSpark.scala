package fichierparquet;

import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
class ParquetScalaSpark {


  def show_parquet_file(path: String, number_of_rows: Int): java.util.List[String] = {

    var result: List[String] = List.empty[String]
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val parquetFileDF = spark.read.parquet(path)

    val parquet_some_rows = parquetFileDF.take(number_of_rows).map(_.mkString(" , "))
    for (i <- 0 to number_of_rows - 1) {

      result = parquet_some_rows(i) :: result

    }

    result.asJava
  }
  def structure_parquet_file(path: String):String = {

   
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val parquetFileDF = spark.read.parquet(path)
    val file_schema = parquetFileDF.schema.simpleString

   

   file_schema 
  }
  def filterParquet(path: String, field: String, value: String): java.util.List[String] = {
    var result: List[String] = List.empty[String]
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    val parquetFileDF = spark.read.parquet(path)

    val filtered = parquetFileDF.filter(field + "='" + value + "'").take(20)
      .map(_.mkString(" , "))
      
    // parquetFileDF.createTempView("something")
    // val filtered = spark.sql("select * from something where " + field + " = '" + value + "'")
    //  .take(30).map(_.mkString(" , "))

    for (i <- 0 to filtered.length - 1) {

      result = filtered(i) :: result

    }

    result.asJava
  }

}