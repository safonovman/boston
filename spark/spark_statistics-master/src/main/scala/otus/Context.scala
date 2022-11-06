package otus

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkConf = new SparkConf()
    .setAppName("SparkBoston")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")

  val spark = { SparkSession
    .builder()
    .config(sparkConf)
    //      .config("spark.sql.autoBroadcastJoinThreshold", 0)
    //      .master("local[*]")
    .getOrCreate()}

}
