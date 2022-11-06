package otus

import org.apache.log4j.{Level, Logger}


import scala.collection.immutable.ListMap

object BostonAnalytic extends  App with Context {
  override def main(args: Array[String]): Unit = {
    import spark.implicits._
    Logger.getLogger("org").setLevel(Level.OFF)

    val (inputFile1, inputFile2, outputFile) = (args(0), args(1), args(2))


    val offenseCodes = spark.read.option("header", "true").option("inferSchema", "true")
      .csv(inputFile1)

    val crimeFacts = spark.read.option("header", "true")
      .option("inferSchema", "true").csv(inputFile2)


    import org.apache.spark.sql.functions.broadcast
    val offenseCodesBroadcast = broadcast(offenseCodes)

    val crimeStatsWithBroadcast = crimeFacts
      .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")


    crimeStatsWithBroadcast.createOrReplaceTempView("robWithBro")
    val crimeTotal = spark.sql(
      """select DISTRICT, count(*)  as crimes_total, ceil( count(*)/12 ) as crimes_monthly,
        | avg(Lat) as  lat, avg(Long) as long
        | from robWithBro group by DISTRICT order by crimes_total desc""".stripMargin)


    val frequent_crime_types_sql = spark.sql(
      """select DISTRICT,  Name  from robWithBro """.stripMargin)

    val frequent_crime_types: Array[(String, String)] = frequent_crime_types_sql.rdd
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .map { tuple =>
        val distr: String = tuple._1.get(0) match {
          case null => "NULL"
          case _ => tuple._1.get(0).toString
        }
        val crime = tuple._1.get(1).toString.split("-").head
        val count = tuple._2
        (distr, (crime, count))
      }.groupByKey
      .mapValues { tuples =>
        val listmap: ListMap[String, Int] = ListMap(tuples.toMap.toSeq.sortWith(_._2 > _._2): _*)
        listmap.slice(0, 3).keysIterator.reduce((a, b) => a + ", " + b)
      }.collect()

    val frequent_crime_top = spark.createDataFrame(frequent_crime_types)
      .toDF("DISTRICT", "frequent_crime_types")

    val result = crimeTotal.join(frequent_crime_top, "DISTRICT")
      .write.parquet(outputFile)


    val res = spark.read.parquet(outputFile)
    res.show()


  }
}


