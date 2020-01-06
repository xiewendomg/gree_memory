package cn.gl

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.EsSparkSQL

object SparkSqlToEsTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Spark_hive")
    conf.setMaster("local[*]")
    conf.set("es.nodes", "192.168.88.151")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes.wan.only", "true")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    //val userTest: DataFrame = hiveContext.sql("select * from default.test_user where id != ''  ")
    val testUser2: DataFrame = hiveContext.sql("select * from default.test_user2 where id != '' ")
    //testUser2.show()
    //userTest.show()

    //直接插入
    // EsSparkSQL.saveToEs(userTest,"bjqtest/doc")

    //指定ID插入   id不能为null
    EsSparkSQL.saveToEs(testUser2,"bjqtest2/doc",Map("es.mapping.id"->"id"))

    sc.stop()

  }

}
