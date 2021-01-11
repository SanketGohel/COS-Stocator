package com.main.scala

import com.main.scala.ScalaDB.connectURL
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DisplayEmployee {

//  def connectURL(): java.sql.Connection = {
//    val url = "jdbc:db2://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB"
//    val username = "wfq59175"
//    val password = "qtztcn5bn^f6rszp"
//
//    var connection = java.sql.DriverManager.getConnection(url, username, password)
//    Console.print("HI")
//    connection
//  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("Spark SQL")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.sqlContext.read.format("csv").option("header", "true").load("cos://candidate-exercise.myCos/emp-data.csv")

    df.show(15)

//    val con: java.sql.Connection = connectURL()

//    con.setAutoCommit(false)

//    var st = con.createStatement()
//    var rs = st.executeQuery(CREATE_TABLE)

//    st.executeQuery("insert into pop_data values('1','2','3','4','5','6')")


//    df.foreach{row =>
//      var rs = st.executeQuery("insert into pop_data values()")
//    }


//    while (rs.next()) {
//      printf(rs.getString(1))
//    }

//    rs.close()
//
//    st.close()

    val dbMap = spark.read.format("jdbc").option("url", "jdbc:db2://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB:user=wfq59175:password=qtztcn5bn^f6rszp;").option("dbtable", "emp_data")load()

    dbMap.show()

    var CREATE_TABLE = "create table if not exists pop_data (" +
      "Name varchar(255)," +
      "Gender varchar(60)," +
      "Department varchar(255)," +
      "Salary varchar(255)," +
      "Loc varchar(255)," +
      "Rating varchar(128))"

    Console.print("Done!!!")

  }

}
