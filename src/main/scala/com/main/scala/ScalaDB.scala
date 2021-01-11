package com.main.scala

object ScalaDB {

  def connectURL(): java.sql.Connection = {
    val url = "jdbc:db2://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB"
    val username = "wfq59175"
    val password = "qtztcn5bn^f6rszp"

    var connection = java.sql.DriverManager.getConnection(url, username, password)
    connection
  }


  def main(args: Array[String]): Unit = {

    val con: java.sql.Connection = connectURL()

    con.setAutoCommit(false)

    var st = con.createStatement()

    var rs = st.executeQuery("show tables")

    while (rs.next()) {
      printf(rs.getString(1))
    }

    rs.close()

    st.close()
    //
    //    val conf = new SparkConf().setAppName("Spark SQL").setMaster("local")
    //    val sc = new SparkContext()
    //
    //    val sqlContext = new SQLContext(sc)
    //
    //    val spark = SparkSession
    //      .builder
    //      .master("local")
    //      .appName("Spark SQL")
    //      .config("spark.some.config.option", "some-value")
    //      .getOrCreate()
    //
    //    val db = spark
    //
    //    val dbMap = spark.sqlContext.read.format("csv").load.("jdbc", Map(
    //      "url" -> "jdbc:db2://dashdb-txn-sbox-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB:user=wfq59175:password=qtztcn5bn^f6rszp;",
    //      "driver" -> "com.ibm.cloud.sql.jdbc.Driver",
    //      "dbtable" -> "Employee"))
    //
    //    dbMap.show()
  }
}
