name := "Data_migration"

version := "0.1"

scalaVersion := "2.11.11"
//
//libraryDependencies ++= Seq(
//  "org.apache.spark" % "spark-core" % "2.3.2",
//  "org.apache.spark" % "spark-sql" % "2.3.2"
//)
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.5.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies +="mysql" % "mysql-connector-java" % "5.1.12"
libraryDependencies +="com.databricks" %% "spark-csv" % "1.3.0"
//libraryDependencies +="com.datastax.spark" % "spark-cassandra-connector" % "2.3.2"
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"
