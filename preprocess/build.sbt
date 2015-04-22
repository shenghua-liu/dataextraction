name := "subgraph Project"

version := "1.0"

scalaVersion := "2.10.4"

ideaExcludeFolders += ".idea"

ideaExcludeFolders += ".idea_modules"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" %"1.2.0" 

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"
