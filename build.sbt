name := "SparkIntro"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.4.0"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" classifier "models"

libraryDependencies += "edu.stanford.nlp" % "stanford-parser" % "3.5.2"

libraryDependencies += "com.opencsv" % "opencsv" % "3.3"