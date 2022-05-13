scalaVersion := "2.12.8"

name := "minhash"
scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")

//libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "2.4.3")
)
