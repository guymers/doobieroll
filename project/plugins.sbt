addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.1.10")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.3.0")
// Need to publish locally to include a fix jcmd detection is
//addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.3.8-SNAPSHOT")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.3.7")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.3")
