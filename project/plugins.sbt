resolvers ++= Seq(Resolver.typesafeRepo("releases"),
  "Artima Maven Repository" at "https://repo.artima.com/releases"
)


addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.0")
addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat" % "1.0.9")
addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.7")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.2")
addSbtPlugin("org.wartremover" % "sbt-wartremover-contrib" % "1.3.1")

addSbtPlugin("com.updateimpact" % "updateimpact-sbt-plugin" % "2.1.3")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
