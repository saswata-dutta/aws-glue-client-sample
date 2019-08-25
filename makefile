clean:
	sbt clean


lint:
	sbt clean scapegoat scalafmtAll


build:
	sbt clean scapegoat scalafmtAll test assembly


publish: build
	sbt publish
