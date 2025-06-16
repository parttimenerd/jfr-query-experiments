JFR Query Experiments
=====================

Experiments with `jfr` tool code and JFR queries.

It's essentially a standalone version of the `jfr view` command,
based, currently, on the [JDK 21](https://github.com/openjdk/jdk21u) code.

And it has a (highly experimental) web mode to easily test JFR queries:

```shell
java -jar target/query.jar web recording.jfr
-> starts a web server on port 8080
-> open http://localhost:8080
```

![Screenshot of the web UI](img/tool.png)

Build
-----

```shell
git clone https://github.com/parttimenerd/jfr-query-experiments
cd jfr-query-experiments
mvn clean package
```

Run
---
```shell
java -jar target/jfr-query-experiments.jar
-> shows the help
```

Purpose
-------
Play with JFR queries, maybe extend them, without having to build the whole JDK.

License
-------
GPL-2.0, Copyright 2017 - 2025 SAP SE or an SAP affiliate company and contributors.