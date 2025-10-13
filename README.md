JFR Query Experiments
=====================

Working on JFR files using SQL. Essentially transforming JFR files into a DuckDB database
and then using [DuckDB](https://duckdb.org/) to query it, with support for all JFR views.

Previously, we tried to use the JFR query language directly, but it is quite limited.

The purpose of this project is to ease the pain of exploring JFR files and finding interesting
patterns in them.

Build
-----
```shell
mvn clean package
```

Main Usage
----------

Transform a JFR file into a DuckDB database file:

```shell
> java -jar target/query.jar duckdb import jfr_files/recording.jfr duckdb.db
> duckdb duckdb.db "SELECT * FROM Events";
┌───────────────────────────────┬───────┐
│             name              │ count │
│            varchar            │ int32 │
├───────────────────────────────┼───────┤
│ GCPhaseParallel               │ 69426 │
│ ObjectAllocationSample        │  6273 │
```

Use `duckdb -ui duckdb.db` to get a web-based UI to explore the database.

Directly query a JFR file (implicitly creating a DuckDB file, disable via `--no-cache`):

```
> java -jar target/query.jar query jfr_files/metal.jfr "hot-methods" 
╔═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╤═════════╤═════════╗
║ Method                                                                                                              │ Samples │ Percent ║
╠═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╪═════════╪═════════╣
║ java.util.concurrent.ForkJoinPool.deactivate(ForkJoinPool.WorkQueue, int)                                           │ 1066    │ 8.09%   ║
╟─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼─────────┼─────────╢
║ scala.collection.immutable.RedBlackTree$.lookup(RedBlackTree.Tree, Object, Ordering)                                │ 695     │ 5.27%   ║
```

View names are directly replaced by `SELECT * FROM <view name>`, so `hot-methods` is
`SELECT * FROM "Hot Methods"`.

The full list of views is available via the `views` command the full list of macros `macros`.

Limitations:
- Stack traces are stored a fixed size (10 frames by default) and only have methods in their frames
   - so no line number, bytecode index or the type of the frame
   - this saves a lot of space and makes queries faster
- Only basic support for JFR specific datatypes, as we have to map them to DuckDB types



TODO
- performance comparison with Calcite based Gunnar Morling stuff and other tools (like jfr)
- better table printer
- support for annotating data types
   - maybe also a simple data flow analysis to track types and find the actual types of columns 
     (like memory or duration)
- write basic blog post

License
-------
GPL-2.0, Copyright 2017 - 2025 SAP SE or an SAP affiliate company and contributors.