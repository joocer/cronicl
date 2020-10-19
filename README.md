# cronicl: data processing pipelines.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/joocer/danvers/blob/master/LICENSE)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_cronicl&metric=security_rating)](https://sonarcloud.io/dashboard?id=joocer_cronicl)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=joocer_cronicl&metric=bugs)](https://sonarcloud.io/dashboard?id=joocer_cronicl)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_cronicl&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=joocer_cronicl)

**cronicl** is a simple data processing pipeline tool written in Python.

No additional infrastructure needed, if you can run Python, you can process data through cronicl.

Pipelines can be short-lived (e.g. to process a file) or long-lived (e.g. to process streaming input).

## Features
-  Programatically define pipeline flow and processing steps
-  Record the number of times each step is executed and execution time
-  Automatic version tracking of processing steps
-  Trace messages through the pipeline (random samples)
-  Monitor via HTTP interface 

## Dependencies

[NetworkX](https://networkx.org/)  
[Flask](https://flask.palletsprojects.com/)

## License
[Apache 2.0](LICENSE)


