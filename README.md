# cronicl: data pipelines.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/joocer/danvers/blob/master/LICENSE)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_cronicl&metric=security_rating)](https://sonarcloud.io/dashboard?id=joocer_cronicl)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=joocer_cronicl&metric=bugs)](https://sonarcloud.io/dashboard?id=joocer_cronicl)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=joocer_cronicl&metric=sqale_rating)](https://sonarcloud.io/dashboard?id=joocer_cronicl)
[![CircleCI](https://circleci.com/gh/joocer/cronicl.svg?style=shield)](https://circleci.com/gh/joocer/cronicl)
[![PyPI Latest Release](https://img.shields.io/pypi/v/cronicl.svg)](https://pypi.org/project/cronicl/)
[![Known Vulnerabilities](https://snyk.io/test/github/joocer/cronicl/badge.svg?targetFile=requirements.txt)](https://snyk.io/test/github/joocer/cronicl?targetFile=requirements.txt)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**cronicl** is a simple data processing library written in Python.

No additional infrastructure or tools needed, if you can run Python, you can process data through cronicl.

Programatically author, connect, monitor and run workflows in Python.

## Features
-  Programatically define pipeline flow and processing operations
-  Record the number of times each operation is executed and execution time
-  Automatic version tracking of processing operations
-  Trace messages through the pipeline (random sampling), including santization of sensitive information from trace logs
-  Monitor via HTTP interface 
-  Automatic retry of operations

## Install

From PyPi (recommended)
~~~
pip install cronicl
~~~
From GitHub
~~~
pip install https://github.com/joocer/cronicl/releases/download/v0.1.0/cronicl-0.1.0-py3-none-any.whl
~~~

## Dependencies

[NetworkX](https://networkx.org/)  
[Flask](https://flask.palletsprojects.com/)

## License
[Apache 2.0](LICENSE)


