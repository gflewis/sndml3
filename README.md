The ServiceNow Data Mart Loader is a Java command-line application which uses the ServiceNow REST API to extract meta-data and data from ServiceNow. The Loader creates tables in an SQL database based on meta-data extracted from ServiceNow. It supports a variety of load and synchronization operation. 

Version 3 is essentially a complete rebuild of the application. The following are significant changes since version 2.
* The SOAP API has been replaced with the REST API and the JSONv2 API for improved performance.
* The version 2 "script" syntax has been abandoned and replaced with a simpler YAML syntax.
* Version 3 includes a `partition` option for improved reliability when backloading large task based tables.
* Version 3 includes a `metrics` file option to enable incremental loads since the last run.
* Version 3 includes a `sync` action which compares `sys_updated_on` to determine which records need to be updated, inserted or deleted.
* While the `dialect` option is still supported, it is no longer required. By default the code will select a dialect from the templates file based on the JDBC URL.

This program is freely distributed software. You are welcome to redistribute and/or modify it. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY, explicit or implied. 

For a quick tutorial, please see https://github.com/gflewis/sndml3/wiki/Getting-Started.

For additional information please refer to the wiki: https://github.com/gflewis/sndml3/wiki.
