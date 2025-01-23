---
title: ServiceNow DataPump 3.5
description: Exporting ServiceNow data to Oracle, SQL Server, MySQL or PostgreSQL with version 3.5 of SNDML and the DataPump App
---

DataPump is a contributed application which can be used to export ServiceNow data to 
Oracle, Microsoft SQL Server, MySQL or PostgreSQL. This application has two parts:

* A Java application (SNDML _a.k.a._ Java Agent) which runs the exports. 
  This application is executed on a Linux or Windows server.
* A scoped ServiceNow app (**x_108443_sndml**) which is installed in the ServiceNow instance.
  This application is used to configure the agent and manage the export jobs.

Both parts can be downloaded from 
[https://github.com/gflewis/sndml3/releases](https://github.com/gflewis/sndml3/releases).

Beginning with Release 3.5.0.10, the **Assets** section of the release will contain 
a ZIP file with the following:
* **DataPump-v3.5.x.x-Install.xml** - _Update Set to install the ServiceNow app_
* **sndml-3.5.x.x-mssql.jar** - _JAR file for use with Microsoft SQL Server_
* **sndml-3.5.x.x-mysql.jar** - _JAR file for use with MySQL_
* **sndml-3.5.x.x-ora.jar** - _JAR file for use with Oracle_
* **sndml-3.5.x.x-pg.jar** - _JAR file for use with PostgreSQL_

For instructions, please refer to the following pages
* [Getting Started](getting_started)
* [Scheduling Exports](scheduling_exports)
* [Optimizing Exports](optimizing_exports)

## [Getting Started](getting_started)
* [Create Users and Grant Roles](getting_started#create_users_and_grant_roles)
* [Create a Connection Profile](getting_started#create_a_connection_profile)
* [Test Connectivity](getting_started#test_connectivity)
* [Create a Database Agent Record](getting_started#create_a_database_agent_record)