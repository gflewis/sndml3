---
title: Getting started with DataPump
description: Exporting ServiceNow data to Oracle, SQL Server, MySQL or PostgreSQL with version 3.5 of SNDML and the DataPump App
---
## Introduction

DataPump is a contributed application which can be used to export ServiceNow data to 
Oracle, Microsoft SQL Server, MySQL or PostgreSQL. This application has two parts:

* A Java application (Java agent) which runs the exports. 
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

## Create Users and Grant Roles

After installing the Update Set in your instance, 
the first step is to create two new ServiceNow users 
which will be used by the Java agent.

### datapump.agent
This account will be used to retrieve configuration information from the DataPump scoped app 
and to update the status of running jobs.
* Set **Time zone** to **GMT**
* Set **Web service access only** to **true**
* Grant **x_108443_sndml.daemon** role
* Assign the user a secure password which will be entered int the Connection Profile below

### datapump.reader
This account will be used to export data from the instance.
It requires "read" access to any tables which will be exported.
* Set **Time zone** to **GMT**
* Set **Web service access only** to **true**
* Grant **snc_read_only** role
* Grant **soap_query** role
* Grant **itil** role and/or any roles necessary to read the requisite tables.
* Assign the user a secure password which will be entered int the Connection Profile below

Do not grant  **x_108443_sndml.admin** role to either of these service accounts.
Users with **x_108443_sndml.admin** role can configure and monitor DataPump jobs.

## Create a Connection Profile

The **Connection Profile** is a Java properties file that contains 
credentials for the database and the ServiceNow instance, 
as well as other parameters that affect processing. 
The Connection Profile looks like this:

```
database.url=jdbc:mysql://name-of-database-host/dbname
database.schema=******
database.username=******
database.password=******

app.instance=dev000000
app.username=datapump.agent
app.password=******

app.agent=main

reader.instance=dev000000
reader.username=datapump.reader
reader.password=******
```

Since the **Connection Profile** contains passwords, 
it should be in a secured location on your Linux or Windows server.

The format of **database.url** will vary based on whether you are using 
MySQL, PostgreSQL, Oracle or Microsoft SQL Server. 
Please refer to the documentation on configuring a JDBC URL based on the type of your database.

The values of **appinstance** and **reader.instance** can either be a full URL (starting with `https://`)
or an instance name.

The value of **app.agent** must match the name used in the **Database Agent** record below.

## Test Connectivity

Before configuring the Agent, it is a good idea to run a quick connectivity test.
This will verify that the profile contain valid credentials,
and that the Java program can write to target database schema.

For this test, you should choose a table that has some data, but is not too large.
Good tables for this test might include 
**cmdb_ci_service** or **cmn_location**.

Using the appropriate JAR file, type the following command:

    java -ea -jar <jarfilename> -p <profilename> -t <tablename>

The Java program should connect to ServiceNow and to the database,
create a table in the database schema,
and copy the ServiceNow data into the target table.

If you run the command a second time, the the `CREATE TABLE` will be missing.
When SNDML starts a job, 
it first check to see if the target table exists in the schema.
The program will issue a `CREATE TABLE` statement only if
there is no existing table with the correct name in the target schema.

If everything works successfully, then we can begin to configure the agent.

## Create a Database Agent Record

In your ServiceNow instance, go to **DataPump > Agents** and click **New**. 
Create a new Database Agent record with the name "main".

## Configure a Database Table and a Job

For the first test of the agent, you should again choose a ServiceNow table 
which has a small number of records.

1. Go to **DataPump > Agents**.
2. Open the "main" agent configured above.
3. Click the **New** button above the **Tables** related list.
4. Select a **Source table**.
5. **Save** the record.
6. Click the **New** button above the **Jobs** related list.
7. For **Action type** select "Insert".
8. **Save** the record.
9. Click the **Execute Now** button.

This newly created **Job Run** record has a status of **Ready**.
It is waiting to be executed by the Java agent.

## Run an SNDML Scan

On your Linux or Windows server, type this command:

    java -ea -jar <jarfilename> -p <profilename> --scan

The `--scan` command looks for any **Job Run** records that are **Ready**,
and executes them.

As the job executes, the **Job Run** record will be updated,
and rows will be appended to the **Job Run Logs** related list.

When each job completes, `--scan` checks for new **Job Run** records that are **Ready**.
If none are found then the Java program terminates.

## Run an SNDML Daemon

To scan for jobs in an infinite loop, execute SNDML in **daemon** mode using this command:

    java -ea -jar <jarfilename> -p <profilename> --daemon
    
By default, `--daemon` will scan for new jobs every 2 minutes. 
This can be changed by setting the value of the **Connection Profile** property `daemon.interval_seconds`.

## Action Types
There are several types of jobs.

### Insert
"Insert" is used for initial loading or reloading of SQL tables. 
It inserts rows into the target table. 
If a record with the same sys_id already exists in the target table, 
then a primary key violation will occur and the row will be skipped.

If Truncate is checked, then the SQL table will be truncated prior to the load.

### Upsert
"Upsert" is used to load or update SQL tables. 
If the target record exists (based on **sys_id**), then it will be updated. 
Otherwise, it will be inserted.

If **Since Last** is checked, then only records inserted or updated in ServiceNow since the last run 
will be processed. The following filter will be used when retrieving records from ServiceNow:
<blockquote><code>sys_updated_on>=</code><i><b><small>lastrunstart</small></b></i></blockquote>
where 
<i><b><small>lastrunstart</small></b></i>
is determined from the "Last Run Start" field on the Database Table record.

### Sync
"Sync" compares the timestamps (`sys_updated_on`) in the source and target tables. 
Based on this comparison it will insert, update or delete target records. 
If the values of sys_updated_on match, then the record will be skipped.

If a Filter has been configured for the Database Table, 
the Sync will delete any records which do not match the filter.

### Execute
"Execute" executes an arbitrary SQL statement. This is typically used to run a database stored procedure.

