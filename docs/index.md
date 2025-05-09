---
title: Getting Started with ServiceNow DataPump
description: Exporting ServiceNow data to Oracle, SQL Server, MySQL or PostgreSQL with version 3.5 of SNDML and the DataPump App
layout: index
---

**SNDML** is a Java application which exports ServiceNow data to
Oracle, Microsoft SQL Server, MySQL or PostgreSQL.
**SNDML** is run on a Linux or Windows server.

**DataPump** is a scoped ServiceNow app which is installed in the ServiceNow instance
and is used to configure and manage SNDML jobs.

**SNDML** and **DataPump** are contributed software which can be downloaded from GitHub.
This page contains instructions for installing and configuring **DataPump** and **SNDML 3.5**.

## Contents
* [Downloading](#downloading)
* [Create Users and Grant Roles](#create-users-and-grant-roles)
* [Create a Connection Profile](#create-a-connection-profile)
* [Test Connectivity](#test-connectivity)
* [Create a Database Agent Record](#create-a-database-agent-record)
* [Configure a Table and a Job](#configure-a-table-and-a-job)
* [Run an SNDML Scan](#run-an-sndml-scan)
* [Methods for Running Jobs](#methods-for-running-jobs)
* [Creating Schedules](#creating-schedules)
* [Synchronized Scanning](#synchronized-scanning)
* [Run SNDML as a Daemon](#run-sndml-as-a-daemon)
* [Run SNDML as an HTTP Server](#run-sndml-as-an-http-server)
* [Shut down Daemon or Server](#shut-down-daemon-or-server)
* [Logging](#logging)
* [Job Action Types](#job-action-types)
* [Optimizing Exports](#optimizing-exports)
* [Partitioned Load](#partitioned-load)
* [Feedback and Support](#feedback-and-support)

## Downloading

JAR files for **SNDML** and the installation Update Set for **DataPump** can be downloaded from
* [https://github.com/gflewis/sndml3/releases](https://github.com/gflewis/sndml3/releases)

When you unpack the ZIP file (**sndml-3.5.n.n.zip**) you should find these files:
* **DataPump-v3.5.n-Install.xml** - _Update Set to install or upgrade the ServiceNow app_
* **sndml-3.5.n.n-mssql.jar** - _JAR file for use with Microsoft SQL Server_
* **sndml-3.5.n.n-mysql.jar** - _JAR file for use with MySQL_
* **sndml-3.5.n.n-ora.jar** - _JAR file for use with Oracle_
* **sndml-3.5.n.n-pg.jar** - _JAR file for use with PostgreSQL_

(Note that for some releases the Update Set version may not exactly match the release number.)
The **Update Set** should be installed in your ServiceNow instance.
If you have installed an earlier version of **DataPump**
(including v1.1 from the ServiceNow Share site)
then you should be able to use the **Install.xml**
Update Set to upgrade to the latest version.
Please test the upgrade using a non-production instance of ServiceNow.

The appropriate JAR file (based on your database)
should be copied to the Linux or Windows server that will be running the jobs.

## Create Users and Grant Roles

After installing the Update Set in your ServiceNow instance, 
the first step is to create two new users 
for use by by the Java agent.

### datapump.agent
This user will be used to retrieve configuration information from the DataPump scoped app 
and to update the status of running jobs.
* Set **Time zone** to **GMT**
* Set **Web service access only** to **true**
* Grant **x_108443_sndml.daemon** role
* Assign the user a secure password which will be entered in the **Connection Profile** below

### datapump.reader
This user will be used to export data from the instance.
It requires "read" access to any tables which will be exported.
* Set **Time zone** to **GMT**
* Set **Web service access only** to **true**
* Grant **snc_read_only** role
* Grant **soap_query** role
* Grant **itil** role and/or any roles necessary to read the requisite tables.
* Assign the user a secure password which will be entered in the **Connection Profile** below

Do not grant **x_108443_sndml.admin** role to either of these users.

Users with **x_108443_sndml.admin** role can configure and monitor DataPump jobs.

## Create a Connection Profile

The **Connection Profile** is a Java properties file that contains 
credentials for the database and the ServiceNow instance, 
as well as other parameters that affect processing. 
The name of the Connection Profile is passed to the Java program
using the `-p` or <code>&#8209;&#8209;profile</code>  command line option.
The Connection Profile looks like this:

```
database.url=jdbc:mysql://<database-host>/<dbname>
database.schema=<schemaname>
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
it should be in a protected location on your Linux or Windows server
and secured using permissions or ACLs.

The format of `database.url` will vary based on whether you are using 
MySQL, PostgreSQL, Oracle or Microsoft SQL Server. 
Please refer to the documentation on configuring a JDBC URL based on the type of your database.

The values of `app.instance` and `reader.instance` can either be a full URL (starting with `https://`)
or an instance name.

The value of `app.agent` must match the name used in the **Database Agent** record below.

For more detail on the Connection Profile refer to
[Connection Profile 3.5](https://github.com/gflewis/sndml3/wiki/Connection-Profile-3.5)

## Test Connectivity

Before configuring the Agent, it is a good idea to run a quick connectivity test.
This will verify that the profile contain valid credentials,
and that the Java program can write to target database schema.

For this test, you should choose a table that has some data, but is not too large.
Good tables for this test might include 
`cmdb_ci_service` or `cmn_location`.

Using the appropriate JAR file, type the following command:

<!--
    java -ea -jar <jarfilename> -p <profilename> -t <tablename>
-->

<pre class="highlight">
java -ea -jar <var>jarfilename</var> -p <var>profilename</var> -t <var>tablename</var>
</pre>


The Java program should connect to ServiceNow and to the database,
create a new table in the target schema,
and copy the ServiceNow data into the newly created table.

If you run the command a second time, the the `CREATE TABLE` will be missing.
When SNDML starts a job, 
it first check to see if the target table exists in the schema.
If an existing table is not found
then SNDML will issue a `CREATE TABLE` statement
before starting the load.

If the initial test is successful, then we can begin configuring the Agent.

## Create a Database Agent Record

In your ServiceNow instance, go to **DataPump > Agents** and click **New**. 
Create a new Database Agent record with the name "main".

## Configure a Table and a Job

For the first test of the Agent, you should again choose a ServiceNow table 
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

For additonal information about configuring jobs
refer to [Job Action Types](#job-action-types) below.

Your newly created **Job Run** record has a status of **Ready**.
It is waiting to be executed by the Java Agent.

## Run an SNDML Scan

On your Linux or Windows server, type this command:
    
<pre class="highlight">
java -ea -jar <var>jarfilename</var> -p <var>profilename</var> --scan
</pre>

The `--scan` command looks for any **Job Run** records that are **Ready**,
and executes them.

As the job executes, the **Job Run** record will be updated,
and rows will be appended to the **Job Run Logs** related list.
If you are viewing the **Job Run** record in ServiceNow, 
then you should see the counters and the status changing.

When each job completes, `--scan` checks for new **Job Run** records that are **Ready**.
If none are found then the Java program terminates.

## Methods for Running Jobs

Once a **Job Run** record is created with a state of "Ready", it must be be detected by the Java agent. 
There are three methods.
* [Synchronized Scanning](#synchronized-scanning) (`--scan`)
* [Run SNDML as a Daemon](#run-sndml-as-a-daemon) (`--daemon`)
* [Run SNDML as an HTTP Server](#run-sndml-as-an-http-server) (`--server`)

With the first two methods (<code>&#8209;&#8209;scan</code> and <code>&#8209;&#8209;daemon</code>) 
there will be a small delay 
between when the **Job Run** record is marked "Ready" and when execution starts.
HTTP Server is new in Release 3.5 and eliminates this delay.
HTTP Server is configured using the **Job Run Autostart** field on the **Agent** record.

## Creating Schedules

DataPump jobs can be grouped together in **Schedules**, and 
automatically activated by the ServiceNow scheduler. 
The steps are as follows:

1. Create a Schedule by going to **DataPump > Schedules**, and clicking New.
2. Create **Jobs** by going to **DataPump > Tables**, opening a table, 
   and clicking the **New** button above the Jobs related list.
3. The **Job** must be saved before it can be added to a Schedule. 
   To add a **Job** to a **Schedule**, edit the **Schedule** field on the **Job** form.
4. To test a Schedule, open the **Schedule** form and click the **Execute Now** button.

Since the DataPump table `x_108443_sndml_action_schedule` is extended from the 
out-of-box **Scheduled Script Execution**,
Schedules can be configured to run at any frequency permitted by ServiceNow.

If a **Job** is part of a **Schedule**, 
then the **Order** field on the **Job** form becomes important. 
Jobs within a Schedule are processed in order, based on the **Order** field. 
If multiple Jobs have the same **Order** value, then they may run concurrently, 
subject to the number of available threads. 
(The number of threads is configured in the Connection Profile.) 
Jobs with a higher **Order** value will remain in a **Scheduled** state 
until all Jobs with a lower **Order** value complete. 

This screenshot shows a schedule with three jobs. 
The table `sys_user_grmember` will be exported after the other two jobs complete.

#### Schedule with three jobs
<img src="images/2025-01-30-schedule-with-3-jobs.png" width="800" class="screenshot"/>

All Jobs within a Schedule will have the same "start time", 
regardless of when they actually start running. 
The Java agent will only export records that were inserted before the "start time". 
"Start time" is based on when the **Job Run** record was created, 
not when the **Status** was changed to "Running". 
All **Job Run** records in a **Schedule Run** are created at the same time, 
therefore the application will not export records inserted after 
the start of another job in the same schedule.

## Synchronized Scanning

Synchronized Scanning involves using **cron** or **Windows Task Scheduler** to run a `--scan`. 
The start time of the Linux or Windows job is synchronized with the start time of the ServiceNow schedule. 
The Linux or Windows job should start a few minutes after the ServiceNow schedule.
For example, if the ServiceNow schedule starts at 5:00 PM,
then you might set your `--scan` to start at 5:02 PM.

## Run SNDML as a Daemon

The `--daemon` option is the simplest to configure. 
This option simply runs SNDML  in an endless loop, 
performing a `--scan` every few minutes.

The frequency of scans can be changed 
by setting the value of the **Connection Profile** property `daemon.interval`
to the number of seconds between scans.


## Run SNDML as an HTTP Server

The `--server` option runs the Java program as an HTTP server.
When the state of a **Job Run** changes to **Ready**,
an HTTP message is sent to the SNDML server 
with the `sys_id` of the **Agent** and the `sys_id` of the **Job Run** record.
SNDML then uses the `app.instance` HTTPS connection (REST API) to retrieve the **Job Run** information,
and it starts execution of the job.

This option requires that you specify a TCP/IP port in the
[Connection Profile](#create-a-connection-profile)
and also on the **Agent** configuration form.
For example, add this line to the **Connection Profile**:

* `server.port=5124`

and configure the **Agent** as follows:

* Set **Job Run Autostart** to **HTTP Server**
* Set **HTTP Server Host** to the IP address of the Linux or Windows server
* Set **HTTP Port** to **5124**

In addition, you must modify your firewall configuration to open TCP/IP port 5124 for inbound connections.

You can choose a different TCP/IP port as long as the **Connection Profile**
and the **Agent** are configured consistently.

As an alternative to opening the TCP/IP port for inbound connnections, 
you can install a MID Server and SNDML on the same box.
In this case you will specify **MID Server** on the **Agent** configuration form.
Since the MID Server will be forwarding TCP/IP messages 
to an SNDML server on the same box,
you should specify the **HTTP Server Host** as `localhost`.

#### Agent configured for HTTP Server on MID Server VM (localhost)
<img src="images/2025-01-28-http-server-via-mid.png" width="600" class="screenshot"/>

Use this command to start the HTTP server as a background process on Linux:

<pre class="highlight">
java -Dlog4j2.configurationFile=log4j2-daemon.xml \
  ‑Dsndml.logFolder=<var>logdirectory</var> ‑Dsndml.logPrefix=<var>agentname</var> \
  -jar <var>jarfilename</var> -p <var>profilename</var> --server >/dev/null 2>&1
</pre>

## Shut down Daemon or Server

To shut down a Daemon or Server, send a SIGTERM signal to the PID.  

Add the following line to the **Connection Profile** to capture the PID 
when SNDML starts.

<pre class="hilight">
loader.pidfile=<var>pidfilename</var>
</pre>                   

For Linux, use the following command to shut down a Daemon or Server.

<pre>
kill $(cat <var>pidfilename</var>)
</pre>

## Logging

SNDML3 uses [Apache Log4j2](https://logging.apache.org/log4j/2.x/) for logging. 
Sample Log4j2 configuration files can be found in the directory 
[/src/main/resources](https://github.com/gflewis/sndml3//blob/master/src/main/resources).
These files are embedded in the JAR, so you can use them directly or modify them based on 
your own logging requirements.

The default configuration file (which writes to the console) is 
[log4j2.xml](https://github.com/gflewis/sndml3/blob/master/src/main/resources/log4j2.xml).

The file 
[log4j2-daemon.xml](https://github.com/gflewis/sndml3/blob/master/src/main/resources/log4j2-daemon.xml)
redirects all output to a designated directory and creates a new log file at the beginning of each day.
This can be useful for `--daemon` or `--server` or 
[Synchronized Scanning](#synchronized-scanning)
using **cron** or **Windows Task Scheduler**. 
This file requires two system properties:

* `sndml.logFolder` - the directory where log files are written
* `sndml.logPrefix` - a prefix which will be prepended to the log file name

As an example, you can use this command to start the daemon as a background process on Linux,
creating a new log file each day in a designated directory.

<pre class="highlight">
java -Dlog4j2.configurationFile=log4j2-daemon.xml \
  ‑Dsndml.logFolder=<var>logdirectory</var> ‑Dsndml.logPrefix=<var>agentname</var> \
  -jar <var>jarfilename</var> -p <var>profilename</var> --daemon >/dev/null 2>&1
</pre>

Note that a `-D` prefix is used when passing system properties to Java, 
and that system properties are case sensitive.

## Job Action Types

### Insert
**Insert** is used for initial loading or reloading of SQL tables. 
It inserts rows into the target table. 
If a record with the same `sys_id` already exists in the target table, 
then a primary key violation will occur and the row will be skipped.

If **Truncate** is checked, then the SQL table will be truncated prior to the load.

### Upsert
**Upsert** is used to load or update SQL tables. 
This is the most common Job Action.
If the target record exists (based on `sys_id`), then it will be updated. 
Otherwise, it will be inserted.

If **Since Last** is checked, then only records inserted or updated in ServiceNow since the last run 
will be processed. The following filter will be used when retrieving records from ServiceNow:

<pre>
sys_updated_on&gt;=<var>lastrunstart</var>
</pre>

where <var>lastrunstart</var>
is determined from the **Last Run Start** field on the Database Table record.

### Sync
**Sync** compares the timestamps (`sys_updated_on`) in the source and target tables. 
Based on this comparison it will insert, update or delete target records. 
If the values of `sys_updated_on` match, then the record will be skipped.

If a **Filter** has been configured for the Database Table, 
the **Sync** will delete any records which do not match the filter.

### Execute
**Execute** executes an arbitrary SQL statement. 
This is typically used to run a database stored procedure following a load.

## Optimizing Exports

The DataPump Java agent uses the REST Table API to retrieve data from ServiceNow. 
Records are retrieved in chunks referred to as "pages". 
By default, the agent will retrieve all fields in the ServiceNow record, and use a page size of 200, 
meaning that it will retrieve and process 200 records at a time. 
(The default page size can changed in the connection profile.) The processing sequence is as follows:

1. Fetch 200 records (_i.e._ one page) from ServiceNow
2. Insert or Update the SQL table
3. Commit the changes to the SQL database
4. Update the record counters in the DataPump **Job Run** table 
5. Repeat

In general, most of the time is spent communicating with ServiceNow. 
Interactions with the SQL database are relatively quick.

The primary two techniques to improve the performance of exports are to reduce the number of columns 
and to increase the page size. 
In this Incident table example, we have have 
increased the page size from 200 to 2000
and selected 5 columns.

<img src="images/2025-01-30-table-advanced.png" width="800" class="screenshot"/>

The fields `sys_id`, `sys_created_on` and `sys_updated_on` are always exported by the Java agent, 
regardless of whether or not they are included in the column list; 
so in this example we are actually exporting 8 columns.

It is important to note that DataPump will NOT add or drop columns in a pre-existing table. 
If you change the Columns setting on the Database Table form after the SQL table has been created, 
then you must either use `ALTER TABLE` to modify the table structure, 
or drop the table and allow SNDML to recreate it.

## Partitioned Load

Task based tables may contain a large amount of history, and may take a many hours to initially export. 
The application allows these tables to be exported in sections (<i>i.e.</i> partitions), 
based on the record creation date (`sys_created_on`).

**Partition** may be specified on the **Advanced** section of the **Job** form 
as **Quarter**, **Month**, **Week** or **Day**. 
When performing a partitioned export, 
the Java agent starts by determining the minimum and maximum values of `sys_created_on`, 
and then divides the work accordingly. 
All partitions are written to the same target table.

Partitions are exported in reverse chronological order. 
In other words, the most recent partition is always exported first. 
In some cases this may permit useful analysis of recent data while the export of older data continues.

If a Partition is specified for a **Job**, then a **Job Run Parts** tab 
will appear at the bottom of the **Job Run** form
as shown in this screenshot.

If **Partition** is specified for a **Job**, then **Threads** may also be specified. 
If the number of threads is 2 or more, then the Java agent will export multiple partitions in parallel. 
In some situations the total export time for large tables may be reduced by using multiple threads.
However, it is recommended that **Threads** not be set to a value greater than 4.

**Caution:** The use of multiple threads may adversely impact the performance of your ServiceNow instance.

#### Job Run Parts for Insert job partitioned by Month
<img src="images/2025-01-28-job-run-parts.png" width="800" class="screenshot"/>

## Feedback and Support

If you have questions or issues with SNDML or the DataPump app,
please use the [SNDM3 Github Issues Page](https://github.com/gflewis/sndml3/issues)
to open a new issue.

You can also open an Issue to provide positive feedback on this product.
Even if you are not having any issues,
we would love to hear what version you are using, 
what features you are using,
what database you are using,
the number of tables you have configured,
and the size of your largest table.
