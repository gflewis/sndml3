---
title: Scheduling Datapump Exports
description: Using ServiceNow schedules and SNDML version 3.5 to automatically export to Oracle, SQL Server, MySQL or PostgreSQL
---
## Creating Schedules
DataPump jobs can be grouped together in **Schedules**, and 
automatically activated by the ServiceNow scheduler. 
The steps are as follows:

1. Create a Schedule by going to **DataPump > Schedules**, and clicking New.
2. Create **Jobs** by going to **DataPump > Tables**, opening a table, 
   and clicking the **New** button above the Jobs related list.
3. The **Job** must be saved before it can be added to a Schedule. 
   To add a **Job** to a **Schedule**, edit the **Schedule** field on the **Job** form.
4. To test a Schedule, open the Schedule form and click the **Execute Now** button.

Since the DataPump table `x_108443_sndml_action_schedule` is extended from the 
out-of-box table **Scheduled Script Execution**,
Schedules can be configured to run at any frequency permitted by ServiceNow.

If a **Job** is part of a **Schedule**, then the **Order** field on the Job form becomes important. 
Jobs within a Schedule are processed in order, based on the Order field. 
If multiple Jobs have the same Order number, then they may run concurrently, 
subject to the number of available threads. 
(The number of threads is configured in the connection profile.) 
Jobs with a higher order number will remain in a "Scheduled" state until Jobs with a lower Order number complete. 

This screenshot shows a schedule with three jobs. 
The table **sys_user_grmember** will be exported after the other two jobs complete.

![Schedule with 3 jobs](images/2021-04-25-schedule-with-3-jobs.jpeg)

All Jobs within a Schedule will have the same "start time", regardless of when they actually start running. 
The Java agent will only export records that were inserted before the "start time". 
"Start time" is based on when the **Job Run** record was created, 
not when the **Status** was changed to "Running". 
All **Job Run** records in a **Schedule Run** are created at the same time, 
therefore the application will not export records inserted after the start of another job in the same schedule.

## Running Scheduled Jobs
Once a **Job Run** record is created with a state of "Ready", it must be be detected by the Java agent. 
There are four methods for this.
* Synchronized Scan (`--scan`)
* Run as Daemon (`--daemon`)
* Run Job through MID Server (`--jobrun`)
* Run as HTTP Server (`--server`)

With the first two methods (`--scan` and `--daemon`) there will be a small delay 
between when the **Job Run** record is marked **Ready** and when execution starts.
The second two methods (`--jobrun` and `--server`) are new in Release 3.5
and eliminate this delay.
These two methods  and are condigured using the 
**Job Run Autostart** field on the **Agent** record.

## Synchronized Scan (`--scan`)

This method involves using **cron** or **Windows Task Scheduler** to execute the Java agent, 
and synchronizing the times with your ServiceNow schedules. 
For example, if you know that your ServiceNow schedules are set to run at the top of the hour, 
then create a **cron** or **Windows Task Scheduler** job which runs a couple of minutes later.

The SNDML JAR file contains an embedded Log4J2 Rolling File Appender configuration 
which can be helpful if you are using **cron** or **Windows Task Scheduler**. 
The name of this configuration file is **log4j2-daemon.xml**, 
and it requires two system properties:

* `sndml.logFolder` - the directory where log files are written
* `sndml.logPrefix` - a prefix which will be prepended to the log file name

Use this command to run the Java agent redirecting all output to the log directory:

```
java -Dlog4j2.configurationFile=log4j2-daemon.xml ‑Dsndml.logFolder=<path_to_log_directory> \
  ‑Dsndml.logPrefix=<name_of_agent> -jar <path_to_jar> -p <path_to_connection_profile> --scan
```

**blockquote**

<blockquote class="highlight">
<code>java -Dlog4j2.configurationFile=log4j2-daemon.xml ‑Dsndml.logFolder=</code>
<small><var>&lt;path_to_log_directory&gt;</var></small>
<code> \</code><br/>
<code>  ‑Dsndml.logPrefix=</code>
<small><var>&lt;name_of_agent&gt;</var></small>
<code> -jar </code>
<small><var>&lt;path_to_jar&gt;</small></var></small>
<code> -p </code>
<small><var>&lt;path_to_connection_profile&gt;</var></small>
<code> --scan</code>
</blockquote>

**pre** 

<pre class="highlight">
<code>java -Dlog4j2.configurationFile=log4j2-daemon.xml ‑Dsndml.logFolder=</code>
<small><var>&lt;path_to_log_directory&gt;</var></small>
<code> \</code><br/>
<code>  ‑Dsndml.logPrefix=</code>
<small><var>&lt;name_of_agent&gt;</var></small>
<code> -jar </code>
<small><var>&lt;path_to_jar&gt;</small></var></small>
<code> -p </code>
<small><var>&lt;path_to_connection_profile&gt;</var></small>
<code> --scan</code>
</pre>

Note that a "-D" prefix is used when passing system properties to Java, 
and that system properties are case sensitive.

For Linux, use this crontab entry will run the agent at 2, 17, 32 and 47 minutes past the hour:

```
02,17,32,47 * * * * java -Dlog4j2.configurationFile=log4j2-daemon.xml -Dsndml.logFolder=<log_directory> \
  ‑Dsndml.logPrefix=datapump-cron -jar <jar_file> -p <connection_profile> --scan >/dev/null 2>&1
```

## Run as Daemon (`--daemon`)

The `--daemon` option is the simplest to configure. 
This option simply runs SNDML  in an endless loop, 
performing a `--scan` every 2 minutes.

```
java -Dlog4j2.configurationFile=log4j2-daemon.xml ‑Dsndml.logFolder=<path_to_log_directory> \
  ‑Dsndml.logPrefix=<name_of_agent> -jar <path_to_jar> -p <path_to_connection_profile> --daemon >/dev/null 2>&1
```

## Run Job through MID Server (`--jobrun`)

## Run as HTTP Server (`--server`)
