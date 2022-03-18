The ServiceNow Data Mart Loader (SNDML) is a Java application which exports data from ServiceNow into an SQL database such as MySQL, PostgreSQL, Oracle or Microsoft SQL Server. SNDML uses the ServiceNow REST API to extract data from ServiceNow. It uses JDBC to load target tables. It creates tables in the target database based on ServiceNow meta-data. It supports a variety of load and synchronization operations. 

There are two ways to configure the application.
1. **YAML** configuration. For an introduction refer to these wiki pages
   - https://github.com/gflewis/sndml3/wiki/Home
   - https://github.com/gflewis/sndml3/wiki/Getting-Started
   - https://github.com/gflewis/sndml3/wiki/YAML-Configuration
   - https://github.com/gflewis/sndml3/wiki/Options
2. **DataPump** is a scoped app which is installed in the ServiceNow instance and can be used to configure and manage export jobs. For information refer to
   - [Exporting to MySQL, Oracle or SQL Server with DataPump](https://community.servicenow.com/community?id=community_article&sys_id=90628858db7f2010030d443039961918)

This program is freely distributed software. You are welcome to redistribute and/or modify it. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY, explicit or implied. 

