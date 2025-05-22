The **ServiceNow Data Mart Loader (SNDML)** is a Java application which exports data from ServiceNow 
into an SQL database such as MySQL, PostgreSQL, Oracle or Microsoft SQL Server. 
SNDML uses the ServiceNow REST API to extract data from ServiceNow. 
It uses JDBC to load target tables. It creates tables in the target database based on ServiceNow meta-data. 
It supports a variety of load and synchronization operations. 

**ServiceNow DataPump** is a scoped ServiceNow app which is installed in the ServiceNow instance and is used to configure and manage SNDML jobs.
For an introduction to the DataPump app, please refer to
- [Exporting ServiceNow Data using DataPump 3.5](https://youtu.be/r3TOvHVKeDQ) (YouTube Video)
- [Getting Started with ServiceNow DataPump](https://gflewis.github.io/sndml3/) (GitHub Pages)

For an overview of the changes in **Relase 3.5** please refer to
- https://github.com/gflewis/sndml3/wiki/Release-3.5
  
If you are using YAML to configure the application, please refer to these wiki pages
- https://github.com/gflewis/sndml3/wiki/Home
- https://github.com/gflewis/sndml3/wiki/Getting-Started
- https://github.com/gflewis/sndml3/wiki/YAML-Configuration
- https://github.com/gflewis/sndml3/wiki/Options

This program is freely distributed software. You are welcome to redistribute and/or modify it. 
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY, explicit or implied. 
