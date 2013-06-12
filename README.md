(c) Copyright 2013 WibiData, Inc.

# KijiShopping #

This project contains the skeleton for a web application that uses the
Kiji platform. It contains two modules in this directory:

* lib - Packages a JAR of gatherers, producers, importers, etc.
* app - The web application servlet.


### Configuring your environment for the application ###

You must have set HADOOP_HOME and HBASE_HOME environment variables to
allow the web application to work with Kiji. In particular,
$HADOOP_HOME/conf/{core,hdfs,mapred}-site.xml files need to specify the
addresses of your Hadoop cluster, and $HBASE_HOME/conf/hbase-site.xml needs
to specify the address of your ZooKeeper/HBase cluster.

The easiest way to run this application is to download the Kiji BentoBox
and run the bin/kiji-env.sh script in the BentoBox directory, and then
run `bento start`.

### Starting the application the first time ###

The application requires some amount of data processing to be done in batch
before the webserver is started. This can be done on a one-time basis by
running the build.sh script in the top-level directory.

    $ ./build.sh

Once the app and lib modules have been successfully built, the Kiji schema
has been created, and all of the batch processing has been completed, the
webserver can be started by running the start.sh script in the top-level
directory.

    $ ./start.sh

Open up a web browser to http://localhost:11786/ to see your app in action!

### Making code changes ###

The "app" module depends on the lib project. To build the entire project,
type 'mvn install' at the top level (in this directory).

    $ mvn install

If successful, you can launch the web application on your local machine
using the maven jetty plugin. You must do this from the 'app' module.

    $ cd app
    $ mvn jetty:run
