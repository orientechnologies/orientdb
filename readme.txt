           .                                          
          .`        `                                 
          ,      `:.                                  
         `,`    ,:`                                   
         .,.   :,,                                    
         .,,  ,,,                                     
    .    .,.:::::  ````                                 :::::::::     :::::::::
    ,`   .::,,,,::.,,,,,,`;;                      .:    ::::::::::    :::    :::
    `,.  ::,,,,,,,:.,,.`  `                       .:    :::      :::  :::     :::
     ,,:,:,,,,,,,,::.   `        `         ``     .:    :::      :::  :::     :::
      ,,:.,,,,,,,,,: `::, ,,   ::,::`   : :,::`  ::::   :::      :::  :::    :::
       ,:,,,,,,,,,,::,:   ,,  :.    :   ::    :   .:    :::      :::  :::::::
        :,,,,,,,,,,:,::   ,,  :      :  :     :   .:    :::      :::  :::::::::
  `     :,,,,,,,,,,:,::,  ,, .::::::::  :     :   .:    :::      :::  :::     :::
  `,...,,:,,,,,,,,,: .:,. ,, ,,         :     :   .:    :::      :::  :::     :::
    .,,,,::,,,,,,,:  `: , ,,  :     `   :     :   .:    :::      :::  :::     :::
      ...,::,,,,::.. `:  .,,  :,    :   :     :   .:    :::::::::::   :::     :::
           ,::::,,,. `:   ,,   :::::    :     :   .:    :::::::::     ::::::::::
           ,,:` `,,.                                  
          ,,,    .,`                                  
         ,,.     `,                                          GRAPH DATABASE   
       ``        `.                                   
                 ``                                   
                 `                                    
*******************************************************************************
                                 ORIENT DATABASE
                        http://www.orientechnologies.com
*******************************************************************************

 Requirements
---------------

Before to download, compile and install the last version of OrientDB please
assure to have the following tools installed:

 Java
---------------
OrientDB needs Java Run-Time (JRE/JDK) version 6 or major to run the Server.
We suggest to use Java version 7 because it's faster than Java 6.
 
To download Java go to: http://www.java.com/en/download/

Note: Please assure to download the JDK and not JRE.

 Apache Ant
---------------
Apache Ant version 1.6.5 (previous version should works too).

You can download Ant from here:
> http://ant.apache.org/bindownload.cgi

 Apache Maven
---------------
Apache Maven is used to run part of the test suite

You can download Maven from here:
> http://maven.apache.org/download.cgi

 Build the last version of OrientDB
-------------------------------------

The OrientDB development team is very active, so if your in the middle of the
development of your application we suggest to use last SNAPSHOT from the git
source repository. All you need is:

- JDK (Java Development Kit) 6+ 
  -> http://www.oracle.com/technetwork/java/javase/downloads/index.html
- git SCM tool
  -> http://git-scm.com/
- Apache Ant v1.8.2+
  -> http://ant.apache.org/manual/install.html

Then follow these simple steps in a shell (Mac/Linux) or a Command Prompt
(Windows):

> git clone https://github.com/nuvolabase/orientdb.git
> cd orientdb
> git checkout -b develop
> git pull origin develop
> ant clean installg

At the end of the build you will have a brand new distribution under the path:
../releases/orientdb-graphed-1.5.0. Use it as a normal OrientDB distribution
directory.

Every time you want to update your distribution with last changes do:

> git pull origin develop
> ant clean installg

At the end of the build your distribution (../releases/orientdb-graphed-1.5.0)
will be updated with last OrientDB libraries. Every time you compile a new version,
assure to have the permissions to execute the .sh files under the "bin" directory:

> cd ../releases/orientdb-graphed-1.5.0/bin
> chmod u+x *.sh


 Information
---------------

For more information visit the official website: http://www.orientdb.org.

Remember OrientDB is an Open Source project released with the Apache v2 license,
so it's always FREE for any purpose. If you're interested to Enterprise tools,
professional support, training or consultancy contact: info@orientechnologies.com.

Enjoy with Graphs,
Orient Technologies
The company behind OrientDB
(www.orientechnologies.com)
