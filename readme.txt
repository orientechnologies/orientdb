           .                                          
          .`        `                                 
          ,      `:.                                  
         `,`    ,:`                                   
         .,.   :,,                                    
         .,,  ,,,                                     
    .    .,.:::::  ````                               
    ,`   .::,,,,::.,,,,,,`;;                      .:  
    `,.  ::,,,,,,,:.,,.`  `                       .:  
     ,,:,:,,,,,,,,::.   `        `         ``     .:  
      ,,:.,,,,,,,,,: `::, ,,   ::,::`   : :,::`  :::: 
       ,:,,,,,,,,,,::,:   ,,  :.    :   ::    :   .:  
        :,,,,,,,,,,:,::   ,,  :      :  :     :   .:  
  `     :,,,,,,,,,,:,::,  ,, .::::::::  :     :   .:  
  `,...,,:,,,,,,,,,: .:,. ,, ,,         :     :   .:  
    .,,,,::,,,,,,,:  `: , ,,  :     `   :     :   .:  
      ...,::,,,,::.. `:  .,,  :,    :   :     :   .:  
           ,::::,,,. `:   ,,   :::::    :     :   .:  
           ,,:` `,,.                                  
          ,,,    .,`                                  
         ,,.     `,                 DOCUMENT-GRAPH DB   
       ``        `.                                   
                 ``                                   
                 `                                    
*******************************************************************************
                                  ORIENT DATABASE
                         http://www.orientechnologies.com
*******************************************************************************

1 Pre-requirements

Before to download, compile and install the latest version of Orient please
assure to have the following tools installed:

1.1 Java
OrientDB needs Java Run-Time (JRE/JDK) version 6 or major to run the Server. Clients
need Java release 5 or major. We suggest to use Java version 7 because the
performance improvement.
 
To download Java go to: http://www.java.com/en/download/

Note: Please assure to download the JDK and not JRE.

1.2 Apache Ant
Apache Ant version 1.6.5 (previous version should works too).

You can download Ant from here:
> http://ant.apache.org/bindownload.cgi

1.3 Apache Maven
Apache Maven is used to run part of the test suite

You can download Maven from here:
> http://maven.apache.org/download.cgi

2 Build the last version of Orient

Follow the instruction below:

a) Open a shell and go in the path where you want to download the
   sources
b) Type:
   git clone git://github.com/nuvolabase/orientdb.git
c) Move to the directory *orientdb* and execute *ant* (or run the build script)
d) Wait for building and when finished
e) Enjoy ;-)

If you want to build the Graphed Edition type:
ant clean installg

If you need help, start from here:
> http://groups.google.com/group/orient-database

3 Launch the tests

The test suites will create the demo and the tinkerpop (for Graphed Edition) default databases

Follow the instruction below:

a) Open a shell and go in the path where you have installed the OrientDB sources.
b) Type: 
   ant test
c) Wait the process completes
d) Once finished you can close the OrientDB Server instance started for the tests 
e) Type:
   mvn clean test


OrientDB Team
