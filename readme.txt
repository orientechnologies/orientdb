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

1.1 Java5
Java SDK 5.0+. Previous releases don't works since Orient needs Java5 features
such as Annotation and Typed Collection. But if you don't have constraints we
suggest you to download the last update of Java6.
 
You can download Java5 from here:
> http://java.sun.com/j2se/1.5.0/download.jsp

Note: Please assure to download the JDK and not JRE.

1.1 Apache Ant
Apache Ant version 1.6.5 (previous version should works too).

You can download from here:
> http://ant.apache.org/bindownload.cgi

2 Build the last version of Orient

Follow the instruction below:

a) Open a shell and go in the path where you want to download the
   sources.
b) Type:
   svn checkout https://orient.googlecode.com/svn/trunk/ orient
c) Move to the directory *trunk* and execute *ant*
d) Wait for building and when finished
e) Enjoy ;-)

If you want help start from here:
> http://groups.google.com/group/orient-database

3 Launch the tests

Follow the instruction below:

a) Open a shell and go in the path where you have installed the OrientDB sources.
b) Type: ant -f build-db.xml test
c) Wait for the completition.
d) Once finished you can close the OrientDB Server instance started for the tests 

Orient Database staff
