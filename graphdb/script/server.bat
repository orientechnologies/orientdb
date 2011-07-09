@echo off
rem
rem Copyright (c) 1999-2011 Luca Garulli @www.orientechnologies.com
rem

echo           .                                              
echo          .`        `                                     
echo          ,      `:.                                      
echo         `,`    ,:`                                       
echo         .,.   :,,                                        
echo         .,,  ,,,                                         
echo    .    .,.:::::  ````                                   
echo    ,`   .::,,,,::.,,,,,,`;;                      .:      
echo    `,.  ::,,,,,,,:.,,.`  `                       .:      
echo     ,,:,:,,,,,,,,::.   `        `         ``     .:      
echo      ,,:.,,,,,,,,,: `::, ,,   ::,::`   : :,::`  ::::     
echo       ,:,,,,,,,,,,::,:   ,,  :.    :   ::    :   .:      
echo        :,,,,,,,,,,:,::   ,,  :      :  :     :   .:      
echo  `     :,,,,,,,,,,:,::,  ,, .::::::::  :     :   .:      
echo  `,...,,:,,,,,,,,,: .:,. ,, ,,         :     :   .:      
echo    .,,,,::,,,,,,,:  `: , ,,  :     `   :     :   .:      
echo      ...,::,,,,::.. `:  .,,  :,    :   :     :   .:      
echo           ,::::,,,. `:   ,,   :::::    :     :   .:      
echo           ,,:` `,,.                                      
echo          ,,,    .,`                                      
echo         ,,.     `,                  GRAPH-DB Server        
echo       ``        `.                                       
echo                 ``                                       
echo                 `                                        

rem Guess ORIENTDB_HOME if not defined
set CURRENT_DIR=%cd%

if exist "%JAVA_HOME%\bin\java.exe" goto setJavaHome
set JAVA=java
goto okJava

:setJavaHome
set JAVA="%JAVA_HOME%\bin\java"

:okJava
if not "%ORIENTDB_HOME%" == "" goto gotHome
set ORIENTDB_HOME=%CURRENT_DIR%
if exist "%ORIENTDB_HOME%\bin\server.bat" goto okHome
cd ..
set ORIENTDB_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%ORIENTDB_HOME%\bin\server.bat" goto okHome
echo The ORIENTDB_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto end

:okHome
rem Get remaining unshifted command line arguments and save them in the
set CMD_LINE_ARGS=

:setArgs
if ""%1""=="""" goto doneSetArgs
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto setArgs

:doneSetArgs
set CONFIG_FILE=%ORIENTDB_HOME%/config/orientdb-server-config.xml
set LOG_FILE=%ORIENTDB_HOME%/config/orientdb-server-log.properties
set LOG_CONSOLE_LEVEL=info
set LOG_FILE_LEVEL=fine
set WWW_PATH=%ORIENTDB_HOME%/www
REM set JAVA_OPTS=-Xms1024m -Xmx1024m

call %JAVA% -server %JAVA_OPTS% -XX:+UseParallelGC -XX:+AggressiveOpts -XX:CompileThreshold=200 -Djava.util.logging.config.file="%LOG_FILE%" -Dorientdb.config.file="%CONFIG_FILE%" -Dorientdb.www.path="%WWW_PATH%" -Dlog.console.level=%LOG_CONSOLE_LEVEL% -Dlog.file.level=%LOG_FILE_LEVEL% -Dorientdb.build.number=@BUILD@ -cp "%ORIENTDB_HOME%\lib\*" com.orientechnologies.orient.server.OServerMain %CMD_LINE_ARGS%

:end