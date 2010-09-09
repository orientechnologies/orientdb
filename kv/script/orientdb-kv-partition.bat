@echo off

rem
rem Copyright (c) 1999-2010 Luca Garulli
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
echo         ,,.     `,  K E Y - V A L U E   S E R V E R        
echo       ``        `.                                       
echo                 ``    (CLUSTER-PARTITION POWERED BY
echo                 `              HAZELCAST)  

rem Guess ORIENTDB_HOME if not defined
set CURRENT_DIR=%cd%

if not "%ORIENTDB_HOME%" == "" goto gotHome
set ORIENTDB_HOME=%CURRENT_DIR%
if exist "%ORIENTDB_HOME%\bin\orientdb-kv.bat" goto okHome
cd ..
set ORIENTDB_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%ORIENTDB_HOME%\bin\orientdb-kv.bat" goto okHome
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

set CONFIG_FILE=%ORIENTDB_HOME%/config/orientdb-kv-partition-config.xml
set LOG_LEVEL=warning
set HAZELCAST_FILE=%ORIENTDB_HOME%/config/hazelcast.xml
set WWW_PATH=%ORIENTDB_HOME%/www

call "%JAVA_HOME%\bin\java" -server -Xms1024m -Xmx1024m -XX:+UseParallelGC -XX:+AggressiveOpts -XX:CompileThreshold=200 -Dhazelcast.config="%HAZELCAST_FILE%" -Dorientdb.config.file="%CONFIG_FILE%" -Dorientdb.www.path="%WWW_PATH%" -Dorientdb.log.level=%LOG_LEVEL% -jar "%ORIENTDB_HOME%\lib\orientdb-kv-@VERSION@.jar" %CMD_LINE_ARGS%

:end
