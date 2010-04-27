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

rem Guess ORIENT_HOME if not defined
set CURRENT_DIR=%cd%

if not "%ORIENT_HOME%" == "" goto gotHome
set ORIENT_HOME=%CURRENT_DIR%
if exist "%ORIENT_HOME%\bin\orient-kv.bat" goto okHome
cd ..
set ORIENT_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%ORIENT_HOME%\bin\orient-kv.bat" goto okHome
echo The ORIENT_HOME environment variable is not defined correctly
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

set CONFIG_FILE=%ORIENT_HOME%/config/orient-kv-partition.config
set LOG_LEVEL=warning
set HAZELCAST_FILE=%ORIENT_HOME%/config/hazelcast.xml
set WWW_PATH=%ORIENT_HOME%/www

call "%JAVA_HOME%\bin\java" -server -Xms1024m -Xmx1024m -XX:+UseParallelGC -XX:+AggressiveOpts -XX:CompileThreshold=200 -Dhazelcast.config="%HAZELCAST_FILE%" -Dorient.config.file="%CONFIG_FILE%" -Dorient.www.path="%WWW_PATH%" -Dorient.log.level=%LOG_LEVEL% -jar "%ORIENT_HOME%\lib\orient-database-kv.jar" %CMD_LINE_ARGS%

:end
