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
echo         ,,.     `,                D B   S E R V E R        
echo       ``        `.                                       
echo                 ``                                       
echo                 `                                        

rem Guess ORIENT_HOME if not defined
set CURRENT_DIR=%cd%

if exist "%JAVA_HOME%\bin\java.exe" goto setJavaHome
set JAVA=java
goto okJava

:setJavaHome
set JAVA="%JAVA_HOME%\bin\java"

:okJava
if not "%ORIENT_HOME%" == "" goto gotHome
set ORIENT_HOME=%CURRENT_DIR%
if exist "%ORIENT_HOME%\bin\orient-server.bat" goto okHome
cd ..
set ORIENT_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%ORIENT_HOME%\bin\orient-server.bat" goto okHome
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
set CONFIG_FILE=%ORIENT_HOME%/config/orient-server-config.xml
set LOG_LEVEL=warning
set WWW_PATH=%ORIENT_HOME%/www
REM set JAVA_OPTS=-Xms1024m -Xmx1024m

call %JAVA% -server %JAVA_OPTS% -XX:+UseParallelGC -XX:+AggressiveOpts -XX:CompileThreshold=200 -Dorient.config.file="%CONFIG_FILE%" -Dorient.www.path="%WWW_PATH%" -Dorient.log.level=%LOG_LEVEL% -jar "%ORIENT_HOME%\lib\orient-database-server.jar" %CMD_LINE_ARGS%

:end
