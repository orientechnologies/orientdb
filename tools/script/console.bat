@echo off
rem
rem Copyright (c) 1999-2011 Luca Garulli @www.orientechnologies.com
rem
rem Guess ORIENTDB_HOME if not defined
set CURRENT_DIR=%cd%

if exist "%JAVA_HOME%\bin\java.exe" goto setJavaHome
set JAVA="java"
goto okJava

:setJavaHome
set JAVA="%JAVA_HOME%\bin\java"

:okJava
if not "%ORIENTDB_HOME%" == "" goto gotHome
set ORIENTDB_HOME=%CURRENT_DIR%
if exist "%ORIENTDB_HOME%\bin\console.bat" goto okHome
cd ..
set ORIENTDB_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%ORIENTDB_HOME%\bin\console.bat" goto okHome
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

call %JAVA% -client -Dorientdb.build.number=@BUILD@ -jar "%ORIENTDB_HOME%\lib\orientdb-tools-@VERSION@.jar" %CMD_LINE_ARGS%

:end
