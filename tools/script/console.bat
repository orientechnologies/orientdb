@echo off
rem
rem Copyright (c) 1999-2010 Luca Garulli
rem
rem Guess ORIENT_HOME if not defined
set CURRENT_DIR=%cd%

if not "%ORIENT_HOME%" == "" goto gotHome
set ORIENT_HOME=%CURRENT_DIR%
if exist "%ORIENT_HOME%\bin\console.bat" goto okHome
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

call "%JAVA_HOME%\bin\java" -server -jar "%ORIENT_HOME%\lib\orient-database-tools.jar" %CMD_LINE_ARGS%

:end
