:: Windows launcher script for Gremlin

@echo off

set LIBDIR=..\lib
set PLUGINDIR=..\plugins


set CP=
for %%i in (%LIBDIR%\*.jar) do call :concatsep %%i

set JAVA_OPTIONS=-Xms32M -Xmx512M


:: Launch the application

if "%1" == "" goto console

if "%1" == "-e" goto script

if "%1" == "-v" goto version



:console

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %CP%;%PLUGINDIR%\* com.tinkerpop.gremlin.groovy.console.Console

goto :eof



:script


set strg=


FOR %%X IN (%*) DO (

CALL :concat %%X %1 %2

)




java %JAVA_OPTIONS% %JAVA_ARGS% -cp %CP% com.tinkerpop.gremlin.groovy.jsr223.ScriptExecutor %strg%

goto :eof



:version

java %JAVA_OPTIONS% %JAVA_ARGS% -cp %CP% com.tinkerpop.gremlin.Version

goto :eof



:concat

if %1 == %2 goto skip

SET strg=%strg% %1



:concatsep

if "%CP%" == "" (

set CP=%LIBDIR%\%1

)else (

set CP=%CP%;%LIBDIR%\%1

)



:skip
