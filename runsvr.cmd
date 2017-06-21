@echo off
setlocal
set CLASSPATH=.\build\libs\tfwebsock-all.jar
"%JAVA_HOME%\bin\java" com.spreadserve.tfwebsock.ServerMain transficc.props
endlocal
