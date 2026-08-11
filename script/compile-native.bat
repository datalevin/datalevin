echo The current working directory is: %CD%

dir %CD%

if "%GRAALVM_HOME%"=="" (
    echo Please set GRAALVM_HOME
    exit /b
)

echo GRAALVM_HOME %GRAALVM_HOME%

dir "%GRAALVM_HOME%"

set "JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8"

echo JAVA_HOME %JAVA_HOME%

java -version

echo Test Clojure code ...

call lein.bat run

echo Build test uberjar ...

call lein.bat with-profile test0-uberjar do clean, uberjar

if %errorlevel% neq 0 exit /b %errorlevel%

echo Build native test ...

call "%GRAALVM_HOME%\bin\native-image.cmd" ^
   "-R:MaxHeapSize=5g" ^
   "-jar" "target/test0.uberjar.jar" ^
   "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
   dtlv-test0

if %errorlevel% neq 0 exit /b %errorlevel%

echo Run native test ...

.\dtlv-test0 -Xmx5g

if %errorlevel% neq 0 exit /b %errorlevel%

del dtlv-test0

echo Build main uberjar ...

call lein.bat with-profile native-uberjar uberjar

if %errorlevel% neq 0 exit /b %errorlevel%

echo Build native app ...

call "%GRAALVM_HOME%\bin\native-image.cmd" ^
  "-jar" "target/main.uberjar.jar" ^
  "-H:NativeLinkerOption=legacy_stdio_definitions.lib" ^
  dtlv

if %errorlevel% neq 0 exit /b %errorlevel%
