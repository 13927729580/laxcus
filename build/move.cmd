@echo on

set BASE_HOME=d:\lexst
set BUILD_HOME=%BASE_HOME%\build
set DEST_HOME=e:\lexst

copy /Y %BUILD_HOME%\top\*.jar %DEST_HOME%\backup
copy /Y %BUILD_HOME%\home\*.jar %DEST_HOME%\backup
copy /Y %BUILD_HOME%\log\*.jar %DEST_HOME%\backup
copy /Y %BUILD_HOME%\work\*.jar %DEST_HOME%\backup
copy /Y %BUILD_HOME%\data\*.jar %DEST_HOME%\backup
copy /Y %BUILD_HOME%\build\*.jar %DEST_HOME%\backup
copy /Y %BUILD_HOME%\call\*.jar %DEST_HOME%\backup
copy /Y %BUILD_HOME%\live\*.jar %DEST_HOME%\backup
copy /Y %BUILD_HOME%\console\*.jar %DEST_HOME%\backup

copy /Y %BUILD_HOME%\top\*.jar %DEST_HOME%\top2\lib
copy /Y %BUILD_HOME%\top\*.jar %DEST_HOME%\top3\lib
copy /Y %BUILD_HOME%\home\*.jar %DEST_HOME%\home2\lib
copy /Y %BUILD_HOME%\home\*.jar %DEST_HOME%\home3\lib

move /Y %BUILD_HOME%\top\*.jar %DEST_HOME%\top\lib
move /Y %BUILD_HOME%\home\*.jar %DEST_HOME%\home\lib
move /Y %BUILD_HOME%\log\*.jar %DEST_HOME%\log\lib
move /Y %BUILD_HOME%\work\*.jar %DEST_HOME%\work\lib
move /Y %BUILD_HOME%\data\*.jar %DEST_HOME%\data\lib
move /Y %BUILD_HOME%\build\*.jar %DEST_HOME%\build\lib
move /Y %BUILD_HOME%\call\*.jar %DEST_HOME%\call\lib
move /Y %BUILD_HOME%\live\*.jar %DEST_HOME%\live\lib
move /Y %BUILD_HOME%\console\*.jar %DEST_HOME%\console\lib

copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\top2\lib
copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\top3\lib
copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\home2\lib
copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\home3\lib

copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\top\lib
copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\home\lib
copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\log\lib
copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\work\lib
copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\data\lib
copy /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\build\lib
move /Y %BUILD_HOME%\shutdown\shutdown.jar %DEST_HOME%\call\lib

move /Y %BUILD_HOME%\sleep\*.jar %DEST_HOME%\lib