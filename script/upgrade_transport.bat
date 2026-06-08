echo off

::指定起始文件夹
set DIR="%cd%\..\transport"

for /R %DIR% /d %%i in (*) do (
    echo %%i
    cd %%i
    go get -u ./...
    go mod tidy
)
