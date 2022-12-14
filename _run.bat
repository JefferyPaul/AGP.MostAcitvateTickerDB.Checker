echo off
chcp 65001


cd %~dp0
call "venv\Scripts\activate.bat"

python main.py -o "./Output" -d -2 --otoday
pause