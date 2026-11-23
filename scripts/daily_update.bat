@echo off
chcp 65001 >nul
rem Cap nhat du lieu hang ngay (Task Scheduler goi file nay luc 15:30 cac ngay lam viec)
cd /d "%~dp0.."
call .venv\Scripts\activate.bat
python scripts\daily_update.py
