@echo off
where pm2.cmd >nul 2>&1
if errorlevel 1 (
  echo PM2 is not available on PATH. Install it with: npm install --global pm2
  exit /b 1
)
call pm2.cmd resurrect
