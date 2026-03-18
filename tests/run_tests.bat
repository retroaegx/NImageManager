@echo off
setlocal
cd /d "%~dp0\.."
set "NIM_TEST_OUTPUT=%CD%\tests\output"
if not exist "%NIM_TEST_OUTPUT%" mkdir "%NIM_TEST_OUTPUT%"
if not exist ".venv" (
  py -3 -m venv .venv
  if errorlevel 1 goto :error
)
call ".venv\Scripts\activate.bat"
if errorlevel 1 goto :error
python -m pip install --upgrade pip > "%NIM_TEST_OUTPUT%\bootstrap.log" 2>&1
if errorlevel 1 goto :error
python -m pip install -r tests\requirements-test.txt >> "%NIM_TEST_OUTPUT%\bootstrap.log" 2>&1
if errorlevel 1 goto :error
python tests\run_comprehensive_tests.py %*
set EXITCODE=%ERRORLEVEL%
echo.
echo Bootstrap log : %NIM_TEST_OUTPUT%\bootstrap.log
echo Console log   : %NIM_TEST_OUTPUT%\console.log
echo JSON report   : %NIM_TEST_OUTPUT%\test_report.json
echo MD report     : %NIM_TEST_OUTPUT%\test_report.md
echo Exit code     : %EXITCODE%
if "%NIM_TEST_NO_PAUSE%"=="1" goto :done
pause
:done
endlocal & exit /b %EXITCODE%
:error
echo Failed to prepare test environment.
echo Bootstrap log : %NIM_TEST_OUTPUT%\bootstrap.log
if "%NIM_TEST_NO_PAUSE%"=="1" goto :done_error
pause
:done_error
endlocal & exit /b 1
