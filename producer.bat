@echo off
REM Run Docker container in background
docker run -d --rm --network test_default test-producer

REM Wait for 5 seconds
timeout /t 5 /nobreak > nul