@echo off
cd /d "%~dp0"
title GuitoDesk Signaling Server

echo.
echo  ╔══════════════════════════════════╗
echo  ║   GuitoDesk Signaling Server     ║
echo  ╚══════════════════════════════════╝
echo.

REM Verifica se Node.js está instalado
where node >nul 2>&1
if errorlevel 1 (
    echo  [ERRO] Node.js nao encontrado!
    echo  Instale em: https://nodejs.org
    pause
    exit /b 1
)

REM Instala dependencias se necessario
if not exist "node_modules" (
    echo  Instalando dependencias...
    call npm install
    echo.
)

echo  Iniciando servidor na porta 3000...
echo  Health: http://localhost:3000/health
echo.
node server.js
pause
