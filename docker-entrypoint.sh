#!/bin/bash
set -e

mkdir -p /app/logs

echo "🚀 Iniciando AgroSync + MeteoTask + AlertasTask..."

# 1. Inicia meteoTask.py EN SEGUNDO PLANO
echo "🌦️  Iniciando MeteoTask Forecast..."
python -u /app/app/ProgramedJobs/meteoTask.py &
METEO_PID=$!
echo $METEO_PID > /tmp/meteo.pid

# 2. Inicia alertasTask.py EN SEGUNDO PLANO
echo " Iniciando histVegetaTask..."
python -u /app/app/ProgramedJobs/histVegetaTask.py &
HIST_VEGETA_PID=$!
echo $HIST_VEGETA_PID > /tmp/histVegetaTask.pid

# 3. Inicia alertasTask.py EN SEGUNDO PLANO
echo "🌦️ Iniciando histMeteoTask..."
python -u /app/app/ProgramedJobs/histMeteoTask.py &
HIST_METEO_PID=$!
echo $HIST_METEO_PID > /tmp/histMeteoTask.pid


# 4. Inicia alertasTask.py EN SEGUNDO PLANO
echo " Iniciando AlertasTask..."
python -u /app/app/ProgramedJobs/alertasTask.py &
ALERTAS_PID=$!
echo $ALERTAS_PID > /tmp/alertas.pid


# 5. Espera que todos arranquen
sleep 5

echo "✅ MeteoTask PID: $METEO_PID"
echo "✅ histVegetaTask PID: $HIST_VEGETA_PID"
echo "✅ histMeteoTask PID: $HIST_METEO_PID"
echo "✅ AlertasTask PID: $ALERTAS_PID"
echo "🌤️ MeteoTask + histVegetaTask + histMeteoTask + AlertasTask  corriendo en background"
echo "🔥 Iniciando Flask en puerto 8282..."

# 4. Flask en PRIMER PLANO
exec python -u app/main.py
