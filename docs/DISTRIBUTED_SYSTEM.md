# PowerSight preventivo y distribuido

## Aporte social

PowerSight permite actuar antes de un período de consumo elevado. El usuario
recibe dos respuestas diferentes:

- **estado actual:** si la última lectura ya supera 1.528 kW;
- **pronóstico:** probabilidad de que al menos 10 de los próximos 30 minutos
  superen ese umbral.

Una alerta es especialmente útil cuando el consumo actual aún es normal, pero
la tendencia reciente indica un pico próximo.

## Entrada

`POST /api/v1/forecasts` recibe exactamente 15 lecturas consecutivas, una por
minuto. El backend calcula promedio, máximo, variabilidad, tendencia, consumo
submedido y consumo no identificado. También incorpora las tasas históricas
reales de la hora y de la combinación día-hora.

## Procesamiento concurrente

```text
archivo de 2 millones de filas
  -> workers de limpieza por rangos de bytes
  -> channel de resultados ordenados
  -> workers de ventanas temporales
  -> forecast_training.csv / forecast_test.csv
```

Durante el entrenamiento:

```text
API/coordinador
  -> shard TCP a cada nodo
  -> cada nodo divide su shard entre goroutines
  -> channel local de gradientes
  -> gradiente del nodo por TCP
  -> agregación global y actualización de pesos
```

## Arquitectura

```text
Frontend / Swagger
     | HTTP + JWT
API Go + coordinador TCP ----- MongoDB
     |                         Redis
     +--- ml-node-1
     +--- ml-node-2
     +--- ml-node-3
```

Los pronósticos y alertas se publican por WebSocket:

```text
ws://localhost:8080/ws?token=JWT
```

## Ejecución

```powershell
go run ./data-load/cmd -root . -workers 8
docker compose up --build
```

Swagger:

```text
http://localhost:8080/swagger/
```

Credenciales: `admin / powersight`.

## Interpretación

Una respuesta puede indicar:

```json
{
  "current_status": {
    "active_power_kw": 1.4,
    "currently_high": false
  },
  "horizon_minutes": 30,
  "probability": 0.72,
  "risk_level": "alto",
  "recommendation": {
    "title": "Prevén un pico durante los próximos 30 minutos"
  }
}
```

Esto significa que el hogar todavía no supera el umbral, pero debería evitar
iniciar otra carga intensiva porque existe riesgo de consumo alto sostenido.

## Resultados del modelo generado

- 409,239 ventanas temporales válidas;
- corte temporal 80/20;
- 80 épocas;
- entrenamiento y evaluación concurrentes;
- datos históricos conservados para explicar hora, día y estacionalidad.
