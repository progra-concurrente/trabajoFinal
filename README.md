# PowerSight

PowerSight es un sistema distribuido que ayuda a hogares a prevenir períodos
de consumo eléctrico elevado.

No utiliza machine learning para repetir si la lectura actual ya es alta. La
API separa dos resultados:

- diagnóstico actual, calculado directamente con la última lectura;
- pronóstico de consumo alto sostenido durante los próximos 30 minutos.

El pronóstico usa las últimas 15 lecturas, su promedio, máximo, variabilidad y
tendencia, además de la hora, día, mes y riesgo histórico de la franja.

## Arquitectura

- API y coordinador escritos en Go.
- Tres nodos ML comunicados por TCP con `net` y `bufio`.
- Goroutines y channels en preprocesamiento, API, coordinador y cada nodo.
- MongoDB para pronósticos, recomendaciones, modelos y entrenamientos.
- Redis para caché, resultados parciales y eventos.
- JWT, Swagger y WebSocket.

## Preparar datos y modelo

```powershell
go run ./data-load/cmd -root . -workers 8
```

Genera:

- `processed_data.csv`
- `forecast_training.csv`
- `forecast_test.csv`
- `forecast_model.json`
- agregados históricos y `sustainability_report.json`

## Ejecutar

```powershell
docker compose up --build
```

Abrir:

```text
http://localhost:8080/swagger/
```

Credenciales de demostración:

```text
admin / powersight
```

La explicación completa está en
[`docs/DISTRIBUTED_SYSTEM.md`](docs/DISTRIBUTED_SYSTEM.md).
