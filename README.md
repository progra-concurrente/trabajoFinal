# Parte A - Dataset limpio, analisis sostenible y modelo ML

## Ejecucion

Desde la raiz del proyecto:

```powershell
go -C data-load run ./cmd
```

La ejecucion imprime estadisticas de calidad, metricas del modelo y tiempos en
milisegundos por etapa, junto con su suma total.

Documentacion complementaria:

- `docs/PRESENTACION.md`: contenido organizado para exponer el caso.
- `docs/EXPLICACION_PROCESO.md`: explicacion detallada del flujo, variables,
  archivos generados y reporte final.

## Mision del negocio

La solucion busca apoyar la eficiencia energetica domestica mediante el analisis de consumo electrico. El objetivo no es solo procesar datos, sino detectar horarios y patrones de alto consumo para promover decisiones sostenibles: reducir picos, desplazar actividades intensivas y mejorar el monitoreo del uso de energia en el hogar.

Preguntas que responde la solucion:

- Cuando ocurren los picos de consumo electrico?
- Que horarios concentran mayor demanda domestica?
- Como detectar momentos de alto consumo para promover eficiencia energetica?

## Limpieza y transformacion

El cargador procesa el archivo `data/raw/household_power_consumption.txt` en paralelo por rangos de bytes. Cada worker aplica el flujo:

```text
linea cruda -> parse -> validate -> transform -> collector
```

Reglas de limpieza aplicadas:

1. Mantener solo filas con 9 columnas.
2. Descartar filas con `?` en columnas numericas.
3. Convertir columnas numericas a `float64`.
4. Validar que `Date` y `Time` formen un `DateTime` valido.
5. Descartar filas con `Global_active_power < 0`.
6. Descartar filas con `Voltage <= 0`.
7. Descartar filas con `Global_intensity < 0`.
8. Descartar filas con submeterings negativos.

Variables creadas para ML:

- `Hour`
- `DayOfWeek`
- `Month`
- `SubMeteringTotal = SubMetering1 + SubMetering2 + SubMetering3`
- `OtherConsumption = (GlobalActivePower * 1000 / 60) - SubMeteringTotal`
- `HighConsumption = 1` si `GlobalActivePower >= 1.528`, y `0` en caso contrario

Artefactos generados:

- `data/processed/processed_data.csv`: dataset limpio y listo para ML.
- `data/processed/training_data.csv`: 80% temporal usado para entrenamiento.
- `data/processed/test_data.csv`: 20% temporal reservado para evaluacion.
- `data/processed/hourly_demand.csv`: demanda agregada por hora.
- `data/processed/daily_demand.csv`: demanda agregada por dia de semana.
- `data/processed/monthly_demand.csv`: demanda agregada por mes.
- `data/processed/day_hour_demand.csv`: horas analizadas dentro de cada dia.
- `data/processed/calendar_date_demand.csv`: fechas recurrentes como `December 25`.
- `data/processed/month_hour_demand.csv`: horas analizadas dentro de cada mes.
- `data/processed/sustainability_report.json`: resumen de picos y recomendaciones.

## Diseno del modelo ML

Se implementa una regresion logistica binaria simple para predecir `HighConsumption`. El modelo se plantea como detector de momentos de alto consumo, util para activar alertas o recomendaciones de eficiencia energetica.

Target:

- `HighConsumption`

Features:

- `GlobalReactivePower`
- `Voltage`
- `GlobalIntensity`
- `SubMeteringTotal`
- `OtherConsumption`
- `Hour`
- `DayOfWeek`
- `Month`

No se usa `GlobalActivePower` como feature directa del modelo porque la etiqueta `HighConsumption` se define a partir de esa variable. Asi se evita que el modelo aprenda solo una regla trivial y se orienta la prediccion hacia variables operativas y temporales.

El dataset se divide conservando el orden temporal: el 80% mas antiguo se usa
para entrenamiento y el 20% mas reciente para prueba. El modelo se guarda en
`data/processed/logistic_model.json` con sus pesos, hiperparametros, matriz de
confusion y metricas separadas para entrenamiento y prueba.

## Analisis para sostenibilidad

Los archivos agregados permiten interpretar el comportamiento energetico:

- `hourly_demand.csv` ayuda a identificar horas pico y franjas de mayor demanda domestica.
- `daily_demand.csv` permite revisar si ciertos dias concentran mayor proporcion de alto consumo.
- `monthly_demand.csv` permite observar estacionalidad o cambios de demanda por mes.
- Los CSV detallados permiten investigar patrones por dia, mes, hora y fecha.
- `sustainability_report.json` resume los principales horarios, dias y meses criticos para proponer recomendaciones.

Cada agregado incluye:

- promedio de `GlobalActivePower`
- cantidad de registros
- cantidad de eventos `HighConsumption`
- tasa de alto consumo
- promedio de `OtherConsumption`

## Paralelizacion del calculo

La paralelizacion ocurre en dos niveles:

1. Carga y limpieza: los workers leen rangos del archivo crudo, procesan lineas y mandan registros limpios al collector.
2. Entrenamiento ML: en cada epoca de la regresion logistica, el dataset limpio se divide en particiones. Cada goroutine calcula gradientes locales sobre su particion y el collector promedia los gradientes para actualizar los pesos.
