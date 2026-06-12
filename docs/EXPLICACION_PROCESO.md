# Explicacion del procesamiento

## Vista general

El programa toma el archivo crudo
`data/raw/household_power_consumption.txt`, procesa sus registros en paralelo,
elimina datos inutilizables, crea variables de analisis, genera archivos
resumidos y entrena una regresion logistica.

El orden completo es:

```text
archivo crudo
  -> carga paralela
  -> parseo y limpieza
  -> variables derivadas
  -> dataset procesado
  -> corte temporal 80% entrenamiento / 20% prueba
  -> agregados detallados y reporte ejecutivo
  -> reporte de sostenibilidad
  -> entrenamiento del modelo
  -> reporte final en terminal
```

## Que hace cada parte del codigo

- `cmd/main.go`: recibe parametros y empieza el procesamiento.
- `load.go`: coordina workers, canales, etapas, archivos y metricas de tiempo.
- `parse.go`: separa columnas y convierte texto a fecha o numeros.
- `validate.go`: decide si un registro es fisicamente y estructuralmente valido.
- `transform.go`: crea las nuevas variables.
- `analyze.go`: agrupa los datos y construye el reporte.
- `ml.go`: entrena y evalua la regresion logistica en paralelo.
- `write.go`: guarda CSV y JSON.
- `types.go`: define las estructuras que circulan entre las etapas.

## Variables nuevas y por que existen

### `Timestamp`

Une `Date` y `Time` en un valor temporal real. Sirve para validar la fecha y
extraer hora, dia de semana y mes sin manipular texto manualmente.

### `HasMissingValues`

Indica si alguna medicion contenia `?` o estaba vacia. Permite distinguir una
fila descartada por datos faltantes de otra descartada por formato invalido.

### `Hour`

Representa la hora del dia. Permite descubrir horas pico y da al modelo contexto
temporal.

### `DayOfWeek`

Representa el dia de la semana. Permite comparar patrones entre lunes, fines de
semana y otros dias.

### `DayOfMonth`

Representa el numero de dia dentro del mes. Junto con `Month` permite agrupar
fechas recurrentes como `December 25` sin limitar el analisis a un solo ano.

### `Month`

Representa el mes. Permite analizar estacionalidad y cambios a lo largo del ano.

### `SubMeteringTotal`

Suma el consumo medido en cocina, lavanderia/climatizacion y calentador/aire
acondicionado. Resume tres columnas relacionadas en una sola senal.

### `OtherConsumption`

Estima el consumo que no corresponde a los tres submedidores. Primero se
convierte la potencia activa en energia por minuto y luego se resta el total
submedido. Ayuda a detectar consumo proveniente de otros aparatos.

### `HighConsumption`

Es la variable objetivo del modelo. Convierte el problema en clasificacion:
`1` significa potencia activa mayor o igual a 1.528 kW y `0` significa consumo
por debajo de ese umbral.

### `LoadStats`

Guarda contadores de calidad: lineas leidas, filas limpias, filas con faltantes
y filas invalidas. Cada worker produce sus propios contadores y el colector los
suma.

### `byteRange`, `indexRange` y `chunkResult`

- `byteRange` delimita la parte del archivo asignada a un worker.
- `indexRange` delimita la parte del dataset asignada durante ML.
- `chunkResult` transporta registros limpios y estadisticas desde un worker.

### Variables internas del modelo

- `weights`: coeficientes que el modelo aprende.
- `gradient`: direccion de ajuste de cada peso.
- `learningRate`: tamano de cada actualizacion; se usa 0.25.
- `epochs`: numero de recorridos de entrenamiento; se usan 80.
- `loss`: error probabilistico promedio del modelo.
- `accuracy`: proporcion de clasificaciones correctas.

## Archivos generados y su finalidad

### `processed_data.csv`

Contiene todas las filas validas con las variables originales y derivadas. Es
la fuente limpia para analisis posteriores, modelos o visualizaciones.

### `training_data.csv`

Contiene el 80% mas antiguo del dataset limpio. Es la particion con la que se
calculan gradientes y se actualizan los pesos durante 80 epocas.

### `test_data.csv`

Contiene el 20% mas reciente y no actualiza pesos. Se reserva para medir el
comportamiento del modelo sobre datos que no vio durante el entrenamiento.

### `hourly_demand.csv`

Resume el comportamiento para cada hora de 00:00 a 23:00. Se creo para detectar
franjas horarias criticas.

### `daily_demand.csv`

Resume el comportamiento por dia de semana. Se creo para comprobar si la
demanda cambia entre dias laborales y fines de semana.

### `monthly_demand.csv`

Resume el comportamiento por mes. Se creo para observar estacionalidad.

### `day_hour_demand.csv`

Contiene todas las combinaciones entre dia de semana y hora. Permite responder,
por ejemplo, cuales son las tres horas mas criticas del sabado.

### `calendar_date_demand.csv`

Agrupa la misma fecha de calendario entre los distintos anos, por ejemplo todos
los `December 25`. Es mas robusto que ordenar fechas exactas como
`25/12/2008`, porque una fecha exacta solo contiene aproximadamente un dia de
mediciones.

### `month_hour_demand.csv`

Contiene todas las combinaciones entre mes y hora. Permite detectar si las horas
criticas cambian segun la epoca del ano.

### `sustainability_report.json`

Es un reporte ejecutivo reducido. Incluye:

- `peak_hours`: tres horas globales de mayor riesgo;
- `peak_day_hours`: cinco combinaciones directas de dia y hora;
- `peak_calendar_dates`: cinco fechas recurrentes del calendario.

Incluye la mision, el objetivo y recomendaciones. Se usa JSON porque conserva
esta estructura jerarquica de forma facil de consumir desde una API o frontend.
Cada resultado ejecutivo conserva solo tasa, potencia promedio y cantidad de
registros; las demas columnas permanecen en los CSV.

### `logistic_model.json`

Guarda tipo de modelo, target, features, pesos, hiperparametros, workers,
estrategia de corte y metricas de entrenamiento y prueba. Permite inspeccionar
el entrenamiento o cargar los parametros sin repetir inmediatamente todo el
proceso.

## Como leer el reporte de terminal

- `Rows read`: lineas de datos examinadas.
- `Rows clean`: filas que superaron parseo y validacion.
- `Rows dropped by missing values`: filas descartadas por `?` o vacios.
- `Rows dropped by invalid values`: filas con formato, fecha o valores invalidos.
- `ML model`: algoritmo entrenado.
- `ML parallel workers`: goroutines utilizadas en el entrenamiento.
- `Training metrics`: resultados sobre los datos usados para aprender.
- `Test metrics`: resultados sobre datos posteriores que no actualizaron pesos.
- `Confusion matrix`: cantidades TP, TN, FP y FN.
- `Sustainability report`: ubicacion del JSON de resultados interpretables.
- `Metricas de tiempo`: duracion de cada etapa en milisegundos.
- `TOTAL`: suma de las seis etapas medidas.

El `accuracy` no debe interpretarse solo. Si la mayoria de registros pertenece
a una clase, un modelo podria obtener un valor alto prediciendo siempre esa
clase. Por eso se agregaron matriz de confusion, precision, recall, specificity,
F1, balanced accuracy, positive rate y loss.

## Como se construye el reporte de sostenibilidad

Para cada grupo se acumulan cantidad de registros, potencia activa, eventos de
alto consumo y consumo no submedido. Luego se calculan promedios y la tasa de
alto consumo. Finalmente, los grupos se ordenan de mayor a menor tasa; si dos
grupos empatan, se prioriza el de mayor potencia activa promedio.

Por eso un grupo incluido en `peak_hours` no es simplemente el que tiene mas
filas. Es el que presenta una mayor proporcion de eventos de alto consumo.

El JSON evita repetir primero un dia y luego ese mismo dia con sus horas. En su
lugar ordena directamente todas las combinaciones dia-hora. Las fechas de
calendario agrupan todos los anos disponibles: `December 25` representa todos
los registros de cada 25 de diciembre.

Los agregados completos por dia, mes y mes-hora permanecen en CSV para analisis
exploratorio, pero no ocupan espacio en el reporte ejecutivo.

Cada porcentaje representa la proporcion de registros de esa combinacion que
supera el umbral de alto consumo, no su participacion sobre todo el dataset.

## Que significa el reporte actual

La ejecucion validada leyo 2,075,259 registros. De ellos, 2,049,280 fueron
utilizables y 25,979 se eliminaron por tener valores faltantes. No se
encontraron valores negativos o formatos invalidos despues de aceptar fechas
con dia o mes de uno o dos digitos.

El reporte indica que:

- 21:00 tiene una tasa de alto consumo de 0.5624: aproximadamente 56 de cada
  100 registros de esa hora superan el umbral.
- 20:00 alcanza 0.5532 y 19:00 alcanza 0.4908, por lo que la noche es la franja
  principal para recomendaciones.
- Sabado y domingo tienen las mayores tasas semanales, 31.54% y 30.17%.
- Diciembre y enero tienen tasas cercanas a 39.3%, lo que sugiere un patron
  estacional de mayor demanda.

La prueba temporal contiene 409,856 registros. El modelo obtiene 80.83% de
accuracy y 0.4111 de loss, pero la matriz de confusion cambia la interpretacion:
detecta 13,408 altos consumos y omite 78,583. Su recall es 14.58%, aunque la
precision y specificity sean 100% porque no genera falsos positivos.

Esto significa que el modelo actual es demasiado conservador. Para el objetivo
de alertar alto consumo, el recall y F1 son mas importantes que el accuracy
aislado. Una mejora razonable seria ajustar el umbral de decision con el
conjunto de entrenamiento o usar pesos de clase para penalizar mas los falsos
negativos. Como metricas futuras tambien pueden agregarse PR-AUC y ROC-AUC para
comparar umbrales sin elegir uno de antemano.

## Respuestas sobre el modelo y la paralelizacion

### 1. Que archivos usa el modelo y cuantas veces se ejecuta

El unico input externo del pipeline es
`data/raw/household_power_consumption.txt`. Durante la ejecucion se transforma
en un slice limpio y se separa en entrenamiento y prueba. Se guardan
`training_data.csv` y `test_data.csv` como evidencia reproducible, aunque el
modelo recibe estas particiones directamente en memoria y no vuelve a leer los
CSV.

El entrenamiento se ejecuta una vez cada vez que se corre `go run ./cmd`.
Dentro de esa ejecucion hay 80 epocas. Una epoca es una iteracion de aprendizaje
sobre todo `training_data`, no un modelo nuevo. Al terminar las épocas, los
pesos finales se evaluan una vez sobre entrenamiento y una vez sobre prueba.

### 2. Como se entrena en paralelo

En cada epoca, `training_data` se divide en rangos de indices. Cada goroutine
calcula predicciones, loss y gradientes sobre su rango, sin modificar los pesos
globales. Los resultados locales viajan por un canal. El coordinador suma los
gradientes, los divide entre el total de filas y actualiza los pesos una sola
vez. `WaitGroup` garantiza que todos los workers terminen antes de pasar a la
siguiente epoca.

### 3. Como funciona el preprocesamiento paralelo

El archivo crudo se divide en rangos de bytes. Cada worker abre el mismo archivo
pero lee un rango distinto, ajustando el inicio hasta un salto de linea. Dentro
de su rango ejecuta parseo, deteccion de faltantes, validacion y creacion de
variables. Cada worker devuelve un `chunkResult`. El colector coloca cada chunk
en su indice original, conserva el orden cronologico y suma las estadisticas.

### 4. Metricas

- matriz de confusion: muestra los cuatro tipos de resultado;
- precision: de las alertas emitidas, cuantas fueron correctas;
- recall: cuantos altos consumos reales fueron detectados;
- specificity: cuantos consumos normales fueron reconocidos;
- F1: equilibrio entre precision y recall;
- balanced accuracy: promedio de recall y specificity, util con desbalance;
- positive rate: proporcion real de altos consumos;
- loss: calidad de las probabilidades, no solo de la clase final.

Estas metricas deben analizarse principalmente en `test_metrics`. Una diferencia
grande entre entrenamiento y prueba indicaria sobreajuste o cambio temporal.
