# Presentacion del caso

## 1. Problema y motivacion

El caso estudia el consumo electrico de un hogar a partir de mediciones tomadas
cada minuto. El archivo original contiene mas de dos millones de registros con
fecha, hora, potencia, voltaje, intensidad y consumo de tres submedidores.

El problema consiste en transformar ese volumen de datos crudos en informacion
util para:

- detectar momentos de alto consumo;
- encontrar horas, dias y meses de mayor demanda;
- generar recomendaciones de eficiencia energetica;
- demostrar que la concurrencia reduce el tiempo de procesamiento;
- entrenar un modelo que clasifique si un registro representa alto consumo.

La motivacion es ambiental y operativa. Conocer los patrones de demanda permite
reducir picos evitables, desplazar actividades intensivas a otros horarios y
crear alertas de consumo.

## 2. Limpieza y analisis de datos

### Flujo de limpieza

Cada linea pasa por cuatro pasos:

```text
lectura -> parseo -> validacion -> transformacion
```

Las reglas aplicadas son:

1. Comprobar que existan las nueve columnas esperadas.
2. Convertir fecha y hora a un `Timestamp` valido.
3. Convertir las siete mediciones numericas a `float64`.
4. Detectar `?` o valores vacios como datos faltantes.
5. Eliminar registros con potencia, intensidad o submedidores negativos.
6. Eliminar registros cuyo voltaje sea cero o negativo.
7. Contabilizar filas leidas, limpias, faltantes e invalidas.

### Variables derivadas

Se crean variables que no aparecen directamente en el archivo original:

- `Hour`: hora de 0 a 23, usada para estudiar patrones intradiarios.
- `DayOfWeek`: dia de 0 a 6, usado para comparar dias de la semana.
- `Month`: mes de 1 a 12, usado para observar estacionalidad.
- `SubMeteringTotal`: suma de los tres submedidores.
- `OtherConsumption`: energia del minuto que no esta explicada por los tres
  submedidores.
- `HighConsumption`: etiqueta binaria; vale 1 cuando
  `GlobalActivePower >= 1.528` y 0 en caso contrario.

Las formulas principales son:

```text
SubMeteringTotal = SubMetering1 + SubMetering2 + SubMetering3
ActiveEnergyPerMinute = GlobalActivePower * 1000 / 60
OtherConsumption = ActiveEnergyPerMinute - SubMeteringTotal
```

### Analisis

Los registros limpios se agrupan por hora, dia y mes. Para cada grupo se
calculan:

- cantidad de registros;
- potencia activa promedio;
- cantidad de eventos de alto consumo;
- tasa de alto consumo;
- consumo promedio no explicado por los submedidores.

La tasa de alto consumo es:

```text
HighConsumptionRate = eventos de alto consumo / registros del grupo
```

Esta tasa permite comparar grupos de diferente tamano sin favorecer
automaticamente al que tenga mas observaciones.

Para el reporte ejecutivo se priorizan dos vistas accionables:

- dia y hora;
- fecha de calendario recurrente, por ejemplo `December 25`.

`peak_day_hours` presenta directamente las cinco combinaciones dia-hora con
mayor tasa, sin repetir primero un ranking de dias. `peak_calendar_dates`
combina la misma fecha de todos los anos disponibles para encontrar patrones
recurrentes. Se exigen al menos 1,000 registros por dia-hora y 5,000 por fecha
de calendario para exigir una cobertura cercana a cuatro anos.

## 3. Diseno del modelo ML

Se utiliza regresion logistica binaria porque el objetivo tiene dos clases:
consumo normal (`0`) y alto consumo (`1`).

El target es `HighConsumption`. Las features son:

- `GlobalReactivePower`;
- `Voltage`;
- `GlobalIntensity`;
- `SubMeteringTotal`;
- `OtherConsumption`;
- `Hour`;
- `DayOfWeek`;
- `Month`.

`GlobalActivePower` no se usa como entrada porque la etiqueta se construye con
esa misma variable. Incluirla produciria fuga de informacion y el modelo solo
aprenderia a repetir el umbral.

Antes del entrenamiento, las variables de escalas grandes se dividen por
valores de referencia. Esto evita que una variable domine el gradiente solo por
su unidad de medida. El modelo usa 80 epocas, una tasa de aprendizaje de 0.25 y
un umbral de probabilidad de 0.5 para decidir la clase.

El dataset limpio se mantiene en orden cronologico y se divide de esta forma:

- 80% inicial y mas antiguo: entrenamiento;
- 20% final y mas reciente: prueba.

El corte temporal es mas exigente que mezclar aleatoriamente los registros:
evalua si el modelo aprendido con el pasado funciona sobre observaciones
posteriores. Tambien evita que registros futuros influyan en el entrenamiento.

En cada epoca:

1. Se calcula una probabilidad con la funcion sigmoide.
2. Se compara la probabilidad con la etiqueta real.
3. Se calcula el gradiente y la perdida logistica.
4. Se actualizan los pesos del modelo.
5. Al final se calcula el `accuracy`.

## 4. Paralelizacion del calculo

La concurrencia se aplica en dos partes.

### Carga y limpieza

El archivo se divide en rangos de bytes segun la cantidad de workers. Cada
goroutine abre el archivo, procesa su rango y envia un `chunkResult` por un
canal. El resultado contiene registros limpios y estadisticas locales.

Un colector combina los bloques y suma las estadisticas. Los limites de cada
rango se ajustan hasta el siguiente salto de linea para evitar procesar una
fila incompleta.

### Entrenamiento ML

En cada epoca el dataset se divide en rangos de indices. Cada goroutine calcula
el gradiente y la perdida de su particion. Luego se suman los resultados y se
dividen por el numero total de registros para obtener el gradiente promedio.

Tambien se paraleliza la evaluacion. Cada worker construye una matriz de
confusion local y calcula una parte de la loss. El colector suma todos los
resultados para obtener accuracy, precision, recall, specificity, F1 y balanced
accuracy.

### Sincronizacion

- `WaitGroup` espera la finalizacion de las goroutines.
- Los canales transportan resultados sin compartir directamente los slices
  locales de cada worker.
- El canal se cierra solo despues de que todos los workers terminan.

## 5. Metricas de rendimiento

La terminal muestra el tiempo en milisegundos de:

1. carga y limpieza paralela;
2. escritura del dataset limpio;
3. analisis de demanda y sostenibilidad;
4. escritura de reportes;
5. entrenamiento ML paralelo;
6. escritura del modelo;
7. suma total de las etapas.

Estas mediciones permiten identificar el cuello de botella y comparar
ejecuciones con distinto numero de workers:

```powershell
go -C data-load run ./cmd -workers 1
go -C data-load run ./cmd -workers 4
go -C data-load run ./cmd -workers 8
```

La comparacion debe hacerse sobre el mismo equipo y el mismo archivo. Aumentar
workers no garantiza una mejora ilimitada porque la lectura de disco, la
escritura y la coordinacion tambien tienen costo.

## 6. Resultados de la ejecucion validada

Con cuatro workers se procesaron:

- 2,075,259 filas leidas;
- 2,049,280 filas limpias;
- 25,979 filas descartadas por datos faltantes;
- 0 filas descartadas por valores invalidos.

El corte produjo 1,639,424 filas de entrenamiento y 409,856 filas de prueba.
Sobre prueba se obtuvo:

- accuracy: 80.83%;
- precision: 100%;
- recall: 14.58%;
- specificity: 100%;
- F1: 25.44%;
- balanced accuracy: 57.29%;
- loss: 0.4111;
- matriz: TP=13,408, TN=317,865, FP=0 y FN=78,583.

El accuracy aislado parece bueno, pero el recall muestra que el modelo solo
detecta 14.58% de los altos consumos reales. Es conservador: no produce falsas
alarmas, pero omite demasiados eventos. Para una alerta energetica convendria
bajar el umbral de decision o dar mayor peso a la clase de alto consumo.

Los principales patrones encontrados fueron:

- horas con mayor tasa de alto consumo: 21:00 (56.24%), 20:00 (55.32%) y
  19:00 (49.08%);
- dias principales: sabado (31.54%) y domingo (30.17%);
- meses principales: diciembre (39.34%), enero (39.33%) y febrero (32.20%).

Los nuevos cruces muestran:

- sabado: 19:00 (58.22%), 20:00 (55.32%) y 21:00 (53.41%);
- domingo: 21:00 (63.44%), 20:00 (62.22%) y 19:00 (58.25%);
- diciembre: sus dias principales son sabado (49.55%) y domingo (48.76%);
- enero: sus horas principales son 19:00 (78.75%), 20:00 (78.40%) y
  21:00 (71.25%).

Esto permite pasar de una conclusion general, como "el fin de semana consume
mas", a una accion concreta, como "priorizar alertas los domingos entre 19:00
y 21:00".

En la corrida de validacion, la suma de etapas fue aproximadamente 21,642 ms.
La escritura de los tres datasets fue la etapa mas costosa, con 12,981 ms,
seguida por entrenamiento y evaluacion con 5,619 ms. Estos valores son una
muestra del equipo de prueba y pueden cambiar en otra computadora.

## 7. Conclusion

La solucion convierte un archivo grande y con datos faltantes en productos
utiles para analisis y prediccion. La paralelizacion acelera las operaciones
intensivas de lectura, limpieza y calculo de gradientes. Los resultados muestran
dos franjas relevantes para acciones de ahorro: la noche, especialmente entre
19:00 y 21:00, y la manana alrededor de las 07:00 y 08:00.
