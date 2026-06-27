# Proceso predictivo de PowerSight

## Objetivo

Pronosticar si habrá consumo alto sostenido durante los próximos 30 minutos,
para que el hogar pueda actuar antes de que el pico ocurra o se prolongue.

## Target

Para cada minuto válido:

```text
FutureSustainedHighConsumption30m = 1
si al menos 10 de los próximos 30 minutos
superan 1.528 kW
```

La etiqueta utiliza únicamente el futuro. Las variables de entrada utilizan
los 15 minutos anteriores, incluyendo la lectura actual.

## Variables

- potencia activa actual;
- promedio, máximo, desviación y tendencia de potencia en 15 minutos;
- potencia reactiva, voltaje e intensidad actuales;
- consumo submedido y no identificado;
- hora, día y mes;
- tasa histórica de alto consumo para la hora;
- tasa histórica para la combinación día-hora.

## Concurrencia

La limpieza divide el archivo crudo en rangos de bytes. Después, otro pool de
workers construye ventanas predictivas independientes y las combina en orden
mediante channels.

Durante el entrenamiento distribuido, el coordinador asigna shards a los nodos
por TCP. Cada nodo vuelve a dividir su shard entre goroutines, combina
gradientes parciales por un channel y devuelve el resultado al coordinador.

## Separación temporal

El 80% inicial se usa para entrenamiento y el 20% posterior para prueba. No se
mezclan registros futuros con el pasado. Las tasas históricas empleadas como
features se calculan con la parte inicial del dataset.

## Resultado para el usuario

La API recibe 15 lecturas consecutivas y devuelve:

- estado actual;
- probabilidad de alto consumo sostenido en los siguientes 30 minutos;
- contexto histórico de la franja;
- recomendación preventiva;
- nodo utilizado y tiempo de respuesta.
