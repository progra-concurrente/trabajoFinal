# Presentación de PowerSight

## Problema

Los hogares suelen conocer el consumo cuando ya ocurrió. PowerSight busca
anticipar un período de alto consumo para permitir que el usuario separe
cargas, postergue equipos intensivos y reduzca desperdicios.

## Solución

1. Analiza patrones históricos por hora, día, mes y día-hora.
2. Diagnostica el consumo actual sin ML.
3. Usa una ventana reciente de 15 minutos para pronosticar los próximos 30.
4. Entrega recomendaciones concretas y alertas WebSocket.

## Aporte concurrente y distribuido

- limpieza y ventanas con workers, goroutines y channels;
- tres nodos ML por TCP;
- gradientes locales concurrentes dentro de cada nodo;
- agregación y tolerancia a caída en el coordinador;
- procesamiento batch y persistencia asíncrona en la API.

## Impacto

El resultado no se limita a decir “consumo alto”. Indica si el hogar aún está
a tiempo de prevenir un pico futuro y qué acción puede tomar durante la
ventana pronosticada.
