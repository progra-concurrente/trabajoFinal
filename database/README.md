# Almacenamiento

MongoDB conserva:

- `forecasts`: diagnóstico, pronóstico y recomendación;
- `models`: versiones de modelos predictivos;
- `training_runs`: épocas y métricas;
- `cluster_events`: conexiones, fallos y reasignaciones.

Redis conserva respuestas de pronóstico precalculadas, contexto histórico,
estado reciente del cluster y eventos colaborativos.
