# Streamlit

O dashboard consulta os marts do dbt no PostgreSQL Analytics e apresenta KPIs,
séries diárias, alertas, diagnóstico de Benford, avaliação do Isolation Forest e
saúde das cargas.

Com o PostgreSQL Analytics ativo e o `dbt build` concluído, execute pela raiz:

```bash
docker compose --env-file .env -f infrastructure/streamlit/docker-compose.yml up -d --build
```

Acesse <http://localhost:8501>. A porta pode ser alterada com
`STREAMLIT_PORT` no `.env`.

Para acompanhar ou encerrar:

```bash
docker compose --env-file .env -f infrastructure/streamlit/docker-compose.yml logs -f dashboard
docker compose --env-file .env -f infrastructure/streamlit/docker-compose.yml down
```
