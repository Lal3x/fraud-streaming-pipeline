# Grafana

O Grafana usa o PostgreSQL Analytics como datasource e provisiona o dashboard
`Fraud Analytics / Fraud Streaming — Visão geral` automaticamente.

## Iniciar pela raiz

Garanta que o PostgreSQL Analytics esteja ativo:

```bash
docker compose --env-file .env \
  -f infrastructure/analytics/docker-compose.yml up -d postgres
```

Todas as configurações ficam no `.env` da raiz. Defina nele uma senha segura em
`GRAFANA_ADMIN_PASSWORD` e mantenha as variáveis `ANALYTICS_DB_*` iguais às do
banco Analytics.

Depois, suba o Grafana:

```bash
docker compose --env-file .env \
  -f infrastructure/grafana/docker-compose.yml up -d
```

Acesse <http://localhost:3000> com o usuário e senha definidos em
`GRAFANA_ADMIN_USER` e `GRAFANA_ADMIN_PASSWORD`.

O dashboard consulta as tabelas criadas pelo dbt. Caso estejam ausentes ou
desatualizadas, execute `dbt build` antes de abrir os painéis.

## Operação

```bash
docker compose --env-file .env \
  -f infrastructure/grafana/docker-compose.yml ps

docker compose --env-file .env \
  -f infrastructure/grafana/docker-compose.yml logs -f grafana

docker compose --env-file .env \
  -f infrastructure/grafana/docker-compose.yml down
```

O comando `down` preserva o volume `fraud-grafana-data`. Para segurança, o
cadastro público e o acesso anônimo ficam desabilitados.
