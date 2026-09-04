# Solução de problemas

## `load_postgres` encontrou zero registros

Confira se Bronze e Silver receberam executores no Spark Master:

[http://localhost:8080](http://localhost:8080)

Os dois devem aparecer como aplicações ativas. A configuração local reserva
512 MiB e um core para cada executor.

O loader agora falha explicitamente quando encontra menos registros que o lote
publicado. Aguarde o retry do Airflow ou verifique os logs do Silver.

## PostgreSQL recusou conexão na porta 5434

```bash
docker compose --env-file .env \
  -f infrastructure/analytics/docker-compose.yml up -d postgres
```

Espere o estado `healthy` antes de repetir `load_postgres`.

## Grafana não mostra dados

Verifique o intervalo de tempo. Os eventos PaySim são simulados a partir de
2023, e não usam o horário atual como `event_time`.

## Streamlit mostra dados antigos

Clique em **Atualizar dados**. As consultas usam cache de 30 segundos.

## Uso elevado de memória

```bash
docker stats
free -h
```

Suba Grafana e Streamlit somente depois da DAG quando a máquina tiver pouca
memória. No WSL2, confirme também quanto da memória física foi disponibilizado
à VM.
