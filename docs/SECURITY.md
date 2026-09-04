# Segurança

## Escopo

Este repositório é um ambiente educacional executado localmente. Ele não representa
um sistema antifraude de produção e não deve processar dados financeiros reais.

## Credenciais

- Copie `.env.example` para `.env` e substitua todos os valores `change-me`.
- Nunca versione `.env`, chaves, tokens, certificados, dumps ou arquivos do PaySim.
- As credenciais de desenvolvimento não devem ser reutilizadas fora deste laboratório.

## Docker socket

O Airflow monta `/var/run/docker.sock` para iniciar jobs efêmeros do Docker Compose.
Esse socket equivale a acesso privilegiado ao daemon Docker. Use o projeto somente
em uma máquina local confiável. Em ambientes compartilhados, substitua a montagem
direta por um executor isolado ou um proxy com permissões restritas.

## Serviços locais

Kafka, MinIO, PostgreSQL, Grafana, Streamlit e interfaces administrativas são
publicados apenas para demonstração local. Não exponha essas portas diretamente
à Internet.

## Dados

O PaySim é sintético e não é versionado. Os diretórios de dados, checkpoints,
Parquet, logs, artefatos dbt e modelos gerados permanecem fora do Git.

## Relato de vulnerabilidades

Não abra uma issue pública contendo segredos ou detalhes exploráveis. Envie o relato
de forma privada ao responsável pelo repositório por meio do recurso de security
advisories do GitHub.
