# fastapi-rabbitmq

Projeto FastAPI que publica um CSV no RabbitMQ e um consumer processa o arquivo para inserir dados em lote no Postgres (async).

## Requisitos

- Python 3.12+
- Docker (para Postgres e RabbitMQ)

## Dependências

As principais dependências:

- `fastapi`
- `pika`
- `sqlalchemy` (async)
- `asyncpg`
- `alembic`

Instalação com `uv` (inclui instalação do `uv`):

Windows (PowerShell):

```/dev/null/commands.sh#L1-1
powershell -ExecutionPolicy Bypass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

macOS/Linux:

```/dev/null/commands.sh#L1-1
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Instalando dependências do projeto:

```/dev/null/commands.sh#L1-1
uv sync
```

## Subindo Postgres e RabbitMQ

```/dev/null/commands.sh#L1-1
docker compose up -d
```

RabbitMQ UI: `http://localhost:15672`  
Credenciais padrão: `guest / guest`

## Variáveis de ambiente

As configurações usam defaults, mas você pode definir:

- `RABBITMQ_HOST`
- `RABBITMQ_PORT`
- `RABBITMQ_USERNAME`
- `RABBITMQ_PASSWORD`

O banco está configurado para:

```
postgresql+asyncpg://postgres:postgres@localhost/fastapi_rabbitmq
```

## Migrations (Alembic)

### Inicialização

A pasta de migrations está em `migrations/`.

### Aplicar migration

```/dev/null/commands.sh#L1-1
alembic upgrade head
```

> O `env.py` já está preparado para async.

## Rodando a API

```/dev/null/commands.sh#L1-1
uv run fastapi dev main.py
```

## Rodando o Consumer

Abra outro terminal e rode:

```/dev/null/commands.sh#L1-1
python main_consumer.py
```

## Enviando CSV pela rota

Endpoint:

```
POST /publish/finance-yahoo/csv
```

O `country` é passado pela rota e gravado em todas as linhas inseridas.

## Estrutura de dados esperada no CSV

Headers esperados:

- `symbol`
- `name`
- `price`

Exemplo:

```/dev/null/example.csv#L1-3
symbol,name,price
PETR4,Petrobras,36.10
VALE3,Vale,59.20
```
