from fastapi import FastAPI

try:
    from producer.routers import rabbitmq
except ModuleNotFoundError:
    from routers import rabbitmq

app = FastAPI()

app.include_router(rabbitmq.router)
