from fastapi import FastAPI

from routers import yahoo_finance

app = FastAPI()

app.include_router(yahoo_finance.router)
