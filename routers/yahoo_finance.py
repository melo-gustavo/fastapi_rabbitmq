from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from business.yahoo_finance import YahooFinanceProducer

router = APIRouter()


@router.post("/publish/finance-yahoo/csv")
async def send_csv(
    file: UploadFile = File(...), country: str = Form("")
) -> dict[str, str | int]:
    try:
        return await YahooFinanceProducer().publish_csv(file, country)
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
