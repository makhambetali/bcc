from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

from analyzer import ClientAnalyzer

class Item(BaseModel):
    id: int

app = FastAPI()
origins = ["*"]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)
@app.post("/process_id")
async def process_id(item: Item):
    print(f"Получен ID: {item.id}")
    

    if item.id >= 1 and item.id <= 60:
        try:
            ca = ClientAnalyzer(item.id)
            row = ca.execute()
            return {"status": "success", "received_id": item.id, "row": row}
        except:
            return {"status": "fail", "received_id": item.id, "reason": "Ошибка при обработке информации"}
    
    return {"status": "fail","received_id": item.id, "reason": "ID клиента должен быть в диапазоне 1-60"}