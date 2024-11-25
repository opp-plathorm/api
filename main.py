from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json

app = FastAPI()

# Создание экземпляра KafkaProducer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # адрес вашего Kafka брокера
    api_version = (0,10,2),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # сериализация сообщений в JSON
)

# Модель данных для входящих сообщений
class Message(BaseModel):
    key: str
    value: str

@app.post("/send/")
async def send_message(message: Message):
    try:
        producer.send('my_topic', value=message.dict())
        producer.flush()  # Убедитесь, что все сообщения отправлены
        return {"status": "success", "message": f"Сообщение отправлено: {message}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.on_event("shutdown")
def shutdown_event():
    producer.close()  # Закрытие продюсера при завершении приложения

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
