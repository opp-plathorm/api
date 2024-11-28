from fastapi import FastAPI
import model_messege as mm
from config_kafka import producer, consumer
import threading
app = FastAPI()

auth_results = {}

def consume_auth_results():
    for message in consumer:
        # Сохраняем результат авторизации в глобальную переменную
        auth_results[message.value['username']] = message.value['success']
        
threading.Thread(target=consume_auth_results, daemon=True).start()

@app.post("/send/")
async def send_message(message: mm.Message):
    try:
        producer.send('my_topic', value=message.dict())
        producer.flush()  # Убедитесь, что все сообщения отправлены
        return {"status": "success", "message": f"Сообщение отправлено: {message}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/login")
async def login(auth_request: mm.Login):
    username = auth_request.username
    password = auth_request.password

    # Отправка данных в Kafka для обработки
    producer.send("auth-topic", {'username': username, 'password': password})
    producer.flush()

    # Ожидание результата авторизации
    while username not in auth_results:
        pass  # Блокируем поток до получения результата

    # Получаем результат авторизации
    success = auth_results[username]
    message = 'Авторизация успешна.' if success else 'Неверное имя пользователя или пароль.'

    return {"message": message}
    
@app.on_event("shutdown")
def shutdown_event():
    producer.close()  # Закрытие продюсера при завершении приложения

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
