from fastapi import FastAPI
import model_messege as mm
from config_kafka import producer, consumer
import threading
app = FastAPI()

auth_results = {}

def consume_auth_results():
    for message in consumer:
        # Сохраняем результат авторизации в глобальную переменную
        print("5")
        auth_results[message.value['login']] = message.value['success']
        
threading.Thread(target=consume_auth_results, daemon=True).start()

@app.post("/api/login")
async def login(auth_request: mm.Login):
    print("1")
    login = auth_request.login
    password = auth_request.password
    print("2")
    # Отправка данных в Kafka для обработки
    producer.send("auth-topic", {'login': login, 'password': password})
    print("3")
    producer.flush()
    print("4")
    # Ожидание результата авторизации
    while login not in auth_results:
        
        pass
    print("6")
    success = auth_results[login]
    print("7")
    message = 'Авторизация успешна.' if success else 'Неверное имя пользователя или пароль.'
    print("8")
    return {"message": message}
    
@app.on_event("shutdown")
def shutdown_event():
    producer.close()  # Закрытие продюсера при завершении приложения

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
