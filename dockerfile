# Используем официальный образ Python
FROM python:3.11.8-slim AS build
# Устанавливаем рабочую директорию
WORKDIR /app
# Копируем файл с зависимостями в контейнер
COPY requirements.txt .
# Устанавливаем необходимые зависимости
RUN pip install -r requirements.txt
# Копируем файл с кодом в контейнер
COPY main.py .

FROM build AS final
WORKDIR /app
# Команда для запуска приложения
CMD ["python", "main.py"]
