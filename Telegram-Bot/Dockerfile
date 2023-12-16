FROM python:3.9-slim

WORKDIR /Telegram-Bot

COPY requirements.txt ./
COPY main.py ./
COPY superjob_analytics_bot.py ./
COPY keys.py ./

RUN pip install -r requirements.txt

CMD ["python", "main.py"]