FROM python:3.9-slim

WORKDIR /Telegram-Bot

COPY requirements_tg.txt ./
COPY main_tg.py ./
COPY superjob_analytics_bot.py ./
COPY keys_tg.py ./

RUN pip install -r requirements_tg.txt

CMD ["python", "main_tg.py"]