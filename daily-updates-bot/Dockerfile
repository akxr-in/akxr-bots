FROM python:3.10-slim
WORKDIR /daily-updates-bot
COPY send-message.py .
RUN pip install zulip
CMD ["python3", "/daily-updates-bot/send-message.py"]
