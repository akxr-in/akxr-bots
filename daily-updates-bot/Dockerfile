FROM python:3.10-slim
WORKDIR /daily-updates-bot
COPY send_update_request.py .
COPY ~/.zuliprc .
RUN pip install zulip
CMD ["python3", "/daily-updates-bot/send_update_request.py"]
