FROM python:3.9-slim

WORKDIR /app


ARG S3_BUCKET
ARG AWS_REGION
ARG SQS_QUEUE_URL
ARG SQS_DLQ_URL
ARG ep_stage_db_user
ARG ep_stage_db_host
ARG ep_stage_db_port
ARG ep_stage_db


ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG ep_stage_db_password


ENV S3_BUCKET=${S3_BUCKET}
ENV AWS_REGION=${AWS_REGION}
ENV SQS_QUEUE_URL=${SQS_QUEUE_URL}
ENV SQS_DLQ_URL=${SQS_DLQ_URL}
ENV ep_stage_db_user=${ep_stage_db_user}
ENV ep_stage_db_host=${ep_stage_db_host}
ENV ep_stage_db_port=${ep_stage_db_port}
ENV ep_stage_db=${ep_stage_db}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 5001
CMD ["python", "main.py"]
