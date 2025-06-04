# OCR Pipeline

# Deployment

# S3 Event Notifications + SQS + Flask Service

- A queue-based approach that:
  1. S3 sends notifications to an SQS queue when files are uploaded
  2. Flask service polls the SQS queue for new messages
  3. Process files as messages are received
