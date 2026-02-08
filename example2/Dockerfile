FROM public.ecr.aws/lambda/python:3.9

COPY lambda_handler.py .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["lambda_handler.lambda_handler"]
