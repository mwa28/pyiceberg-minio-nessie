FROM public.ecr.aws/lambda/python:3.13-arm64

RUN mkdir /home/lambdauser \
  && chown -R 1000:1000 /home/lambdauser

USER 1000
ENV PYTHONUSERBASE=/home/lambdauser/.local

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip3 install --user -r requirements.txt --no-cache-dir
# Copy function code
COPY ingest.py ${LAMBDA_TASK_ROOT}

CMD [ "ingest.handler" ]

