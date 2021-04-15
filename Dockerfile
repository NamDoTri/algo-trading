FROM public.ecr.aws/lambda/python:3.7

WORKDIR /src

COPY /src /src
COPY requirements.txt requirements.txt

RUN yum install -y mysql-devel gcc
RUN pip install -r requirements.txt

CMD ["handler.handler"]