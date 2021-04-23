FROM public.ecr.aws/lambda/python:3.8

COPY . / ./

RUN yum install -y mysql-devel gcc
RUN pip install -r requirements.txt
RUN pwd

CMD ["position_trading.main"]   

