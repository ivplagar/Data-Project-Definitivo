FROM python:3.9.12



COPY ./main.py /app/
COPY ./requirements.txt /app/

WORKDIR /app/

RUN pip install psycopg2
RUN pip install geojson
RUN pip install -r requirements.txt
RUN pip install pandas 

CMD [ "python3", "main.py" ]