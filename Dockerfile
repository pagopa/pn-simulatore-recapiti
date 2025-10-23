FROM 911845998067.dkr.ecr.eu-central-1.amazonaws.com/python:3.13.7-slim-bookworm


ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y locales && sed -i '/it_IT.UTF-8/s/^# //g' /etc/locale.gen && locale-gen
ENV LANG=it_IT.UTF-8
ENV LANGUAGE=it_IT:it
ENV LC_ALL=it_IT.UTF-8

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . .

RUN python manage.py collectstatic --noinput ;
RUN python manage.py migrate --noinput ;


EXPOSE 8080

CMD gunicorn --chdir ./WebApp PagoPA.wsgi:application --bind 0.0.0.0:8080 --workers 3;
