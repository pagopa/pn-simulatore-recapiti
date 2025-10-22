FROM 911845998067.dkr.ecr.eu-central-1.amazonaws.com/python:3.13.7-slim-bookworm


ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# installiamo lingua locale e impostiamo la lingua italiana di default
RUN apt-get update && apt-get install -y locales && sed -i '/it_IT.UTF-8/s/^# //g' /etc/locale.gen && locale-gen
ENV LANG=it_IT.UTF-8
ENV LANGUAGE=it_IT:it
ENV LC_ALL=it_IT.UTF-8

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . .

# se siamo in prod lanciamo il comando collectstatic e il comando migrate
RUN if [ "$DJANGO_ENV" = "prod" ]; then \ 
      python manage.py collectstatic --noinput ; \
      python manage.py migrate --noinput ; \
    fi

EXPOSE 8080


# specificare 'dev' o 'prod' per la variabile d'ambiente DJANGO_ENV al momento del run
CMD if [ "$DJANGO_ENV" = "prod" ] ; then \
      gunicorn --chdir ./WebApp PagoPA.wsgi:application --bind 0.0.0.0:8080 --workers 3; \
    else \
      python WebApp/manage.py runserver 0.0.0.0:8080 ; \
    fi
