FROM 911845998067.dkr.ecr.eu-central-1.amazonaws.com/python:3.13.7-slim-bookworm


ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . .

# se siamo in prod lanciamo il comando collectstatic
RUN if [ "$DJANGO_ENV" = "prod" ]; then \ 
      python manage.py collectstatic --noinput ; \
    fi

EXPOSE 8080


# specificare 'dev' o 'prod' per la variabile d'ambiente DJANGO_ENV al momento del run
CMD if [ "$DJANGO_ENV" = "prod" ] ; then \
      gunicorn --chdir ./WebApp PagoPA.wsgi:application --bind 0.0.0.0:8080 --workers 3; \
    else \
      python WebApp/manage.py runserver 0.0.0.0:8080 ; \
    fi
