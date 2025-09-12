FROM python:3.13


ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . .

# se siamo in prod lanciamo il comando collectstatic
RUN if [ "$DJANGO_ENV" = "prod" ]; then \ 
      python manage.py collectstatic --noinput ; \
    fi

EXPOSE 8000


# specificare 'dev' o 'prod' per la variabile d'ambiente DJANGO_ENV al momento del run
CMD if [ "$DJANGO_ENV" = "prod" ] ; then \
      gunicorn PagoPA.wsgi:application --bind 0.0.0.0:8000 --workers 3 ; \
    else \
      python manage.py runserver 0.0.0.0:8000 ; \
    fi
