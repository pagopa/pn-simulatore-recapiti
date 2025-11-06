#!/bin/sh

# interrompe lo script se vi sono errori
set -e

echo "START DJANGO DB MIGRATE"
python WebApp/manage.py migrate --fake
python WebApp/manage.py migrate --noinput
echo "END DJANGO DB MIGRATE"

echo "START DJANGO COLLECTSTATIC"
#python WebApp/manage.py collectstatic --noinput
echo "END DJANGO COLLECTSTATIC"

exec gunicorn --chdir ./WebApp PagoPA.wsgi:application --bind 0.0.0.0:8080 --workers 3