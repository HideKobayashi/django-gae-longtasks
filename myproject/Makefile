migrate:
	python manage.py makemigrations
	python manage.py migrate

run: migrate
	python manage.py runserver 8080

test: migrate
	pytest -v -x
