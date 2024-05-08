lint:
	poetry run flake8 --max-line-length=200 --extend-ignore=W503 hubspot --show-source
	poetry run black ./ --check

format:
	isort ./
	poetry run black ./

format-lint: format lint