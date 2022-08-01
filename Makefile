quality_checks:
	isort .
	black .

setup:
	pipenv install
	pipenv install --dev