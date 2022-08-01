LOCAL_TAG:=$(shell date +"%Y-%m-%d-%H-%M")
LOCAL_IMAGE_NAME:=citibike-model-duration:${LOCAL_TAG}

test:
	pytest tests/

quality_checks:
	isort .
	black .

build_webservice: quality_checks
	docker build -t ${LOCAL_IMAGE_NAME} .
	docker run --env-file .env -d -it \
		--rm -v $(pwd):/app \
		-p 9696:9696 ${LOCAL_IMAGE_NAME}
	python test.py

integration_test: build
	LOCAL_IMAGE_NAME=${LOCAL_IMAGE_NAME} bash integraton-test/run.sh

publish: build integration_test
	LOCAL_IMAGE_NAME=${LOCAL_IMAGE_NAME} bash scripts/publish.sh

setup:
	pipenv install --dev