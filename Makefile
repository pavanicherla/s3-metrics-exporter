IMAGE_REPO = seanlim0101/s3-metrics-exporter
IMAGE_TAG = dev

build:
	find . -type d -name '__pycache__' | xargs rm -rf
	docker build . -t ${IMAGE_REPO}:${IMAGE_TAG}

tag:
	docker tag ${IMAGE_REPO}:${IMAGE_TAG} ${IMAGE_REPO}:$(TAG)
	docker push ${IMAGE_REPO}:$(TAG)

test:
	echo 'no test defined'

scan:
	docker scout cves local://${IMAGE_REPO}:${IMAGE_TAG}

scan-fix:
	docker scout recommendations local://${IMAGE_REPO}:${IMAGE_TAG}

quickview:
	docker scout quickview local://${IMAGE_REPO}:${IMAGE_TAG}

run:
	docker run -dp 8080:8080 ${IMAGE_REPO}:${IMAGE_TAG}

run-interactive:
	docker run -it -p 8080:8080 --rm ${IMAGE_REPO}:${IMAGE_TAG} bash

run-mounted:
	docker run -p 8080:8080 --rm -v "${PWD}:/opt" ${IMAGE_REPO}:${IMAGE_TAG}

run-interactive-mounted:
	docker run -it -p 8080:8080 --rm -v "${PWD}:/opt" ${IMAGE_REPO}:${IMAGE_TAG} bash
