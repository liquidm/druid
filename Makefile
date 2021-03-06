PROJECT_NAME = druid

PROJECT_ROOT = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_BIN = $(PROJECT_NAME)

PROJECT_REV = $(shell git rev-parse HEAD)
PROJECT_IMAGE = registry.build.lqm.io/$(PROJECT_NAME):$(PROJECT_REV)

PROJECT_ARTIFACT = /tmp/$(PROJECT_NAME)_$(PROJECT_REV)

.PHONY: all build image artifact publish-image publish-artifact

default: build

all: build

build:
	mvn clean install -DskipTests -Pdist,bundle-contrib-exts --quiet

image:
	docker build -t $(PROJECT_IMAGE) .

artifact: image
	$(eval CID := $(shell docker create $(PROJECT_IMAGE)))
	docker cp $(CID):/tmp/druid/distribution/target $(PROJECT_ARTIFACT)
	docker rm $(CID)

publish-image: image
	docker push $(PROJECT_IMAGE)

publish-artifact: artifact
	cd $(PROJECT_ARTIFACT) && (find . -maxdepth 1 -name *.tar.gz | sed -e 's,^\./,,' | xargs -I '{}' gsutil cp {} gs://lqm-artifact-storage/$(PROJECT_NAME)/${PROJECT_REV})
