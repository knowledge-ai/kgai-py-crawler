help: ## Lists available commands and their explanation
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

test: ## runs all tests through pytest
	pipenv shell pytest -m "not slow"

test-all: ## runs all tests through pytest
	pipenv shell pytest -m "not slow"

pre-commit-encrypt: ## installs a git pre-commit hook to encrypt .env file
	cp ./pre-commit ./.git/hooks/ & chmod +x ./.git/hooks/pre-commit

build-docker: ## builds docker image based on release scripts
	. ./release_util.sh

build-release: ## release a new version of the app and build the docker for it
	export APP_RELEASE=1 && . ./release_util.sh && unset APP_RELEASE

run-app: ## runs the dockerized app
	docker run knowledgeai/kgai-py-crawler:$(shell cat "./version.md")

run-app-interactive: ## runs the dockerized app interactive mode
	docker run -it knowledgeai/kgai-py-crawler:$(shell cat "./version.md")

push-image: ## pushes the image to the docker repo
	docker push knowledgeai/kgai-py-crawler:$(shell cat "./version.md")
	


