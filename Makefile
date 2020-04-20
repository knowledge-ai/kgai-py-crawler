help: ## Lists available commands and their explanation
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

test: ## runs all tests through pytest
	pipenv shell pytest -m "not slow"

test-all: ## runs all tests through pytest
	pipenv shell pytest -m "not slow"

pre-commit-encrypt: ## installs a git pre-commit hook to encrypt .env file
	cp ./pre-commit ./.git/hooks/ & chmod +x ./.git/hooks/pre-commit
	


