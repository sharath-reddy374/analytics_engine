.PHONY: help install dev test clean seed-data run-pipeline process-user format lint setup-dev test-connection

help:
	@echo "EdYou AI Engine - Available commands:"
	@echo "  install        - Install Python dependencies"
	@echo "  dev            - Run development server locally"
	@echo "  test           - Run tests"
	@echo "  test-connection - Test DynamoDB connection"
	@echo "  seed-data      - Seed database with sample data"
	@echo "  run-pipeline   - Run daily pipeline manually"
	@echo "  process-user   - Process a single user by email"
	@echo "  clean          - Clean up temporary files"
	@echo "  format         - Format code"
	@echo "  lint           - Lint code"
	@echo "  setup-dev      - Set up development environment"

install:
	pip install -r requirements.txt

dev:
	python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

test:
	pytest tests/ -v

test-connection:
	python scripts/test_dynamodb_connection.py

seed-data:
	python scripts/seed_sample_data.py

run-pipeline:
	python scripts/daily_pipeline.py

process-user:
	@if [ -z "$(EMAIL)" ]; then \
		echo "Usage: make process-user EMAIL=user@example.com"; \
		exit 1; \
	fi
	python scripts/process_single_user.py $(EMAIL)

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache/
	rm -rf logs/*.log

# Development helpers
format:
	black .
	isort .

lint:
	flake8 .
	mypy .

setup-dev: install
	@echo "Setting up development environment..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "Created .env file from .env.example"; \
		echo "Please update .env with your AWS credentials"; \
	fi
	@echo "Development environment ready!"
	@echo "Run 'make test-connection' to verify DynamoDB access"
	@echo "Run 'make dev' to start the server"
