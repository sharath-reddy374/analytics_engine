.PHONY: help install dev test clean docker-up docker-down seed-data run-pipeline process-user format lint setup-dev docker-clean

help:
	@echo "EdYou AI Engine - Available commands:"
	@echo "  install     - Install Python dependencies"
	@echo "  dev         - Run development server"
	@echo "  test        - Run tests"
	@echo "  docker-up   - Start all services with Docker Compose"
	@echo "  docker-down - Stop all Docker services"
	@echo "  seed-data   - Seed database with sample data"
	@echo "  run-pipeline - Run daily pipeline manually"
	@echo "  process-user - Process a single user by email"
	@echo "  clean       - Clean up temporary files"
	@echo "  format      - Format code"
	@echo "  lint        - Lint code"
	@echo "  setup-dev   - Set up development environment"
	@echo "  docker-clean - Remove orphan containers and unused resources"

install:
	pip install -r requirements.txt

dev:
	python run.py

test:
	pytest tests/ -v

docker-up:
	docker-compose up -d --remove-orphans
	@echo "Services starting... Wait 30 seconds then run 'make process-user EMAIL=your@email.com'"

docker-down:
	docker-compose down --remove-orphans

docker-clean:
	docker-compose down --remove-orphans
	docker system prune -f
	docker volume prune -f

seed-data:
	python scripts/seed_sample_data.py

run-pipeline:
	python scripts/daily_pipeline.py

process-user:
	@if [ -z "$(EMAIL)" ]; then \
		echo "Usage: make process-user EMAIL=user@example.com"; \
		exit 1; \
	fi
	docker-compose exec edyou-engine python scripts/process_single_user.py $(EMAIL)

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
	pre-commit install
