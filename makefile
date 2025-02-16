lint:
	ruff check

lint-fix:
	ruff check --fix

lint-format:
	ruff format

lint-isort:
	ruff check --select I

lint-isort-fix:
	ruff check --select I --fix

lint-base:
	ruff check --fix
	ruff check --select I --fix

check-hints:
	mypy .

dc-up:
	docker compose up -d --build

dc-down:
	docker compose down -v
