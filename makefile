SHELL := /bin/bash
CODE = src/common src/routers src/db src/dto src/middleware src/migrations/env.py src/repositories src/services src/main.py
TEST = pytest --verbosity=2 --strict-markers ${arg} -k "${k}" --cov-report term-missing


lint:
	ruff check $(CODE)

format:
	ruff format $(CODE)

lint-fix:
	ruff check $(CODE) --fix

test:
	${TEST} --cov=. --cov-fail-under=0

test-fast:
	${TEST} -v -m "repo or service or view" --cov=. --cov-fail-under=0


check:
	@echo "\033[1;34m🚀 Check started: $$(date) 🤞\033[0m"
	@start_time=$$(date +%s); \
	make format lint ; \
	end_time=$$(date +%s); \
	elapsed_time=$$(($$end_time - $$start_time)); \
	echo "\033[1;34m✅ Check finished: $$(date)\n⏱️Elapsed Time: $$(($$elapsed_time / 60)) minutes \033[0m"

check-fix:
	@echo "\033[1;34m🚀 Check started: $$(date) 🤞\033[0m"
	@start_time=$$(date +%s); \
	make format lint-fix ; \
	end_time=$$(date +%s); \
	elapsed_time=$$(($$end_time - $$start_time)); \
	echo "\033[1;34m✅ Check finished: $$(date)\n⏱️Elapsed Time: $$(($$elapsed_time / 60)) minutes \033[0m"

run_db: ## run database
	docker-compose up -d


pipe_test_group_1:
	${TEST} -v -m "repo" --cov=.

pipe_test_group_2:
	${TEST} -v -m "service" --cov=.

pipe_test_group_3:
	${TEST} -v -m "view" --cov=.


####################

dev-build:
	cp /home/.npmrc ./
	docker build  -t ${NEXUS}:8083/${CI_PROJECT_NAME}:${CI_PIPELINE_ID}-${CI_COMMIT_SHORT_SHA} . -f Dockerfile.kuber \

dev-push:
	docker push ${NEXUS}:8083/${CI_PROJECT_NAME}:${CI_PIPELINE_ID}-${CI_COMMIT_SHORT_SHA}

dev-deploy:
	/home/gitlab-jobs/dev-kuber/Charts/week-week/scripts/deploy.sh
