.PHONY: all func_test clean clean_pycache \
        help create_env start_env stop_env
SESSION_FILE=.session_conf.sav
TEST_VENV=venv

venv:
	python3 -m venv ${TEST_VENV}

	${TEST_VENV}/bin/pip install --no-cache-dir --disable-pip-version-check \
		-r requirements-test.txt

gitcheck:
	git --no-pager diff HEAD~1 --check

func_test: venv
	${TEST_VENV}/bin/behave --show-timings --stop @wal-g.featureset

clean: stop_env clean_pycache
	rm -rf staging venv ${SESSION_FILE}
	rm -rf .cache
	rm -rf *.egg-info htmlcov .coverage*
	rm -rf .hypothesis
	rm -rf wal-g

clean_pycache:
	find . -name __pycache__ -type d -exec rm -rf {} +

clean_env: stop_env
	rm -rf staging ${SESSION_FILE}

create_env: ${TEST_VENV}
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m env_control create

start_env: create_env
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m env_control start

stop_env:
	test -d ${TEST_VENV}/bin && test -f ${SESSION_FILE} && \
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m env_control stop || true

help:
	@echo 'Targets:'
	@echo '  integration_test   Run integration tests.'
	@echo '  create_env         Create test environment.'
	@echo '  start_env          Start test environment runtime.'
	@echo '  stop_env           Stop test environment runtime.'
	@echo '  clean              Clean up test environment left from the previous test run.'
	@echo '  clean_pycache      Clean up __pycache__ directories.'
	@echo '  help               Show this help message.'
