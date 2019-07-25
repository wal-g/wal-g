.PHONY: all build lint test test_integration test_unit clean clean_pycache \
        install debuild debian_changelog clean-debuild format help \
        create_env start_env stop_env
SESSION_FILE=.session_conf.sav
TEST_VENV=venv


all: integration_test

venv:
	python3 -m venv ${TEST_VENV}

	${TEST_VENV}/bin/pip install --no-cache-dir --disable-pip-version-check \
		-r requirements.txt \
		-r requirements-test.txt

gitcheck:
	git --no-pager diff HEAD~1 --check

integration_test: venv
	${TEST_VENV}/bin/behave --show-timings --stop @wal-g.featureset

clean: stop_env clean_pycache
	rm -rf staging venv ${SESSION_FILE}
	rm -rf .cache
	rm -rf *.egg-info htmlcov .coverage*
	rm -rf .hypothesis
	rm wal-g

clean_pycache:
	find . -name __pycache__ -type d -exec rm -rf {} +

clean_env: stop_env
	rm -rf staging ${SESSION_FILE}

create_env: ${TEST_VENV}
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control create

start_env: create_env
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control start

stop_env:
	test -d ${TEST_VENV}/bin && test -f ${SESSION_FILE} && \
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control stop || true

help:
	@echo 'Targets:'
	@echo "  build (default)    Build project (it's currently no-op)."
	@echo '  all                Alias for "lint integration_test".'
	@echo '  test               Alias for "lint ".'
	@echo '  lint               Run linter tools on tests source code.'
	@echo '  integration_test   Run integration tests.'
	@echo '  create_env         Create test environment.'
	@echo '  start_env          Start test environment runtime.'
	@echo '  stop_env           Stop test environment runtime.'
	@echo '  clean              Clean up test environment left from the previous test run.'
	@echo '  clean_pycache      Clean up __pycache__ directories.'
	@echo "  debuild            Build Debian package."
	@echo "  clean-debuild      Clean up build and test artifacts including ones produced by"
	@echo "                     debuild target outside the project worksapce."
	@echo '                     isort and yapf tools.'
	@echo '  help               Show this help message.'
