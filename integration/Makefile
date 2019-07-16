.PHONY: all build lint test test_integration test_unit clean clean_pycache \
        install debuild debian_changelog clean-debuild format help \
        create_env start_env stop_env
SESSION_FILE=.session_conf.sav
INSTALL_DIR=$(DESTDIR)/opt/yandex/wal-g
TEST_VENV=venv

build:

all: lint integration_test

test: lint

venv:
	python3.6 -m venv ${TEST_VENV}

	${TEST_VENV}/bin/pip install --no-cache-dir --disable-pip-version-check \
		-r requirements.txt \
		-r requirements-test.txt
	${TEST_VENV}/bin/pip install --no-cache-dir --disable-pip-version-check \
		--index-url https://pypi.yandex-team.ru/simple \
		-r requirements-test-ya.txt

gitcheck:
	git --no-pager diff HEAD~1 --check

isort: venv
	${TEST_VENV}/bin/isort --recursive --check-only --ignore-whitespace --diff wal-g tests

yapf: venv
	${TEST_VENV}/bin/yapf -rpd wal-g tests

flake8: venv
	${TEST_VENV}/bin/flake8 wal-g tests

pylint: venv
	${TEST_VENV}/bin/pylint wal-g tests

bandit: venv
	${TEST_VENV}/bin/bandit -r -s B101,B303 wal-g

yadi: venv
	${TEST_VENV}/bin/yadi --remote test

lint: gitcheck isort yapf flake8 pylint yadi bandit

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


install:
	@echo "Installing into $(INSTALL_DIR)"
	python3.6 -m venv $(INSTALL_DIR)
	$(INSTALL_DIR)/bin/pip install .
	mkdir -p $(DESTDIR)/usr/bin/
	ln -s /opt/yandex/wal-g/bin/genbackup $(DESTDIR)/usr/bin/
	ln -s /opt/yandex/wal-g/bin/s3cmd $(DESTDIR)/usr/bin/
	rm -rf $(INSTALL_DIR)/bin/activate*
	find $(INSTALL_DIR) -name __pycache__ -type d -exec rm -rf {} +
	test -n '$(DESTDIR)' \
	    && grep -l -r -F '#!$(INSTALL_DIR)' $(INSTALL_DIR) \
	        | xargs sed -i -e 's|$(INSTALL_DIR)|/opt/yandex/wal-g|' \
	    || true


debuild: debian_changelog
	cd debian && \
	    debuild --check-dirname-level 0 --no-tgz-check --preserve-env -uc -us

debian_changelog:
	@rm -f debian/changelog
	dch --create --package wal-g --distribution trusty \
	    -v `git rev-list HEAD --count`-`git rev-parse --short HEAD` \
	    "Yandex autobuild"

clean_debuild: clean
	rm -rf debian/{changelog,files,wal-g*}
	rm -f ../wal-g_*{build,changes,deb,dsc,tar.gz}

clean_env: stop_env
	rm -rf staging ${SESSION_FILE}

create_env: ${TEST_VENV}
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control create

start_env: create_env
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control start

stop_env:
	test -d ${TEST_VENV}/bin && test -f ${SESSION_FILE} && \
	PATH=${TEST_VENV}/bin:$$PATH ${TEST_VENV}/bin/python -m tests.integration.env_control stop || true

format: venv
	${TEST_VENV}/bin/isort --recursive --apply wal-g tests
	${TEST_VENV}/bin/yapf --recursive --parallel --in-place wal-g tests

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
	@echo '  format             Re-format source code to conform style settings enforced by'
	@echo '                     isort and yapf tools.'
	@echo '  help               Show this help message.'
