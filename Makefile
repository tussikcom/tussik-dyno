.PHONY: docs

hello:
	@echo "Hello World"

init:
	@echo "Initializing"
	@python3 -m pip install --upgrade pip
	@pip install -U build twine pytest

test: init
	@echo "Testing"
	# This runs all of the tests on all supported Python versions.
	tox -p

ci: init
	@echo "CI"
	pytest tests --junitxml=report.xml

clean:
	@echo "Clean"
	@rm -rf build dist tussik.dyno.egg-info src/tussik.dyno.egg-info

build: clean init
	@echo "*"
	@echo "* Build"
	@echo "*"
	@python3 -m build --sdist --wheel

prerelease: build
	@echo "*"
	@echo "* Prerelease"
	@echo "*"
	@twine check --strict dist/* && twine upload --repository testpypi dist/*

release: build
	@echo "*"
	@echo "* Release"
	@echo "*"
	twine upload --repository pypi dist/*

docs:
	@echo "Documentation"
	cd docs && make html
	@echo "\033[95m\n\nBuild successful! View the docs homepage at docs/_build/html/index.html.\n\033[0m"