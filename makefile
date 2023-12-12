SHELL = /bin/bash
.PHONY: docs
.PHONY: test

docs:
	pipenv run pdoc senseurcity -d numpy -o docs/ --math --mermaid --search

test:
	pipenv lock
	pipenv requirements --dev > requirements.txt
	pipenv run tox
