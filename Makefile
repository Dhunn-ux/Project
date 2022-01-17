requirements:
	pip install -r requirements_dev.txt
	pip-compile --output-file requirements.txt requirements.in
	pip install -r requirements.txt


black:
	black --line-length 100 --skip-string-normalization src tests

lint:
	black --line-length 100 --skip-string-normalization --check src tests


install: clean
	python setup.py install



