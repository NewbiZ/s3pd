.PHONY: dist

dist:
	python setup.py sdist

publish:
	twine register dist/s3pd-1.0.0.tar.gz -r pypi
	twine upload dist/s3pd-1.0.0.tar.gz -r pypi
