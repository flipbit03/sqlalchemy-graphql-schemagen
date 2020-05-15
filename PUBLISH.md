##### build package after editing 
    python setup.py sdist bdist_wheel
    
##### upload it to PyPI
    twine upload -r pypi dist/*