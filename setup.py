from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="sqlalchemy-graphql-schemagen",
    license="MIT",
    version="1.0.5",
    author="flipbit03",
    author_email="cadu.coelho@gmail.com",
    description="Generate a full (query+mutation) GraphQL schema from your SQLAlchemy Declarative Model Base.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/flipbit03/sqlalchemy-graphql-schemagen",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "cryptography>=3.2",
        "flask>=1.1.1",
        "flask-graphql>=2.0.1",
        "graphene-sqlalchemy>=2.2",
        "sqlalchemy>=1.3.13",
    ],
)
