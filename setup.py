from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="fbsak",
    license="MIT",
    version="0.8.0",
    author="flipbit03",
    author_email="cadu.coelho@gmail.com",
    description="flipbit03's Swiss Army Knife",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/flipbit03/fbsak",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "aniso8601==7.0.0",
        "click==7.0",
        "flask==1.1.1",
        "flask-graphql==2.0.1",
        "graphene==2.1.8",
        "graphene-sqlalchemy==2.2.2",
        "graphql-core==2.3.1",
        "graphql-relay==2.0.1",
        "graphql-server-core==1.2.0",
        "itsdangerous==1.1.0",
        "jinja2==2.11.1",
        "markupsafe==1.1.1",
        "promise==2.3",
        "rx==1.6.1",
        "singledispatch==3.4.0.3",
        "six==1.14.0",
        "sqlalchemy==1.3.13",
        "werkzeug==1.0.0",
    ],
)
