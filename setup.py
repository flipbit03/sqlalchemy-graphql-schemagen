import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fbsak-flipbit03",  # Replace with your own username
    version="0.8.0",
    author="flipbit03",
    author_email="cadu.coelho@gmail.com",
    description="flipbit03's Swiss Army Knife",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/flipbit03/fbsak",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
