import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sthree",
    version="0.1.0",
    author="Anatoly Scherbakov",
    author_email="altaisoft@gmail.com",
    description=(
        "A suite of typed data structures backed by Amazon S3."
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/anatoly-scherbakov/sthree',
    packages=setuptools.find_packages(exclude=[
        'tests',
        'run.py'
    ]),
    install_requires=[
        'boto3_type_annotations'
    ],
    extras_require={
        'boto': [
            'boto3'
        ],
        'dev': [
            'pytest',
            'boto3',
            'fire'
        ]
    },
    license='MIT',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
