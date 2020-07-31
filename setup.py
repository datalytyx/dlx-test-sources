from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='dlx-test-sources',
    version='1.0.0',
    packages=['render', 'data_generator'],
    url='https://github.com/datalytyx/dlx-test-sources',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only'
    ],
    author='Datalytyx',
    author_email='support@datalytyx.com',
    description='dlx-test-sources',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        'console_scripts': [
            'data-generator=data_generator:main',
            'render=render:main'
        ]
    },
    install_requires=[
        'jinja2==2.11.2',
        'kafka-python==2.0.1',
        'Faker==4.1.1',
        'mysql-connector-python==8.0.21',
        'pyodbc==4.0.30',
        'cx-Oracle==8.0.0',
        'colorlog==4.2.1',
    ]
)
