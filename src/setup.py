from setuptools import setup

setup(
    name='etl_pipeline',
    version='0.1',
    packages=['etl_pipeline'],
    install_requires=[
        'pandas>=1.0.0',
        'numpy>=1.18.0',
        'requests>=2.20.0',
    ],
)