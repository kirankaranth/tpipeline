from setuptools import setup, find_packages

setup(
    name='customers_orders',
    version='1.0',
    packages=find_packages(include=['job*']),
    description='customers_orders',
    install_requires=[
        'prophecy-libs==1.3.8'
    ],
    entry_points={
        'console_scripts': [
            'main = job.pipeline:main',
        ],
    },
    data_files=[(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require={
        'test': ['pytest', 'pytest-html'],
    }
)