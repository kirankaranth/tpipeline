from setuptools import setup, find_packages

setup(
    name='join_agg_sort',
    version='1.0',
    packages=find_packages(include=['job*']),
    description='join_agg_sort',
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