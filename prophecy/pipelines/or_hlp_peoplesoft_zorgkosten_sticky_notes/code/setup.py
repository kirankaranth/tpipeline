from setuptools import setup, find_packages

setup(
    name='or_hlp_peoplesoft_zorgkosten_sticky_notes',
    version='1.0',
    packages=find_packages(include=['or_hlp_peoplesoft_zorgkosten_sticky_notes*']),
    description='or_hlp_peoplesoft_zorgkosten_sticky_notes',
    install_requires=[
        'prophecy-libs==1.7.4'
    ],
    entry_points={
        'console_scripts': [
            'main = or_hlp_peoplesoft_zorgkosten_sticky_notes.pipeline:main',
        ],
    },
    data_files=[(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require={
        'test': ['pytest', 'pytest-html'],
    }
)