from setuptools import setup, find_packages
setup(
    name = 'HLP_AFG_PREMIE',
    version = '1.0',
    packages = find_packages(include = ('HLP_AFG_PREMIE*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.9.9'],
    entry_points = {
'console_scripts' : [
'main = HLP_AFG_PREMIE.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
