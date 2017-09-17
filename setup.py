from setuptools import setup, find_packages

setup(
    name='devfestne17',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'paramiko',
        'pymongo',
        'requests',
        'Scrapy',
        'stem',
        'Twisted'
    ],
    entry_points = {
        'console_scripts': ['start_ldch = ldch.base:run_spiders']
    }
)