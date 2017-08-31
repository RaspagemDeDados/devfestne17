from setuptools import setup, find_packages

setup(
    name='devfestne17',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'requests',
        'scrapy',
        'paramiko',
        'stem',
        'twisted',
        'pymongo'
    ],
    entry_points = {
        'console_scripts': ['lhcd = lhcd:main']
    }
)