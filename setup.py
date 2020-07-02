#!/usr/bin/env python

import os
from setuptools import setup, find_packages

version = '1.4.1'

classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: GNU Affero General Public License v3",
    "Programming Language :: Python",
    "Operating System :: OS Independent",
    "Topic :: Software Development :: Libraries",
    "Topic :: Utilities",
    "Environment :: Web Environment",
    "Framework :: Django",
]

def root_dir():
    try:
        return os.path.dirname(__file__)
    except NameError:
        return '.'

long_desc = open(os.path.join(root_dir(), 'README')).read()

setup_args=dict(
    name='django-dag',
    version=version,
    url='https://github.com/elpaso/django-dag',
    author='Alessandro Pasotti',
    author_email='apasotti@gmail.com',
    license='GNU Affero General Public License v3',
    packages=find_packages(exclude=['docs']),
    package_dir={'django_dag': 'django_dag'},
    #package_data={'dag': ['templates/admin/*.html']},
    description='Directed Acyclic Graph implementation for Django 1.6+',
    classifiers=classifiers,
    long_description=long_desc,
)


if __name__ == '__main__':
     setup(**setup_args)
