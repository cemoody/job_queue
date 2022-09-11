#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
"loguru"
 ]

test_requirements = ['pytest>=3', ]

setup(
    author="Christopher Erick Moody",
    author_email='chrisemoody@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Job Queue helps you link jobs together with automatically managed queues",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='job_queue',
    name='job_queue',
    packages=find_packages(include=['job_queue', 'job_queue.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/cemoody/job_queue',
    version='0.1.4',
    zip_safe=False,
)
