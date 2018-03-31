#!/usr/bin/env python
# from setuptools import setup  # Using distutils, seems to be more flexible with build
from distutils.core import setup
import serviceclients

LONG_DESCRIPTION = open('README.md').read()

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Natural Language :: English',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Software Development :: Libraries :: Python Modules'
]

KEYWORDS = 'FanX Service Clients'

# Was going to make it an extra only, to avoid in PyPy
# extras = {
#    'with_ujson': ['ujson==1.35']
# }
#
# Include in setup:
# extras_require=extras

deps = ['boto==2.48.0', 'elasticsearch==5.5.2', 'mysqlclient==1.3.12', 'pika==0.11.2', 'redis==2.10.6', 'ujson==1.35']

# Manage requirements
setup(
    name='serviceclients',
    include_package_data=True,
    version=serviceclients.__version__,
    description=KEYWORDS,
    long_description=LONG_DESCRIPTION,
    author='fanxchange',
    author_email='support@fanxchange.com',
    download_url='https://github.com/fanxchange/fanx-service-clients',
    url='https://github.com/fanxchange/fanx-service-clients',
    packages=['serviceclients', 'serviceclients.aws', 'serviceclients.cache', 'serviceclients.database',
              'serviceclients.queue', 'serviceclients.search'],
    package_dir={'serviceclients': 'serviceclients'},
    # package_data={'serviceclients': ['serviceclients/*', ]},
    platforms=['Platform Independent'],
    license='BSD',
    classifiers=CLASSIFIERS,
    keywords=KEYWORDS,
    install_requires=deps
)
