from setuptools import setup
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

setup(
    name='serviceclients',
    include_package_data=True,
    version=serviceclients.__version__,
    description='FanX Service Clients',
    long_description=LONG_DESCRIPTION,
    author='fanxchange',
    download_url='https://github.com/fanxchange/fanx-service-clients',
    url='https://github.com/fanxchange/fanx-service-clients',
    packages=['serviceclients', ],
    package_dir={'serviceclients': 'serviceclients'},
    platforms=['Platform Independent'],
    license='BSD',
    classifiers=CLASSIFIERS,
    keywords=KEYWORDS,
    requires=['boto', 'elasticsearch', 'mysqlclient', 'pika', 'redis', 'PyMySQL'],
    install_requires=['boto', 'elasticsearch', 'mysqlclient', 'pika', 'redis', 'PyMySQL'],
    extras_require={
            'es_and_sqs':  ['ujson']
    }
)
