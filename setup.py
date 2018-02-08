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

# Was going to make it an extra only, to avoid in PyPy
# extras = {
#    'with_ujson': ['ujson==1.35']
# }


# Manage requirements
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
    requires=['boto==2.48.0', 'elasticsearch==5.5.2', 'mysqlclient==1.3.12', 'pika==0.11.2', 'redis==2.10.6',
              'ujson==1.35'],
    # extras_require=extras
)
