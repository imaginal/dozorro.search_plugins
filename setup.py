from setuptools import setup, find_packages

setup(
    name='dozorro.search_plugins',
    version='0.1b', # NOQA
    description="Dozorro Plugin for OpenProcurement Search",
    long_description=open("README.md").read(),
    # Get more strings from
    # http://pypi.python.org/pypi?:action=list_classifiers
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
    ],
    keywords='dozorro prozorro search',
    author='Volodymyr Flonts',
    author_email='flyonts@gmail.com',
    license='Apache License 2.0',
    url='https://github.com/imaginal/dozorro.search_plugins',
    namespace_packages=['dozorro'],
    packages=find_packages(),
    package_data={'': ['*.md', '*.txt']},
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'openprocurement.search',
        'mysqlclient>=1.3',
        'simplejson>=3.11'
    ],
    entry_points={
    }
)
