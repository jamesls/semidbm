import os
from setuptools import setup, find_packages


setup(
    name='semidbm',
    version='0.5.1',
    description="Cross platform (fast) DBM interface in python",
    long_description=open(os.path.join(os.path.dirname(__file__),
                                       'README.rst')).read(),
    license='BSD',
    author='James Saryerwinnie',
    author_email='js@jamesls.com',
    packages = find_packages(),
    zip_safe=False,
    keywords="semidbm",
    url="https://github.com/jamesls/semidbm",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: Implementation :: Jython',
        'License :: OSI Approved :: BSD License',
    ],
)

