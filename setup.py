import os
from setuptools import setup, find_packages


setup(
    name='semidbm',
    version='0.3.0',
    description="An alternative to python's dumbdbm",
    long_description=open(os.path.join(os.path.dirname(__file__),
                                       'README.rst')).read(),
    license='BSD',
    author='James Saryerwinnie',
    author_email='jlsnpi@gmail.com',
    py_modules=['semidbm'],
    zip_safe=False,
    keywords="semidbm",
    url="https://github.com/jamesls/semidbm",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
    ],
)

