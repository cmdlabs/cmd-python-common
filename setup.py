import os
import sys
from setuptools import find_packages, setup
from setuptools.command.install import install

VERSION = "1.0.3"

# Version number is described at https://www.python.org/dev/peps/pep-0440.
if os.environ.get('CI_COMMIT_TAG'):
    # Versioned Release
    CI_VERSION = os.environ.get('CI_COMMIT_TAG')
    IS_CI_BUILD = True
    IS_VERSION_BUILD = True
elif os.environ.get('CI_JOB_ID'):
    # Beta Release
    CI_VERSION = "%s.b%s" % (VERSION, os.environ.get('CI_JOB_ID'))
    IS_CI_BUILD = True
    IS_VERSION_BUILD = False
else:
    # Alpha Release
    CI_VERSION = "%s.a" % VERSION
    IS_CI_BUILD = False
    IS_VERSION_BUILD = False

def readme():
    """print long description"""
    with open('README.md') as f:
        return f.read()


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version"""

    def run(self):
        if (IS_CI_BUILD and IS_VERSION_BUILD):
            if CI_VERSION != VERSION:
                info = "Git tag: {0} does not match the version of this app: {1}".format(
                    CI_VERSION, VERSION
                )
                sys.exit(info)


setup(
    name="runcmd",
    version=CI_VERSION,  # Required
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    description="CMD commoun python modules and function used for all the client projects",
    long_description=readme(),
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'dist', 'dependencies']),
    url='https://gitlab.runcmd.cmdsolutions.com.au/cmd-modules/cmd-python-common',
    author='CMD Solutions',
    author_email='sales@cmdsolutiosn.com.au',
    install_requires=[
        'boto3==1.9.62',
        'pyspark==2.4.0',
        'pymssql==2.1.1',
        'pandas==0.23.4',
        'requests==2.21.0'
    ],
    cmdclass={
        'verify': VerifyVersionCommand,
    }
)
