import os
import os.path as osp
import re
import shutil
import sys
import subprocess
from setuptools import setup, find_packages
from setuptools.command.build_ext import build_ext


with open(osp.join(osp.abspath(osp.dirname(__file__)), 'README.md')) as f:
    long_description = f.read()


def find_version():
    with open(osp.join('foamcore', '__init__.py')) as fp:
        for line in fp:
            m = re.search(r'^__version__ = "(\d+\.\d+\.\d[a-z]*\d*)"', line, re.M)
            if m is None:
                # could be a hotfix
                m = re.search(r'^__version__ = "(\d.){3}\d"', line, re.M)
            if m is not None:
                return m.group(1)
        raise RuntimeError("Unable to find version string.")


class BuildExt(build_ext):

    _thirdparty_exec_files = [
        "foamcore/thirdparty/bin/redis-server",
        "foamcore/thirdparty/bin/redis-cli"
    ]

    description = "Build third-party libraries."

    def run(self):
        command = ["./build.sh", "-p", sys.executable]
        subprocess.check_call(command)
        self._move_thirdparty_exec_files()

    def _move_thirdparty_exec_files(self):
        for filename in self._thirdparty_exec_files:
            src = filename
            dst = os.path.join(self.build_lib, filename)

            parent_directory = os.path.dirname(dst)
            if not os.path.exists(parent_directory):
                os.makedirs(parent_directory)

            if not os.path.exists(dst):
                self.announce(f"copy {src} to {dst}", level=1)
                shutil.copy(src, dst)


setup(
    name='foamcore',
    version=find_version(),
    author='Jun Zhu',
    author_email='zhujun981661@gmail.com',
    description='',
    long_description=long_description,
    url='',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'foamcore=foamcore.services:application',
            'foamcore-redis-cli=foamcore.services:start_redis_client',
        ],
    },
    tests_require=['pytest'],
    cmdclass={
        'build_ext': BuildExt,
    },
    install_requires=[
        'hiredis',
        'redis',
        'psutil',
        'foamclient>=0.1.3'
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-cov',
        ],
    },
    python_requires='>=3.9',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Scientific/Engineering :: Information Analysis',
    ]
)
