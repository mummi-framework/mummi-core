# Copyright (c) 2021, Lawrence Livermore National Security, LLC. All rights reserved. LLNL-CODE-827197.
# This work was produced at the Lawrence Livermore National Laboratory (LLNL) under contract no. DE-AC52-07NA27344 (Contract 44) between the U.S. Department of Energy (DOE) and Lawrence Livermore National Security, LLC (LLNS) for the operation of LLNL.  See license for disclaimers, notice of U.S. Government Rights and license terms and conditions.
# ------------------------------------------------------------------------------
import sys
from os.path import abspath, split, join
from setuptools import setup, find_packages

'''
import platform
dir = '{}-{}-{}.{}'.format(platform.system().lower(), platform.machine(), platform.python_version_tuple()[0], platform.python_version_tuple()[1])
print dir
'''

# ------------------------------------------------------------------------------
# relevant paths
src_path = split(abspath(sys.argv[0]))[0]
src_path = join(src_path, 'mummi_core')

# ------------------------------------------------------------------------------
setup(
    name='mummi_core',
    description='MuMMI Core',
    version='0.9.0',
    author='Harsh Bhatia, Helgi I Ingólfsson, Francesco Di Natale, Joseph Moon, Joseph R Chavez',
    author_email='hbhatia@llnl.gov, ingolfsson1@llnl.gov, dinatale3@llnl.gov, moon15@llnl.gov, chavez35@llnl.gov',
    url='https://github.com/mummi-framework/mummi-core',
    entry_points={
      'console_scripts': [
          'mummi_cmdclient = mummi_core.utils.cmdClient:main',
          'mummi_cmdserver = mummi_core.utils.cmdServer:main',
          'mummi_monitor = mummi_core.scripts.monitor_mummi:main',
          'mummi_bind_local_redis = mummi_core.scripts. bind_local_redis:main',
          'mummi_bind_global_redis = mummi_core.scripts.bind_global_redis:main'
      ]
    },
    packages=find_packages(),
    install_requires=['pip>=21.2.4',
                      'pytest>=6.2.4',
                      'numpy>=1.20.2',
                      'pytaridx>=1.0.2',
                      'psutil>=5.8.0',
                      'filelock>=3.0.12',
                      'redis>=3.5.3',
                      'PyYAML>=5.3.1',
                      'cryptography>=2.7',
                      'maestrowf>=1.1.8'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
    ],
)

# ------------------------------------------------------------------------------
