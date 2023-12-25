from setuptools import setup, find_packages
import os
import sys

_here = os.path.abspath(os.path.dirname(__file__))
# with open('README.md') as f:
#     readme = f.read()

python_2 = sys.version_info[0] == 2


def read(fname):
    with open(fname, 'rU' if python_2 else 'r') as fhandle:
        return fhandle.read()


def read_reqs(fname):
    req_path = os.path.join(_here, fname)
    return [req.strip() for req in read(req_path).splitlines() if req.strip()]


all_reqs = read_reqs('requirements.txt')
setup(
    name='ray_kafka_producer',
    version='0.0.1',
    description='Python SDK to produce Kafka messages to a Kafka cluster',
    long_description=readme,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=all_reqs,
    include_package_data=True
)
