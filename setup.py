"""Handle packaging and distribution of module code.
"""
import setuptools

REQUIRED_PACKAGES = [
    'requests'
]

setuptools.setup(
    name='voter_warehouse',
    version='0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
)
