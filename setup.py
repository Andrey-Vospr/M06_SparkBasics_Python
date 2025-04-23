#!/usr/bin/env python
import os
import shutil
from setuptools import setup, find_packages
from setuptools.command.bdist_egg import bdist_egg as _bdist_egg
from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
from setuptools.command.sdist import sdist as _sdist


# Define a custom output directory
OUTPUT_DIR = os.path.abspath("docker/dist")

# Ensure the directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


class bdist_egg(_bdist_egg):
    """Custom command to move the generated .egg file to docker/dist/"""
    def run(self):
        super().run()
        self._move_files()

    def _move_files(self):
        if os.path.exists("dist"):
            for file in os.listdir("dist"):
                shutil.move(os.path.join("dist", file), os.path.join(OUTPUT_DIR, file))

setup(
    name='sparkbasics',
    version='1.0.0',
    description='BDCC Pyspark Basics project',
    py_modules=['__main__'],
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    zip_safe=False,
    cmdclass={
        'bdist_egg': bdist_egg,
    }
)
