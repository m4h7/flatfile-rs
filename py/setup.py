from setuptools import setup, Extension

module1 = Extension('_flatfile',
                    include_dirs = ['../include'],
                    extra_compile_args = ["-std=c99", "-Wall", "-Wextra", "-Wno-unused-parameter"],
                    libraries = ['libflatfile', 'resolv'],
                    library_dirs = ['../clib/target/release'],
                    sources = ['flatfile.c'])

setup(name='yrml-flatfile',
      version='1.3.15',
      description='flatfile package',
      packages=["flatfile"],
      ext_modules=[module1])
