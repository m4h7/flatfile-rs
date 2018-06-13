from distutils.core import setup, Extension

module1 = Extension('flatfile',
                    include_dirs = ['../include'],
                    libraries = ['libflatfile', 'resolv'],
                    library_dirs = ['../clib/target/debug'],
                    sources = ['flatfile.c'])

setup(name='Flatfile',
      version='1.0',
      description='flatfile package',
      ext_modules=[module1])
