#! /bin/sh
set -e
(cd ../clib; cargo build; cd -)
rm -rf build
python3 setup.py build
cp build/lib.macosx-10.13-x86_64-3.6/flatfile.cpython-36m-darwin.so ./
python3 test.py
