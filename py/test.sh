#! /bin/sh
set -e
(cd ../rs; cargo build; cd -)
(cd ../clib; cargo build; cd -)
rm -rf build
python3 setup.py build
cp build/lib.macosx-10.13-x86_64-3.7/_flatfile.cpython-37m-darwin.so ./
python3 test.py
