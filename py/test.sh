#! /bin/sh
set -e
(cd ../rs; cargo build; cargo build --release; cd -)
(cd ../clib; cargo build; cargo build --release; cd -)
rm -rf build
python3 setup.py build
cp build/lib.macosx-10.13-x86_64-3.7/_flatfile.cpython-37m-darwin.so ./
python3 test.py
