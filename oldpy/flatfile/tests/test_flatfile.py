from unittest import TestCase
import io

import flatfile

class TestFlatfile(TestCase):
    def test_all(self):
        md = """
          column a string datetime
          column b string _ lz4
          column c u32le
          column d u64le
        """
        m = flatfile.metadata_parse(md)
        f = io.BytesIO()
        w = {'a': 'aaaa', 'b': 'bbbb', 'c': 12345678, 'd': 1122334455667788 }
        m.write(w, f)
        f.seek(0)
        r = m.read(f)
        self.assertEqual(r['a'], w['a'])
        self.assertEqual(r['b'], w['b'])
        self.assertEqual(r['c'], w['c'])
        self.assertEqual(r['d'], w['d'])
        f.close()

    def test_nulls(self):
        md = """
          column a string datetime
          column b string _ lz4
          column c u32le
          column d u64le
        """
        m = flatfile.metadata_parse(md)
        f = io.BytesIO()
        w = {'a': 'aaaa', 'c': 12345678, 'd': 1122334455667788 }
        m.write(w, f)
        f.seek(0)
        r = m.read(f)
        self.assertEqual(r['a'], w['a'])
        self.assertTrue(r.get('b') is None)
        self.assertEqual(r['c'], w['c'])
        self.assertEqual(r['d'], w['d'])
        f.close()
