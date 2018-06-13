from hypothesis import given
import hypothesis.strategies as st
import unittest
import io

import flatfile


class TestTlogHypothesis(unittest.TestCase):

    @given(st.text(), st.text())
    def test_hypothesis_put_get(self, k, v):
        md = """
          column k string
          column v string _ lz4
        """
        m = flatfile.metadata_parse(md)
        f = io.BytesIO()
        w = {'k': k, 'v': v}
        m.write(w, f)
        f.seek(0)
        r = m.read(f)
        self.assertEqual(r['k'], w['k'])
        self.assertEqual(r['v'], w['v'])
        f.close()

if __name__ == '__main__':
    unittest.main()
