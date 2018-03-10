"""Unit Tests for FinanceMatch.
"""

import unittest
import pipeline


class MyTest(unittest.TestCase):

    def test_enhance_voter(self):
        TESTS = [
            ({'FIRSTNAME': 'LYDIA',
              'MIDDLENAME': None,
              'LASTNAME': 'NIEVES',
              'RADDNUMBER': '5210',
              'RAPARTMENT': '12E',
              'RCITY': 'MANHATTAN',
              'RHALFCODE': None,
              'RPOSTDIRECTION': None,
              'RPREDIRECTION': None,
              'RSTREETNAME': 'BROADWAY',
              'RZIP4': None,
              'RZIP5': 10463,
              'TOWNCITY': 'MANHATTAN'
              }, {
                'FIRSTNAME': 'LYDIA',
                'MIDDLENAME': None,
                'LASTNAME': 'NIEVES',
                'RADDNUMBER': '5210',
                'RAPARTMENT': '12E',
                'RCITY': 'MANHATTAN',
                'RHALFCODE': None,
                'RPOSTDIRECTION': None,
                'RPREDIRECTION': None,
                'RSTREETNAME': 'BROADWAY',
                'RZIP4': None,
                'RZIP5': 10463,
                'TOWNCITY': 'MANHATTAN',
                'match_string': 'NIEVES, LYDIA , 5210 BROADWAY APT 12E, NEW YORK, NY 10463'})
        ]
        for t_input, t_expected in TESTS:
            self.maxDiff = None
            self.assertEqual(t_expected, pipeline.enhance_voter(t_input))


if __name__ == '__main__':
    unittest.main()
