import unittest

from helpers.pitt_test_config import PittTestConfig
from parameterized import parameterized
from pitt_api_tests.test_adformats_bug import AdformatsBugTest
from pitt_api_tests.test_aggregation import AggregationTest
from pitt_api_tests.test_competing_lineitems import CompetingLineitemsTest
from pitt_api_tests.test_dimensions import DimensionsTest
from pitt_api_tests.test_exclude import ExcludeTest
from pitt_api_tests.test_good_cases import GoodCasesTest
from pitt_api_tests.test_hierarchy import HierarchyTest
from pitt_api_tests.test_include import IncludeTest
from pitt_api_tests.test_incorrect_dates import IncorrectDatesTest
from pitt_api_tests.test_keyvalue import KeyvalueTest
from pitt_api_tests.test_negative import NegativeTest
from pitt_api_tests.test_positive import PositiveTest


class PittTests(unittest.TestCase):
    configs = [
        PittTestConfig('inv-dev-02.inventale.com', 'pavel.kiselev', '6561bf7aacf5e58c6e03d6badcf13831', '79505',
                       'ifms5'),
        PittTestConfig('inv-dev-02.inventale.com', 'pavel.kiselev', '6561bf7aacf5e58c6e03d6badcf13831', '156535',
                       'ifms5')
    ]

    @parameterized.expand(configs)
    def test_adformats_bug(self, cfg):
        self.assertTrue(AdformatsBugTest.adformats_bug_test(cfg), 'Test adformats_bug failed!')

    @parameterized.expand(configs)
    @unittest.expectedFailure
    def test_aggregation(self, cfg):
        self.assertTrue(AggregationTest.aggregation_test(cfg), 'Aggregation test failed')

    @parameterized.expand(configs)
    def test_incorrect_dates(self, cfg):
        self.assertTrue(IncorrectDatesTest.incorrect_dates_test(cfg), 'Incorrect dates test failed')

    @parameterized.expand(configs)
    def test_exclude(self, cfg):
        self.assertTrue(ExcludeTest.exclude_test(cfg), 'Exclude test failed')

    @parameterized.expand(configs)
    def test_include(self, cfg):
        self.assertTrue(IncludeTest.include_test(cfg), 'Include test failed')

    @parameterized.expand(configs)
    def test_good_cases(self, cfg):
        self.assertTrue(GoodCasesTest.good_cases_test(cfg), 'Good cases test failed')

    @parameterized.expand(configs)
    def test_dimensions(self, cfg):
        self.assertTrue(DimensionsTest.dimensions_test(cfg), 'Dimensions test failed')

    @parameterized.expand(configs)
    def test_negative(self, cfg):
        self.assertTrue(NegativeTest.negative_test(cfg), 'Negative test failed')

    @parameterized.expand(configs)
    def test_positive(self, cfg):
        self.assertTrue(PositiveTest.positive_test(cfg), 'Positive test failed')

    @parameterized.expand(configs)
    def test_hierarchy(self, cfg):
        self.assertTrue(HierarchyTest.hierarchy_test(cfg), 'Hierarchy test failed')

    @parameterized.expand(configs)
    def test_competing_lineitems(self, cfg):
        self.assertTrue(CompetingLineitemsTest.competing_lineitems_test(cfg), 'Competing LIs test failed')

    @parameterized.expand(configs)
    def test_keyvalue(self, cfg):
        self.assertTrue(KeyvalueTest.keyvalue_test(cfg), 'Keyvalue test failed')
