import unittest

from .context import yfc_utils as yfcu

class TestUtils(unittest.TestCase):

    def setUp(self):
        pass

    def test_sigfigs(self):
        self.assertEqual(yfcu.GetSigFigs(1.0), 1)
        self.assertEqual(yfcu.GetSigFigs(1.1), 2)
        self.assertEqual(yfcu.GetSigFigs(9.0), 1)
        self.assertEqual(yfcu.GetSigFigs(9.9), 2)

        self.assertEqual(yfcu.GetSigFigs(11.0), 2)


    def test_magnitude(self):
        self.assertEqual(yfcu.GetMagnitude(1.0), 1)
        self.assertEqual(yfcu.GetMagnitude(9.0), 1)
        self.assertEqual(yfcu.GetMagnitude(9.9), 1)

        self.assertEqual(yfcu.GetMagnitude(0.1), -1)
        self.assertEqual(yfcu.GetMagnitude(0.9), -1)
        self.assertEqual(yfcu.GetMagnitude(0.99), -1)

        self.assertEqual(yfcu.GetMagnitude(0.01), -2)
        self.assertEqual(yfcu.GetMagnitude(0.09), -2)


    def test_calculateRounding(self):
        self.assertEqual(yfcu.CalculateRounding(12345, 4), 0)
        self.assertEqual(yfcu.CalculateRounding(1234, 4), 0)
        self.assertEqual(yfcu.CalculateRounding(123, 4), 1)

        self.assertEqual(yfcu.CalculateRounding(123.4, 4), 1)
        self.assertEqual(yfcu.CalculateRounding(12.34, 4), 2)
        self.assertEqual(yfcu.CalculateRounding(1.234, 4), 3)
        self.assertEqual(yfcu.CalculateRounding(.1234, 4), 4)

        self.assertEqual(yfcu.CalculateRounding(123.4, 3), 0)
        self.assertEqual(yfcu.CalculateRounding(12.34, 3), 1)
        self.assertEqual(yfcu.CalculateRounding(1.234, 3), 2)
        self.assertEqual(yfcu.CalculateRounding(.1234, 3), 3)

        self.assertEqual(yfcu.CalculateRounding(1.0, 4), 3)


if __name__ == '__main__':
    unittest.main()
