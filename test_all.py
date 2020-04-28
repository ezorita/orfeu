import pytest
import sys
import unittest
import urllib
try:
   from urllib.request import urlopen
except ImportError:
   from urllib2 import urlopen


class TestBasic(unittest.TestCase):

   def test_Python_version(self):
      if sys.version_info < (3,5):
         # Make sure import fails on unsupported Python versions.
         with self.assertRaises(ImportError):
            import lims_sync_7900ht
      else:
         # Make sure import succeeds on supported Python versions.
         import lims_sync_7900ht

   def test_API_URLs(self):

      pytest.importorskip('lims_sync_7900ht')
      import lims_sync_7900ht

      # Hard-coded here. Will fail if the base URL is changed in the code.
      prefixall = 'https://orfeu.cnag.crg.eu/prbblims_devel/api/covid19/'

      with urlopen(prefixall) as response:
         data = response.read()

      self.assertTrue('pcrplate' in str(data))
      self.assertTrue('pcrwell' in str(data))
      self.assertTrue('pcrrun' in str(data))
      self.assertTrue('detector' in str(data))
      self.assertTrue('results' in str(data))
      self.assertTrue('amplification' in str(data))
      self.assertTrue('organization' in str(data))

      self.assertTrue(lims_sync_7900ht.pcrplate_url.startswith(prefixall))
      self.assertTrue(lims_sync_7900ht.pcrwell_url.startswith(prefixall))
      self.assertTrue(lims_sync_7900ht.pcrrun_url.startswith(prefixall))
      self.assertTrue(lims_sync_7900ht.detector_url.startswith(prefixall))
      self.assertTrue(lims_sync_7900ht.results_url.startswith(prefixall))
      self.assertTrue(lims_sync_7900ht.amplification_url.startswith(prefixall))
      self.assertTrue(lims_sync_7900ht.organization_url.startswith(prefixall))
