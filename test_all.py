import sys
import unittest
import urllib
import urllib.request

from lims_sync_7900ht import *

class TestBasic(unittest.TestCase):

   def test_API_URLs(self):
      # Hard-coded here. Will fail if the base URL is changed in the code.
      prefixall = 'https://orfeu.cnag.crg.eu/prbblims_devel/api/covid19/'

      with urllib.request.urlopen(prefixall) as response:
         data = response.read()

      self.assertTrue('pcrplate' in str(data))
      self.assertTrue('pcrwell' in str(data))
      self.assertTrue('pcrrun' in str(data))
      self.assertTrue('detector' in str(data))
      self.assertTrue('results' in str(data))
      self.assertTrue('amplification' in str(data))
      self.assertTrue('organization' in str(data))

      self.assertTrue(pcrplate_url.startswith(prefixall))
      self.assertTrue(pcrwell_url.startswith(prefixall))
      self.assertTrue(pcrrun_url.startswith(prefixall))
      self.assertTrue(detector_url.startswith(prefixall))
      self.assertTrue(results_url.startswith(prefixall))
      self.assertTrue(amplification_url.startswith(prefixall))
      self.assertTrue(organization_url.startswith(prefixall))
