import glob, sys, os, io, re
import requests
import datetime
from dateutil.parser import parse as date_parse
import pandas as pd

# TODO: How to handle unfinished syncs (i.e. the connection broke during sync) --> verify script
#

# TODO:
# - Parse and sync from the same script.
# - Write all newly created elements in a file (at least their id), for tracking what needs to be deleted from lims in case the whole thing fails.
# - Write output files at the end of syncing

organization_name = 'CRG'

base_url = 'https://orfeu.cnag.crg.eu'
api_root = '/prbblims_devel/api/covid19/'
pcrplate_base = '{}pcrplate/'.format(api_root)
pcrrun_base = '{}pcrrun/'.format(api_root)
pcrwell_base = '{}pcrwell/'.format(api_root)
detector_base = '{}detector/'.format(api_root)
results_base = '{}results/'.format(api_root)
amplification_base = '{}amplificationdata/'.format(api_root)
organization_base = '{}organization/'.format(api_root)

pcrplate_url = '{}{}'.format(base_url, pcrplate_base)
pcrwell_url = '{}{}'.format(base_url, pcrwell_base)
pcrrun_url = '{}{}'.format(base_url, pcrrun_base)
detector_url = '{}{}'.format(base_url, detector_base)
results_url = '{}{}'.format(base_url, results_base)
amplification_url = '{}{}'.format(base_url, amplification_base)
organization_url = '{}{}'.format(base_url, organization_base)


###
### LOGGING / ERROR CONTROL
###

def assert_error(cond, msg):
   if not cond:
      print(msg)
      # LOG MSG
      sys.exit(1)
   return cond

def assert_warn(cond, msg):
   if not cond:
      # LOG MSG
      print(msg)
      # NOTIFY
      pass
   return cond

###
### LIMS REQUEST METHODS
###

# Get environment variables
LIMS_USER = os.environ.get('LIMS_USER')
LIMS_PASSWORD = os.environ.get('LIMS_PASSWORD')
assert_error(LIMS_USER is not None and LIMS_PASSWORD is not None, 'LIMS API environment variables not set (set env vars LIMS_USER, LIMS_PASSWORD)')
# Construct header
req_headers = {'content-type': 'application/json', 'Authorization': 'ApiKey {}:{}'.format(LIMS_USER, LIMS_PASSWORD) };
  

def lims_request(method, url, params=None, json_data=None, headers=req_headers):
   # methods: GET, OPTIONS, HEAD, POST, PUT, PATCH, DELETE
   r = requests.request(method, url, params=params, headers=headers, json=json_data, verify=False)
   assert_warn(r.status_code < 300,
                  'LIMS request returned non-successful response ({}). Request details: URL={}, PARAMS={}, DATA={}'.format(
                     r.status_code,
                     url,
                     params,
                     json_data
                  ))
   return r, r.status_code


###
### DATA PARSING METHODS
###

def parse_rn(clipped_file):
   data = pd.read_csv(clipped_file, sep='\t', skiprows=1, header=0, index_col=False)
   info = pd.DataFrame({'well': data.iloc[:,0], 'rep': data.iloc[:,1]})
   
   # Split Rn and Delta_Rn in two DataFrames
   rn  = pd.concat([info , data.iloc[:,data.columns.get_loc('Rn')+1:data.columns.get_loc('Delta Rn')]], axis=1)
   drn = pd.concat([info, data.iloc[:,data.columns.get_loc('Delta Rn')+1:]], axis=1)
   return rn, drn

def parse_results(results_file):
   with open(results_file) as f_in:
      run_date = None
      # Catch headers
      for line in f_in:
         if 'Run DateTime' in line:
            run_date = line.rstrip().split('\t')[-1]
         if line[0:4] == 'Well':
            colnames = line.split('\t')
            break
      # Regex data rows
      rows = [line for line in f_in if re.match(r'^[0-9]', line)]


   # Create DataFrame
   data = pd.read_csv(io.StringIO('\n'.join(rows)), delim_whitespace=True, names=colnames)
   
   return data, run_date

def rename_Ct(x):
   return 'NA' if x in ['Unknown','Undetermined'] else x


###
### MAIN SCRIPT
###

if __name__ == '__main__':

   # Arguments (TODO: use library for argument parsing)
   if len(sys.argv) != 2:
      print("usage: {} root_path".format(sys.argv[0]))
      sys.exit(1)
   
   path = sys.argv[1]

   # Test LIMS connection
   _, status = lims_request('GET', base_url)
   assert_error(status < 300, 'Test connection to LIMS API failed')

   # Get a list of organizations, get CRG's uri
   orgs, status = lims_request('GET', organization_url, params={'limit':10000})
   assert_error(status < 300, 'Could not retreive LIMS organizations')
   org_id = [org['id'] for org in orgs.json()['objects'] if org['name'].lower() == organization_name.lower()]
   assert_error(len(org_id) == 1, "Provided organization not found in LIMS 'organization' table: {}".format(organization_name))
   org_id = org_id[0]
   
   # Get list of pcr plates
   # Need a wrapper to check that the request worked
   pcrplates, status = lims_request('GET', url=pcrplate_url, params={'limit': 1000000})
   assert_error(status < 300, 'Could not retreive pcr plates from LIMS')
   pcrplates = pcrplates.json()['objects']
   pcrplates_barcodes = [pcrplate['barcode'] for pcrplate in pcrplates]

   # Get list of detector ids
   detectors, status = lims_request('GET', url=detector_url, params={'limit': 1000000})
   assert_error(status < 300, 'Could not retreive pcr detectors from LIMS')
   detector_ids = {detector['name'].lower(): int(detector['id']) for detector in detectors.json()['objects']}
  
   # Find all processed samples in path
   flist = glob.glob('{}/*_results.txt'.format(path))
   for fname in flist:
      platebc = fname.split('/')[-1].split('_results.txt')[0]
      print('[pcrplate/barcode={}] BEGIN processing sample'.format(platebc))
      
      # Check if PCRPLATE is already in LIMS (TODO: also check if status is PROCESSING)
      if not assert_warn(platebc in pcrplates_barcodes, '[pcrplate/barcode={}] plate barcode not present in LIMS system, cannot sync data until it is created. skipping pcrplate'.format(platebc)):
         continue

      plateobj = [p for p in pcrplates if p['barcode'].lower() == platebc][0]
      print('[pcrplate/barcode={}] found PCRPLATE in LIMS (id:{}, uri:{})'.format(platebc, plateobj['id'], plateobj['uri']))

      import pdb; pdb.set_trace()
      r, status = lims_request('GET', url=pcrrun_url, params={'pcr_plate__iexact': plateobj['resource_uri']))
      if not assert_warn(status == 200, '[pcrplate/barcode={}] error checking presence of PCRRUN. skipping pcrplate'.format(platebc)):
         continue

      if len(r.json()['objects']) > 0:
         if r.json()['objects'][0]['status'] == 'PROCESSING':
            print("[pcrplate/barcode={}] PCRRUN info already in LIMS, but status=PROCESSING. did this sample fail to sync?".format(platebc))
            # NOT SURE YET WHAT TO DO HERE, let's skip for now
            continue
         else:
            print("[pcrplate/barcode={}] PCRRUN info already in LIMS. no need to sync".format(platebc))
            continue
            
      # Push new PCRRUN to LIMS
      pcrrun_data = {
         'id': None,
         'pcr_plate': plateobj['resource_uri'],
         'technician_id': None,
         'pcr_run_instrument_id': None,
         'pcr_run_protocol_id': None,
         'date_run': date_parse(run_date).isoformat(),
         'raw_results_file_path': fname,
         'results_file_path': fname, #TODO: UPDATE PATH
         'run_log_path': fname, #TODO: UPDATE PATH
         'analysis_result_file_path': fname, #TODO: UPDATE PATH
         'status': 'PROCESSING',
         'comments': None
      }
      r, status = lims_request('POST', pcrrun_url, json_data=pcrrun_data)
      if not assert_warn(status == 201, '[pcrplate/barcode={}] error creating PCRRUN in LIMS. skipping pcrplate'.format(platebc)):
         continue
      pcrrun_uri = r.headers['Location']
      print('[pcrplate/barcode={}] created new PCRRUN in LIMS (uri:{})'.format(platebc, pcrrun_uri))

      # Get all PCRWELL for this PCRPLATE
      r, status = lims_request('GET', url=pcrwell_url, params={'limit': 10000, 'pcr_plate__iexact': plateobj['resource_uri']})
      if not assert_warn(status == 200, '[pcrplate/barcode={}] error getting PCRWELLS for this PCRPLATE. skipping pcrplate. (Note: I have created PCRRUN {})'.format(platebc, pcrrun_uri)):
        continue

     # Note: PCRWELL position is in A1, A2... format
     pcrwells = r['objects']
     print('[pcrplate/barcode={}] found {} PCRWELL for this PCRPLATE in LIMS'.format(platebc, len(pcrwells)))

      # NOW NEED TO FILL IN AL WELL INFO IN THE SAME PCRWELL OBJECTS AND PATCH THEM BACK! THE REST (results) IS IDENTICAL! :)

      # Parse results
      results, run_date = parse_results(fname)
      rn, drn = parse_rn('{}/{}_clipped.txt'.format(path, platebc))

      # Format results
      results['Ct'] = results['Ct'].apply(rename_Ct)
      results['plate_barcode'] = platebc

      for row in results.iterrows():
         i = row[0]
         row = row[1]

         ##
         ## CREATE PCR WELL
         ##
         # Make a POST request to pcrwell (THIS REALLY NEEDS THE RNA_EXTRACTION_WELL CORRESPONDENCE, OTHERWISE RETURNS status 500!)
         well_data = {
            'id':None,
            'pcr_plate': pcrplate_uri,
            'rna_extraction_well': '/prbblims_devel/api/covid19/rnaextractionwell/101/', # TODO: Compute corresponding RNA well before
            'position': str(row['Well']),
            'pass_fail': None,
            'automatic_diagnosis': 'I',
            'catsalut_diagnosis': 'W'
         }

         r, status = lims_request('POST', pcrwell_url, json_data=well_data)
         if not assert_warn(status == 201, 'error creating well (pcrplate:{}, pcr_well:{})'.format(platebc, well_data)):
            continue
         pcrwell_uri = r.headers['Location']

         ##
         ## CREATE RESULTS ENTRY
         ##

         # Make a POST request to results
         results_data = {
            'id':None,
            'pcr_well': pcrwell_uri,
            'comments': None,
            'date_analysis': date_parse(run_date).isoformat(),
            'date_sent': datetime.datetime.now().isoformat(),
            'amplification': None,
            'threshold': None if pd.isna(row['Threshold']) else row['Threshold'],
            'detector_id': detector_ids[row['Detector Name'].lower()],
            'detector_lot_number': None,
            'ct': None if row['Ct'] == 'NA' else row['Ct']
         }
         res, status = lims_request('POST', results_url, json_data=results_data)
         if not assert_warn(status == 201, 'error creating results (pcrplate:{}, results:{})'.format(platebc, results_data)):
            continue
         results_uri = r.headers['Location']

         ##
         ## CREATE RN CURVES
         ##
         import pdb; pdb.set_trace() 
         rn_vals = rn[rn['well'] == row['Well']].iloc[:,rn.columns.get_loc('1'):].transpose()[0].tolist()
         drn_vals = drn[drn['well'] == row['Well']].iloc[:,drn.columns.get_loc('1.1'):].transpose()[0].tolist()

         cycle = 1
         for r,d in zip(rn_vals, drn_vals):
            amplification_data = {
               'results': results_uri,
               'cycle': cycle,
               'rn': r,
               'delta_rn': d
            }
            res, status = lims_request('POST', amplification_url, json_data=amplification_data)
            if not assert_warn(status == 201, 'error creating rn point (pcrplate:{}, amplification_data:{})'.format(platebc, amplification_data)):
               continue

            cycle += 1

      # Store parsing output
      rn['bcd'] = platebc
      rnlist = rn.melt(id_vars=['bcd', 'well','rep'], var_name='cycle', value_name='Rn')
      rnlist['cycle'] = rnlist['cycle'].astype(int)
      rnlist.columns = ['Plate_Barcode', 'Well', 'Reporter', 'Cycle', 'Rn']
      rnlist = rnlist.sort_values(by=['Well','Cycle'])
      results[['Well','Plate_barcode','Ct']].to_csv('{}_out.tsv'.format(platebc), sep='\t', index=False)
      rnlist.to_csv('{}_rn.tsv'.format(platebc), sep='\t', index=False)
