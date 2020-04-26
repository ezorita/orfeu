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

# EXPERIMENT DEFINITIONS
default_ct_threshold = 40
positive_control_wells = [357,358,359,360,381,382,383,384]
negative_control_wells = [1,2,3,4,25,26,27,28]

# Expected amplification on A1, A2, B1, respectively
positive_control_amplif = [True, True, True]
negative_control_amplif = [False, False, False]

# NOTIFICATION EMAIL SETTINGS
email_receivers = ["eduard.zorita@crg.eu"]
email_port = 465  # For SSL
smtp_server = "smtp.gmail.com"
email_sender = os.environ.get('LIMS_EMAIL_ADDRESS')
email_password = os.environ.get('LIMS_EMAIL_PASSWORD')
if email_password is None:
   print("ERROR: Plase export environment variables LIMS_EMAIL_ADDRESS and LIMS_EMAIL_PASSWORD to enable email notifications")
   sys.exit(1)

# API DEFINITIONS

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

job_name = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
execution_error_critical = ''
execution_error_cnt = 0
execution_warning_cnt = 0
sync_successful = []
sync_warning = []
sync_error = []

def notify_errors():
   import smtplib, ssl
   if execution_error_critical != '':
      subject = "[!] LIMS sync job {} failed".format(job_name)
   elif execution_error_cnt > 0:
      subject = "[!] LIMS sync job {} completed with errors".format(job_name)
   elif execution_warning_cnt > 0:
      subject = "[!] LIMS sync job {} completed with warnings".format(job_name)
   else:
      subject = "LIMS sync job {} completed successfuly".format(job_name)      
      
   message = """\
   Subject: {}

   This message is sent from Python.""".format(subject)
   
   context = ssl.create_default_context()
   with smtplib.SMTP_SSL(smtp_server, email_port, context=context) as server:
      server.login(email_sender, email_password)
      server.sendmail(email_sender, email_receivers, message)


def assert_critical(cond, msg):
   if not cond:
      print(msg) # LOG msg instead
      # Notify and exit (function)
      sys.exit(1)
   return cond

def assert_error(cond, msg):
   if not cond:
      print(msg) # LOG msg instead
      #execution_error_cnt += 1
   return cond

def assert_warn(cond, msg):
   if not cond:
      print(msg) # LOG msg instead
      #execution_warning_cnt += 1
   return cond

###
### DIAGNOSIS
###

def compute_diagnosis(samples):
   if samples == [False, False, False]\
      or samples == [False, True, False]\
      or samples == [True, False, True]\
      or samples == [False, True, True]:
      return 'I'
   
   elif samples == [True, True, False]\
        or samples == [True, True, True]:
      return 'P'

   elif samples == [False, False, True]:
      return 'N'

   else: return None
   
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
   r = requests.request(method, url, params=params, headers=headers, json=json_data)#, verify=False)
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

   # Get list of pcr plates
   # Need a wrapper to check that the request worked
   pcrplates, status = lims_request('GET', url=pcrplate_url, params={'limit': 1000000})
   assert_error(status < 300, 'Could not retreive pcr plates from LIMS')
   pcrplates = pcrplates.json()['objects']
   pcrplates_barcodes = [pcrplate['barcode'] for pcrplate in pcrplates]

   # Get list of detector ids
   detectors, status = lims_request('GET', url=detector_url, params={'limit': 1000000})
   assert_error(status < 300, 'Could not retreive pcr detectors from LIMS')
   detector_ids = {detector['name'].lower(): detector['resource_uri'] for detector in detectors.json()['objects']}
  
   # Find all processed samples in path
   flist = glob.glob('{}/*_results.txt'.format(path))
   for fname in flist:
      platebc = fname.split('/')[-1].split('_results.txt')[0]
      print('[pcrplate={}] BEGIN sample processing'.format(platebc))
      
      # Check if PCRPLATE is already in LIMS (TODO: also check if status is PROCESSING)
      if not assert_warn(platebc in pcrplates_barcodes, '[pcrplate={}] plate barcode not present in LIMS system, cannot sync data until it is created. skipping pcrplate'.format(platebc)):
         continue


      ##
      ## CHECK SYNC STATUS
      ##

      plateobj = [p for p in pcrplates if p['barcode'].lower() == platebc][0]
      print('[pcrplate={}] PCRPLATE found in LIMS (id:{}, uri:{})'.format(platebc, plateobj['id'], plateobj['resource_uri']))

      # Check if pcrrun for this plate already exists
      r, status = lims_request('GET', url=pcrrun_url, params={'pcr_plate__barcode__exact': platebc})
      if not assert_warn(status == 200, '[pcrplate={}] error checking presence of PCRRUN. skipping pcrplate'.format(platebc)):
         continue

      if len(r.json()['objects']) > 0:
         print("[pcrplate={}] PCRRUN info already in LIMS. no need to sync".format(platebc))
         continue

      ##
      ## PARSE QPCR OUTPUT
      ##

      # Parse results
      results, run_date = parse_results(fname)
      rn, drn = parse_rn('{}/{}_clipped.txt'.format(path, platebc))

      # Format results
      results['Ct'] = results['Ct'].apply(rename_Ct)
      results['plate_barcode'] = platebc


      ##
      ## CREATE QPCR RUN
      ##
            
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
         'status': 'OK',
         'comments': None
      }
      r, status = lims_request('POST', pcrrun_url, json_data=pcrrun_data)
      if not assert_warn(status == 201, '[pcrplate={}] error creating PCRRUN in LIMS. skipping pcrplate'.format(platebc)):
         continue
      pcrrun_uri = r.headers['Location']
      print('[pcrplate={}/pcrrun] post(pcrrun) = {} (uri:{})'.format(platebc, status, pcrrun_uri))

      # Get all PCRWELL for this PCRPLATE
      r, status = lims_request('GET', url=pcrwell_url, params={'limit': 10000, 'pcr_plate__barcode__exact': platebc})
      if not assert_warn(status == 200, '[pcrplate={}/pcrwell] error getting PCRWELLs for this PCRPLATE. skipping pcrplate. (Note: I have created PCRRUN {})'.format(platebc, pcrrun_uri)):
         continue

      # Note: PCRWELL position is in A1, A2... format
      pcrwells = r.json()['objects']
      print('[pcrplate={}/pcrwell] len(pcrplate={}/pcrwell) = {}'.format(platebc, platebc, len(pcrwells)))

      # Create a lookup table of well_position -> well_id
      pcrwell_pos_to_uri = {p['position'].upper():p['resource_uri'] for p in pcrwells}

      # NOW NEED TO FILL IN AL WELL INFO IN THE SAME PCRWELL OBJECTS AND PATCH THEM BACK! THE REST (results) IS IDENTICAL! :)

      # Create diagnosis for each PCRWELL
      diagnosis = [[None,None,None] for i in range(385)]

      for row in results.iterrows():
         i = row[0]
         row = row[1]
         well_num = int(row['Well'])
         pcrwell_pos = chr(65+(well_num-1)//24)+str((well_num-1)%24+1)
         if not assert_warn(pcrwell_pos in pcrwell_pos_to_uri, '[pcrplate={}/pcrwell] well {} not found in LIMS. skipping well'.format(platebc, pcrwell_pos)):
            continue
         
         ##
         ## CREATE RESULTS ENTRY
         ##

         threshold = default_ct_threshold if pd.isna(row['Threshold']) else row['Threshold']
         ct = None if row['Ct'] == 'NA' else row['Ct']
         amplification = None if threshold is None else False if ct is None else float(ct) <= threshold

         if not assert_warn(row['Detector Name'].lower() in detector_ids, '[pcrplate={}/pcrwell={}] detector {} not found in LIMS. setting to "None"'.format(platebc, pcrwell_pos, row['Detector Name'])):
            detector_id = None
         else:
            detector_id = detector_ids[row['Detector Name'].lower()]

         results_data = {
            'id':None,
            'pcr_well': pcrwell_pos_to_uri[pcrwell_pos],
            'comments': None,
            'date_analysis': datetime.datetime.now().isoformat(),
            'date_sent': datetime.datetime.now().isoformat(),
            'amplification': amplification,
            'threshold': threshold,
            'detector': detector_id,
            'detector_lot_number': None,
            'ct': ct
         }

         # Store amplification in diagnosis table
         if ((well_num-1)//24)%2:
            if (well_num-1)%2: # B2 (empty)
               dpos = None
            else: # B1
               dpos = well_num-24
               samp = 2
         else:
            if (well_num-1)%2: # A2
               dpos = well_num-1
               samp = 1
            else: # A1
               dpos = well_num
               samp = 0

         if dpos:
            diagnosis[dpos][samp] = diagnosis[dpos+1][samp] = diagnosis[dpos+24][samp] = amplification

         # Make a POS to create RESULTS entry
         r, status = lims_request('POST', results_url, json_data=results_data)
         if not assert_warn(status == 201, '[pcrplate={}/pcrwell={}/results] error creating RESULTS. skipping pcrwell'.format(platebc, pcrwell_pos)):
            continue
         results_uri = r.headers['Location']

         print('[pcrplate={}/pcrwell={}/results] post(results) = {} (uri:{})'.format(platebc, pcrwell_pos, status, results_uri))

         
         ##
         ## CREATE RN CURVES
         ##
         
         rn_vals = rn[rn['well'] == row['Well']].iloc[:,rn.columns.get_loc('1'):].transpose().iloc[:,0].tolist()
         drn_vals = drn[drn['well'] == row['Well']].iloc[:,drn.columns.get_loc('1.1'):].transpose().iloc[:,0].tolist()

         amplification_data = []
         cycle = 1
         for r,d in zip(rn_vals, drn_vals):
            amplification_data.append({
               'results': results_uri,
               'cycle': cycle,
               'rn': r,
               'delta_rn': d
            })
            cycle += 1
            
         _, status = lims_request('PATCH', amplification_url, json_data={'objects': amplification_data})
         if not assert_warn(status < 300, '[pcrplate={}/pcrwell={}/amplification_data] error in PATCH request to create Rn'.format(platebc, pcrwell_pos)):
            continue
         print('[pcrplate={}/pcrwell={}/results/amplificationdata] patch/post(amplificationdata) = {}'.format(platebc, pcrwell_pos, status))

      # All wells processed at this point, time to update pcrwell
      for pcrwell in pcrwells:
         dpos = int((ord(pcrwell['position'][0].upper())-65)*24 + int(pcrwell['position'][1:]))
         if dpos in positive_control_wells:
            pass_fail = 'P' if diagnosis[dpos] == positive_control_amplif else 'F'
            auto_diagnosis = None
         elif dpos in negative_control_wells:
            pass_fail = 'P' if diagnosis[dpos] == negative_control_amplif else 'F'
            auto_diagnosis = None
         else:
            pass_fail = 'NA'
            auto_diagnosis = compute_diagnosis(diagnosis[dpos])

         pcrwell['pass_fail'] = pass_fail
         pcrwell['automatic_diagnosis'] = auto_diagnosis

      # All wells have been processed, PATCH back to API
      _, status = lims_request('PATCH', pcrwell_url, json_data={'objects': pcrwells})
      if not assert_warn(status < 300, '[pcrplate={}/pcrwell={}/amplification_data] error in PATCH request to update PCRWELL'.format(platebc, pcrwell_pos)):
         continue
      print('[pcrplate={}/pcrwell={}] patch/update(pcrwell) = {}'.format(platebc, pcrwell_pos, status))

      # Store parsing output
      rn['bcd'] = platebc
      rnlist = rn.melt(id_vars=['bcd', 'well','rep'], var_name='cycle', value_name='Rn')
      rnlist['cycle'] = rnlist['cycle'].astype(int)
      #      rnlist.columns = ['Plate_Barcode', 'Well', 'Reporter', 'Cycle', 'Rn']
      #rnlist = rnlist.sort_values(by=['Well','Cycle'])
      results.to_csv('{}_out.tsv'.format(platebc), sep='\t', index=False)
      rnlist.to_csv('{}_rn.tsv'.format(platebc), sep='\t', index=False)
