import glob, sys, os, io, re
import argparse
import requests
import datetime
import logging
from dateutil.parser import parse as date_parse
import pandas as pd

# TODO: How to handle unfinished syncs (i.e. the connection broke during sync) --> verify script
#

# INPUT ARGUMENTS
# - SDS data path
# - Log folder
# - Parsed output folder

# TODO:
# - Parse and sync from the same script.
# - Write all newly created elements in a file (at least their id), for tracking what needs to be deleted from lims in case the whole thing fails.
# - Write output files at the end of syncing

# EXPERIMENT DEFINITIONS
default_ct_threshold   = 40

# Expected amplification in controls (A1, A2, B1)
control_amplif = {
   'Neg':         [False, False, False],
   'Pos_RP':      [False, False, True],
   'Pos_RP_N1N2': [True,  True,  True]
}

# Get environment variables
LIMS_USER      = os.environ.get('LIMS_USER')
LIMS_PASSWORD  = os.environ.get('LIMS_PASSWORD')
EMAIL_SENDER   = os.environ.get('LIMS_EMAIL_ADDRESS')
EMAIL_PASSWORD = os.environ.get('LIMS_EMAIL_PASSWORD')

if LIMS_USER      is None or\
   LIMS_PASSWORD  is None or\
   EMAIL_SENDER   is None or\
   EMAIL_PASSWORD is None:
   print("ERROR: define environment variables LIMS_USER, LIMS_PASSWORD, LIMS_EMAIL_ADDRESS, LIMS_EMAIL_PASSWORD before running this script.")
   sys.exit(1)

# API DEFINITIONS
base_url           = 'https://orfeu.cnag.crg.eu'
api_root           = '/prbblims_devel/api/covid19/'
pcrplate_base      = '{}pcrplate/'.format(api_root)
pcrrun_base        = '{}pcrrun/'.format(api_root)
pcrwell_base       = '{}pcrwell/'.format(api_root)
detector_base      = '{}detector/'.format(api_root)
results_base       = '{}results/'.format(api_root)
amplification_base = '{}amplificationdata/'.format(api_root)
organization_base  = '{}organization/'.format(api_root)

pcrplate_url      = '{}{}'.format(base_url, pcrplate_base)
pcrwell_url       = '{}{}'.format(base_url, pcrwell_base)
pcrrun_url        = '{}{}'.format(base_url, pcrrun_base)
detector_url      = '{}{}'.format(base_url, detector_base)
results_url       = '{}{}'.format(base_url, results_base)
amplification_url = '{}{}'.format(base_url, amplification_base)
organization_url  = '{}{}'.format(base_url, organization_base)


###
### ARGUMENTS
###

def getOptions(args=sys.argv[1:]):
   parser = argparse.ArgumentParser('lims_sync')
   parser.add_argument('path', help='Input folder (where the *_results.txt and *_clipped.txt files are)')
   parser.add_argument('-o', '--output', help='Parsed output folder', required=True)
   parser.add_argument('-l', '--logpath', help='Root folder to store logs', required=True)
   options = parser.parse_args(args)
   return options

###
### LOGGING
###

def setup_logger(log_path):
   job_name = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
   log_level = logging.INFO
   log_file = '{}/{}.log'.format(log_path, job_name)
   log_format = '[%(asctime)s][%(levelname)s]%(message)s'

   logging.basicConfig(level=log_level, filename=log_file, format=log_format)

   return log_file


###
### EMAIL NOTIFICATIONS
### 

email_receivers = ["eduard.zorita@crg.eu"]
smtp_server     = "smtp.gmail.com"
email_port      = 465

def notify_errors():
   import smtplib, ssl
   if execution_error_critical != '':
      subject = "[!] LIMS sync job {} failed".format(job_name)
   elif execution_error_cnt > 0:
      subject = "[!] LIMS sync job {} completed with errors".format(job_name)
   elif execution_warning_cnt > 0:
      subject = "[!] LIMS sync job {} completed with warnings".format(job_name)
   else:
      subject = "LIMS sync job {} completed successfully".format(job_name)      
      
   message = """\
   Subject: {}

   This message is sent from Python.""".format(subject)
   
   context = ssl.create_default_context()
   with smtplib.SMTP_SSL(smtp_server, email_port, context=context) as server:
      server.login(email_sender, email_password)
      server.sendmail(email_sender, email_receivers, message)

      
###
### LOGGING / ERROR CONTROL
###

execution_error_critical = ''
execution_error_cnt      = 0
execution_warning_cnt    = 0
sync_successful          = []
sync_warning             = []
sync_error               = []


def assert_critical(cond, msg):
   if not cond:
      logging.critical(msg)
      sys.exit(1)
   return cond

def assert_error(cond, msg):
   if not cond:
      logging.error(msg)
   return cond

def assert_warning(cond, msg):
   if not cond:
      logging.warning(msg)
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

req_headers = {'content-type': 'application/json', 'Authorization': 'ApiKey {}:{}'.format(LIMS_USER, LIMS_PASSWORD) };

def lims_request(method, url, params=None, json_data=None, headers=req_headers):
   # methods: GET, OPTIONS, HEAD, POST, PUT, PATCH, DELETE
   r = requests.request(method, url, params=params, headers=headers, json=json_data)#, verify=False)
   assert_error(r.status_code < 300,
                  'LIMS request returned non-successful response ({}). Request details: METHOD={}, URL={}, PARAMS={}, DATA={}'.format(
                     r.status_code,
                     method,
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

   # Parse arguments
   options = getOptions(sys.argv[1:])
   path    = options.path
   outpath = options.output

   # Set up logger
   logpath = setup_logger(options.logpath)
   
   # Test LIMS connection
   _, status = lims_request('GET', base_url)
   assert_critical(status < 300, 'Test connection to LIMS API failed')

   # Get list of pcr plates
   pcrplates, status = lims_request('GET', url=pcrplate_url, params={'limit': 1000000})
   assert_critical(status < 300, 'Could not retreive pcr plates from LIMS')
   pcrplates = pcrplates.json()['objects']
   pcrplates_barcodes = [pcrplate['barcode'] for pcrplate in pcrplates]

   # Get list of detector ids
   detectors, status = lims_request('GET', url=detector_url, params={'limit': 1000000})
   assert_critical(status < 300, 'Could not retreive pcr detectors from LIMS')
   detector_ids = {detector['name'].lower(): detector['resource_uri'] for detector in detectors.json()['objects']}
  
   # Find all processed samples in path
   flist = glob.glob('{}/*_results.txt'.format(path))
   
   for fname in flist:
      platebc = fname.split('/')[-1].split('_results.txt')[0]
      logging.info('[pcrplate={}] BEGIN pcrplate processing'.format(platebc))
      
      # Check if PCRPLATE is already in LIMS (TODO: also check if status is PROCESSING)
      if not assert_warning(platebc in pcrplates_barcodes, '[pcrplate={}] pcrplate/barcode not present in LIMS system, cannot sync data until it is created'.format(platebc)):
         logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
         # Add to not_sync list
         continue


      ##
      ## CHECK SYNC STATUS
      ##

      plateobj = [p for p in pcrplates if p['barcode'].lower() == platebc.lower()][0]
      logging.info('[pcrplate={}] pcrplate found in LIMS (id:{}, uri:{})'.format(platebc, plateobj['id'], plateobj['resource_uri']))

      # Check if pcrrun for this plate already exists
      r, status = lims_request('GET', url=pcrrun_url, params={'pcr_plate__barcode__exact': platebc})
      if not assert_error(status == 200, '[pcrplate={}] error checking presence of PCRRUN'.format(platebc)):
         logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
         # Add to not_sync list
         continue

      if len(r.json()['objects']) > 0:
         logging.info("[pcrplate={}] pcrrun info already in LIMS".format(platebc))
         logging.info("[pcrplate={}] SKIP pcrplate processing".format(platebc))
         continue

      ##
      ## GET CONTROL POSITIONS
      ##

      # Get control positions
      control_type = {}
      for control_name in control_amplif:
         # Get request, filter by sample type
         r, status = lims_request("GET", url=pcrwell_url, params={'rna_extraction_well__sample__sample_type__name__exact': control_name, 'pcr_plate__barcode__exact': platebc})
         if not assert_error(status == 200, '[pcrplate={}/pcrwell] error retreiving control position (control name={})'.format(platebc, control_name)):
            logging.warning('[pcrplate={}] automatic control checking disabled for control name={}'.format(platebc, control_name))

         # Create a lookup table: well_position -> control type
         cp = {p['position'] : control_name for p in r.json()['objects']}
         control_type.update(cp)

         
      ##
      ## PARSE QPCR OUTPUT
      ##

      # Parse results
      results, run_date = parse_results(fname)
      rn, drn = parse_rn('{}/{}_clipped.txt'.format(path, platebc))

      # Format results
      results['Ct'] = results['Ct'].apply(rename_Ct)
      results['plate_barcode'] = platebc

      # Format parsed output paths
      results_outfile = '{}/{}_out.tsv'.format(outpath, platebc)
      rn_outfile = '{}/{}_rn.tsv'.format(outpath, platebc)


      ##
      ## CREATE QPCR RUN
      ##
            
      # pcrrun LIMS object
      pcrrun_data = {
         'id': None,
         'pcr_plate': plateobj['resource_uri'],
         'technician_id': None,
         'pcr_run_instrument_id': None,
         'pcr_run_protocol_id': None,
         'date_run': date_parse(run_date).isoformat(),
         'raw_results_file_path': fname,
         'results_file_path': results_outfile,
         'run_log_path': logpath,
         'analysis_result_file_path': fname, #TODO: UPDATE PATH
         'status': 'OK',
         'comments': None
      }

      # POST request (pcrplate)
      r, status = lims_request('POST', pcrrun_url, json_data=pcrrun_data)
      if not assert_error(status == 201, '[pcrplate={}] error creating PCRRUN in LIMS'.format(platebc)):
         logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
         # Add to not_sync list
         continue

      # Get new element uri
      pcrrun_uri = r.headers['Location']
      logging.info('[pcrplate={}/pcrrun] post(pcrrun) = {} (uri:{})'.format(platebc, status, pcrrun_uri))

      # Get all PCRWELL for this PCRPLATE
      r, status = lims_request('GET', url=pcrwell_url, params={'limit': 10000, 'pcr_plate__barcode__exact': platebc})
      if not assert_error(status == 200, '[pcrplate={}/pcrwell] error getting PCRWELLs for this PCRPLATE'.format(platebc)):
         logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
         # Add to not_sync list
         continue

      # PCRWELL position is in A1, A2, B1 format
      pcrwells = r.json()['objects']
      logging.info('[pcrplate={}/pcrwell] len(pcrplate={}/pcrwell) = {}'.format(platebc, platebc, len(pcrwells)))

      # Create a lookup table of well_position -> well_id
      pcrwell_pos_to_uri = {p['position'].upper():p['resource_uri'] for p in pcrwells}

      # Create diagnosis for each PCRWELL
      diagnosis = [[None,None,None] for i in range(385)]

      for row in results.iterrows():
         i = row[0]
         row = row[1]
         well_num = int(row['Well'])
         pcrwell_pos = chr(65+(well_num-1)//24)+str((well_num-1)%24+1)
         logging.info('[pcrplate={}/pcrwell={}] BEGIN pcrwell processing'.format(platebc, pcrwell_pos))
         
         if not assert_warning(pcrwell_pos in pcrwell_pos_to_uri, '[pcrplate={}/pcrwell] well {} not found in LIMS'.format(platebc, pcrwell_pos)):
            logging.info('[pcrplate={}/pcrwell={}] ABORT pcrwell processing'.format(platebc, pcrwell_pos))
            continue
         
         ##
         ## RESULTS
         ##

         # Ct and amplification
         threshold     = default_ct_threshold if pd.isna(row['Threshold']) else row['Threshold']
         ct            = None if row['Ct'] == 'NA' else row['Ct']
         amplification = None if threshold is None else False if ct is None else float(ct) <= threshold

         # qPCR detector
         if not assert_warning(row['Detector Name'].lower() in detector_ids, '[pcrplate={}/pcrwell={}] detector {} not found in LIMS, setting to "None"'.format(platebc, pcrwell_pos, row['Detector Name'])):
            detector_id = None
         else:
            detector_id = detector_ids[row['Detector Name'].lower()]

         # results LIMS object
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

         # POST request (results)
         r, status = lims_request('POST', results_url, json_data=results_data)
         if not assert_error(status == 201, '[pcrplate={}/pcrwell={}/results] error creating results'.format(platebc, pcrwell_pos)):
            logging.info('[pcrplate={}/pcrwell={}] ABORT pcrwell processing'.format(platebc, pcrwell_pos))
            continue

         # Get new element uri
         results_uri = r.headers['Location']
         logging.info('[pcrplate={}/pcrwell={}/results] post(results) = {} (uri:{})'.format(platebc, pcrwell_pos, status, results_uri))

         
         ##
         ## RN/DELTA_RN CURVES
         ##
         
         rn_vals  = rn [rn['well']  == row['Well']].iloc[:,rn.columns.get_loc('1'):].transpose().iloc[:,0].tolist()
         drn_vals = drn[drn['well'] == row['Well']].iloc[:,drn.columns.get_loc('1.1'):].transpose().iloc[:,0].tolist()

         # Create a list of amplificationdata objects
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

         # PATCH request (amplificationdata)
         _, status = lims_request('PATCH', amplification_url, json_data={'objects': amplification_data})
         if not assert_error(status < 300, '[pcrplate={}/pcrwell={}/amplificationdata] error in PATCH request to create Rn'.format(platebc, pcrwell_pos)):
            continue
         logging.info('[pcrplate={}/pcrwell={}/results/amplificationdata] patch/post(amplificationdata) = {}'.format(platebc, pcrwell_pos, status))

         
      ##
      ## AUTOMATIC DIAGNOSIS
      ##

      for pcrwell in pcrwells:
         dpos = int((ord(pcrwell['position'][0].upper())-65)*24 + int(pcrwell['position'][1:]))

         # Check if control well has the expected amplification
         if pcrwell['position'] in control_type:
            import pdb; pdb.set_trace()
            pass_fail = diagnosis[dpos] == control_amplif[control_type[pcrwell['position']]]
            pass_fail = 'P' if pass_fail else 'F'

            if pass_fail == 'F':
               # TODO: Report that control has failed.
               pass
         else:
            pass_fail = 'NA'
            
         auto_diagnosis = compute_diagnosis(diagnosis[dpos])

         pcrwell['pass_fail'] = pass_fail
         pcrwell['automatic_diagnosis'] = auto_diagnosis

      # All wells have been processed, PATCH back to API
      _, status = lims_request('PATCH', pcrwell_url, json_data={'objects': pcrwells})
      if not assert_error(status < 300, '[pcrplate={}/pcrwell] error in PATCH request to update pcrwell (autodiagnosis)'.format(platebc, pcrwell_pos)):
         logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
         # Add to not_sync list
         continue
      logging.info('[pcrplate={}/pcrwell] patch/update(pcrwell) = {}'.format(platebc, status))

      # Store parsing output
      rn['bcd'] = platebc
      rnlist = rn.melt(id_vars=['bcd', 'well','rep'], var_name='cycle', value_name='Rn')
      rnlist['cycle'] = rnlist['cycle'].astype(int)
      #      rnlist.columns = ['Plate_Barcode', 'Well', 'Reporter', 'Cycle', 'Rn']
      #rnlist = rnlist.sort_values(by=['Well','Cycle'])
      results.to_csv(results_outfile, sep='\t', index=False)
      logging.info('[pcrplate={}] parsed results exported to: {}'.format(platebc, results_outfile))
      
      rnlist.to_csv(rn_outfile, sep='\t', index=False)
      logging.info('[pcrplate={}] export Rn/Delta_Rn values to: {}'.format(platebc, rn_outfile))
      
      logging.info('[pcrplate={}] SUCCESS pcrplate processing'.format(platebc))
      # Add to synced list
