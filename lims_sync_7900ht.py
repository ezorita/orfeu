# Requires Python version 3.5 or higher
import sys
if sys.version_info < (3,0):
      raise ImportError('Python version < 3.0 not supported')

import glob, os, io, re
import smtplib, ssl
import traceback
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import argparse
import requests
import datetime
import logging
from dateutil.parser import parse as date_parse
import pandas as pd

__version__ = '0.9'

# TODO: How to handle unfinished syncs (i.e. the connection broke during sync) --> verify script
#

# EXPERIMENT DEFINITIONS
default_ct_threshold = 40

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

job_name  = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
job_start = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

# API DEFINITIONS
base_url           = 'https://orfeu.cnag.crg.eu'
api_root           = '/prbblims/api/covid19/'
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
   log_level = logging.INFO
   log_file = '{}/{}.log'.format(log_path, job_name)
   log_format = '[%(asctime)s][%(levelname)s]%(message)s'

   logging.basicConfig(level=log_level, filename=log_file, format=log_format)

   return log_file


###
### DIAGNOSIS
###

status_code = {
   'N': 0, # Negative diagn.
   'P': 1, # Positive diagn.
   'I': 2, # Inconclusive diagn.
   'NAD': 3, # Not processed (no autodiagnosis)
   'EMP': 4, # Empty well
   'PCT': 5, # Passed control
   'FCT': 6, # Failed control
}

status_color = {
   status_code['N']:   'steelblue',
   status_code['P']:   'deeppink',
   status_code['I']:   'gold',
   status_code['NAD']: 'darkslategray',
   status_code['EMP']: 'lightgray',
   status_code['PCT']: 'darkgreen',
   status_code['FCT']: 'darkred'
}

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
### EMAIL NOTIFICATIONS
### 

email_receivers = ['']
smtp_server     = "smtp.gmail.com"
email_port      = 465

def html_digest(digest, log_file, tb):
   # Convert digest lists to sets
   digest['nofile']  = list(set(digest['nofile']))
   digest['noinfo']  = list(set(digest['noinfo']))
   digest['nowells'] = list(set(digest['nowells']))
   digest['success'] = list(set(digest['success']))
   digest['warning'] = list(set(digest['warning']))
   digest['error']   = list(set(digest['error']))

   job_end = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')
   
   html = '<html><head><style>\nth, td { text-align: center; padding: 10px; }\ntable, th, td { border: 1px solid black; }\n</style></head><body>'
   html_index = '- <a href="#description">Job description</a><br>'
   
   # Header with run description
   html += '<h1>LIMS update report</h1>\n'
   html += 'HTML_REPORT_INDEX'
   html += '<br><h2><a name="description"></a>Job description:</h2>\n'
   html += '<ul><li><b>Job name:</b> <span style="font-family:\'Courier New\'">{}</span></li>'.format(job_name)
   html += '<li><b>Job start:</b> <span style="font-family:\'Courier New\'">{}</span></li>'.format(job_start)
   html += '<li><b>Job end:</b> <span style="font-family:\'Courier New\'">{}</span></li>'.format(job_end)
   html += '<li><b>Script version:</b> <span style="font-family:\'Courier New\'">{}</span></li>'.format(__version__)   
   html += '<li><b>Command:</b> <span style="font-family:\'Courier New\'">{}</span></li>'.format(' '.join(sys.argv))
   html += '<li><b>Working directory:</b> <span style="font-family:\'Courier New\'">{}</span></li>'.format(os.getcwd())
   html += '<li><b>Log file:</b> <span style="font-family:\'Courier New\'">{}</span></li>'.format(log_file)
   html += '<li><b>Exit status:</b> <span style="font-family:\'Courier New\'">{}</span></li></ul>\n'.format(1 if tb else 0)

   if tb:
      html += '<br><h2>Cause of failure:</h2>'
      html += '<span style="font-family:\'Courier New\'">{}</span>'.format(tb.replace('<','&lt;').replace('>','&gt;').replace('\n','<br>'))
      
   # Update summaryn (only if there are samples to talk about)
   if len(digest['noinfo'])  > 0 or \
      len(digest['nofile'])  > 0 or \
      len(digest['nowells'])  > 0 or \
      len(digest['error'])   > 0 or \
      len(digest['warning']) > 0 or \
      len(digest['success']) > 0 or \
      len(digest['nowells']) > 0:
      html_index += '- <a href="#summary">Update summary</a><br>'
      html += '<br><h2><a name="summary"></a>Update summary:</h2>'

      #  No results file found
      if len(digest['nofile']) > 0:
         html += '<br>The following PCR runs <b>did not synchronize</b> because the PCR results have not been exported properly:</b>\n<ul>'
         for bcd in digest['nofile']:
            html += '<li>{}</li>'.format(bcd)
         html += '</ul>'
      
      #  No info
      if len(digest['noinfo']) > 0:
         html += '<br>The following PCR runs <b>did not synchronize</b> because the PCR plate has not been pre-registered in LIMS:</b>\n<ul>'
         for bcd in digest['noinfo']:
            html += '<li>{}</li>'.format(bcd)
         html += '</ul>'
         
      #  No pcrwells found
      if len(digest['nowells']) > 0:
         html += '<br>The following PCR runs <b>did not synchronize</b> because no PCR wells were found in LIMS (did you create the well layout?):</b>\n<ul>'
         for bcd in digest['nowells']:
            html += '<li>{}</li>'.format(bcd)
         html += '</ul>'

      #  Error
      if len(digest['error']) > 0:
         html += '<br>PCR plates with LIMS synchronization <b><span style="color:red">ERRORS</span></b>: (click to see log digest)\n<ul>'
         for bcd in digest['error']:
            html += '<li><a href="#{}error">{}</a></li>'.format(bcd, bcd)
         html += '</ul>'

      #  Warning
      if len(digest['warning']) > 0:
         html += '<br>PCR plates with LIMS synchronization <b><span style="color:orange">WARNINGS</span></b>: (click to see log digest)\n<ul>'
         for bcd in digest['warning']:
            html += '<li><a href="#{}warn">{}</a></li>'.format(bcd, bcd)
         html += '</ul>'

      #  Success (there is sample/control data)
      if len(digest['success']) > 0:
         html += '<br><b>List of synchronized PCR runs:</b>\n<ul>'
         for bcd, resync in digest['success']:
            html += '<li>{}{}</li>'.format(bcd, ' (resync)' if resync else '') 
         html += '</ul>'

         # Sample stats
         html_index += '- <a href="#stats">Sample stats</a><br>'
         html += '<br><h2><a name="stats"></a>Sample stats</h2>\n'
         html += '<table style="white-space:nowrap;"><tr>\
            <th>PCR barcode</th>\
            <th>Total Samples</th>\
            <td>Negative</td>\
            <td>Positive</td>\
            <td>Inconclusive</td>\
            <td>No AD</td>\
            <th>Total Controls</th>\
            <td>Passed</td>\
            <td>Failed</td>\
            </tr>'
                     
         for bcd in digest['sample']:
            # Compute sample frequencies
            freq = pd.Series([c for r in digest['sample'][bcd] for c in r]).value_counts()
            for s in status_code:
               if not status_code[s] in freq.index:
                  freq[status_code[s]] = 0

            # Fill table
            html += '<tr>'
            html += '<td><span style="font-family:\'Courier New\'">{}</span></td>'.format(bcd)
            html += '<td><span style="font-family:\'Courier New\'">{}</span></td>'.format(freq[status_code['P']]+freq[status_code['N']]+freq[status_code['I']]+freq[status_code['NAD']])
            html += '<td><span style="font-family:\'Courier New\'">{}</span></td>'.format(freq[status_code['N']])
            html += '<td><span style="font-family:\'Courier New\'">{}</span></td>'.format(freq[status_code['P']])
            html += '<td><span style="font-family:\'Courier New\'">{}</span></td>'.format(freq[status_code['I']])
            html += '<td><span style="font-family:\'Courier New\'">{}</span></td>'.format(freq[status_code['NAD']])
            html += '<td><span style="font-family:\'Courier New\'">{}</span></td>'.format(freq[status_code['PCT']]+freq[status_code['FCT']])
            html += '<td><span style="font-family:\'Courier New\'">{}</span></td>'.format(freq[status_code['PCT']])
            html += '<td><span style="font-family:\'Courier New\'">{}</span></td>'.format(freq[status_code['FCT']])
            html += '</tr>'
         html += '</table>'

         # PCR plate viz

         # Legend
         html_index += '- <a href="#sampviz">Sample visualization</a><br>'
         html += '<br><h2><a name="sampviz"></a>Sample visualization</h2>\n'
         html += '<table style="white-space:nowrap; empty-cells: show;"><tr>'
         html += '<td style="background-color:{}">&nbsp;</td><td>Negative</td>'.format(status_color[status_code['N']])
         html += '<td style="background-color:{}">&nbsp;</td><td>Positive</td>'.format(status_color[status_code['P']])
         html += '<td style="background-color:{}">&nbsp;</td><td>Inconclusive</td>'.format(status_color[status_code['I']])
         html += '<td style="background-color:{}">&nbsp;</td><td>No-AD</td>'.format(status_color[status_code['NAD']])
         html += '<td style="background-color:{}">&nbsp;</td><td>Empty</td>'.format(status_color[status_code['EMP']])
         html += '<td style="background-color:{}">&nbsp;</td><td>Control OK</td>'.format(status_color[status_code['PCT']])
         html += '<td style="background-color:{}">&nbsp;</td><td>Control FAIL</td>'.format(status_color[status_code['FCT']])
         html += '</tr></table>'
         for bcd in digest['sample']:
            html += '<h3>PCR run: {}</h3>\n'.format(bcd)

            # Table
            html += '<table style="empty-cells: show;"><tr>'
            # Add headers (numbers)
            for i in range(13):
               html += '<th style="width:18px;"><span style="font-family:\'Courier New\'">{}</span></th>'.format(i if i else '')
            html += '</tr>'

            # Add rows
            for i, r in enumerate(digest['sample'][bcd]):
               html += '<tr><th><span style="font-family:\'Courier New\'">{}</span></th>'.format(chr(65+i))
               for c in r:
                  html += '<td style="background-color:{}">&nbsp;</td>'.format(status_color[c])
               html += '</tr>'
            html += '</table>'
         # Control checks
         html_index += '- <a href="#controls">Control checks</a><br>'
         html += '<br><h2><a name="controls"></a>Control checks</h2>\n'

         control_bcd = list(digest['control'].keys())

         if len(control_bcd) > 0:
            # Table header
            control_names = digest['control'][control_bcd[0]].keys()
            html += '<table style="white-space:nowrap"><tr><th>PCR barcode</th>'
            for cname in control_names:
               html += '<th>{}</th>'.format(cname)
            html += '</tr>'

            # Table content
            for bcd in control_bcd:
               html += '<tr><td>{}</td>'.format(bcd)
               for ctl in digest['control'][bcd]:
                  conds = list(set(digest['control'][bcd][ctl]))
                  html += '<td>'
                  if len(conds) > 0:
                     for cond in conds:
                        html += '<span style="color:{}"><b>{}</b></span>({}) '.format('green' if cond[1] == 'P' else 'red', 'Pass' if cond[1] == 'P' else 'Fail', cond[0])
                  else:
                     html += 'None'
                  html += '</td>'

               html +='</tr>'
            html += '</table>'

         else:
            html += 'No control samples found!'

      # Log digests
      if len(digest['error']) > 0 or len(digest['warning']) > 0:
         html_index += '- <a href="#logs">Log digest</a><br>'
         html += '<br><h2><a name="logs"></a>Log digest</h2>\n'
         # Error logs
         if len(digest['error']) > 0:
            # Grep error log from file
            with open(log_file) as f:
               error_lines = [line for line in f if re.search(r'ERROR', line)]

            html += '<h3>Error logs:</h3>'
            for bcd in digest['error']:
               pattern = 'pcrplate={}'.format(bcd)
               html += '<br><a name="{}error"></a>Error log for {}:\n'.format(bcd,bcd)
               html += '<br><p style="font-family:\'Courier New\'">{}</p><br>'.format('<br>'.join([line for line in error_lines if re.search(pattern, line)]))

         # Warning logs
         if len(digest['warning']) > 0:
            # Grep warning log from log file
            with open(log_file) as f:
               warn_lines = [line for line in f if re.search(r'WARNING', line)]

            html += '<h3>Warning logs:</h3>'
            for bcd in digest['warning']:
               pattern = 'pcrplate={}'.format(bcd)
               html += '<br><a name="{}warn"></a>Warning log for {}:\n'.format(bcd,bcd)
               html += '<br><p style="font-family:\'Courier New\'">{}</p><br>'.format('<br>'.join([line for line in warn_lines if re.search(pattern, line)]))

            
   html += "</body></html>"

   # Add index at the top
   html = html.replace('HTML_REPORT_INDEX', html_index)

   return MIMEText(html, 'html')


def send_digest(digest, log_file, tb=None):
   message = MIMEMultipart()
   message['From']    = 'PRBB LIMS <{}>'.format(EMAIL_SENDER)
   message['Subject'] = "LIMS update {} ({})".format('report' if tb is None else 'FAILED', job_name)
   message['Bcc']     = ','.join(email_receivers)
   message.attach(html_digest(digest, log_file, tb))
   
   context = ssl.create_default_context()
   with smtplib.SMTP_SSL(smtp_server, email_port, context=context) as server:
      server.login(EMAIL_SENDER, EMAIL_PASSWORD)
      server.sendmail(EMAIL_SENDER, email_receivers, message.as_string())

      
###
### ERROR CONTROL
###

tb = None

def assert_critical(cond, msg):
   if not cond:
      logging.critical(msg)
      raise AssertionError
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
### LIMS REQUEST METHODS
###

req_headers = {'content-type': 'application/json', 'Authorization': 'ApiKey {}:{}'.format(LIMS_USER, LIMS_PASSWORD) };

def lims_request(method, url, params=None, json_data=None, headers=req_headers):
   # methods: GET, OPTIONS, HEAD, POST, PUT, PATCH, DELETE
   r = requests.request(method, url, params=params, headers=headers, json=json_data, verify=False)
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
      rows = [line for line in f_in if re.search(r'^[0-9]', line)]


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
   logpath = setup_logger(options.logpath).replace('//','/')

   try:
      # Digest structure
      digest = {
         'skipped': [],
         'nofile':  [],
         'noinfo':  [],
         'nowells': [],
         'success': [],
         'warning': [],
         'error':   [],
         'control': {},
         'sample':  {}
      }

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
         resync  = False
         platebc = fname.split('/')[-1].split('_results.txt')[0]

         # Check if PCRPLATE is already in LIMS (TODO: also check if status is PROCESSING)
         if not assert_warning(platebc in pcrplates_barcodes, '[pcrplate={}] pcrplate/barcode not present in LIMS system, cannot sync data until it is created'.format(platebc)):
            logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
            digest['noinfo'].append(platebc)
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
            digest['error'].append(platebc)
            continue

         if len(r.json()['objects']) > 0:
            logging.info("[pcrplate={}] pcrrun info already in LIMS".format(platebc))
            digest['skipped'].append(platebc)
            continue

         logging.info('[pcrplate={}] BEGIN pcrplate processing'.format(platebc))

         ##
         ## CHECK IF RESYNC NEEDED
         ##

         r, status = lims_request('GET', results_url, params={'limit':10000, 'pcr_well__pcr_plate__barcode__exact':platebc})
         if not assert_error(status == 200, '[pcrplate={}] error checking presence of RESULTS'.format(platebc)):
            logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
            digest['error'].append(platebc)
            continue


         res_objs = r.json()['objects']
         if len(res_objs) > 0:
            ##
            ## RESYNC: DELETE CURRENT RESULTS
            ##

            logging.info('[pcrplate={}] results information is already in LIMS: RESYNC'.format(platebc))
            resync = True

            # Check first
            if not assert_error(len(res_objs) <= 384, '[pcrplate={}] error when querying RESULTS for this plate, got {} objects'.format(platebc, len(res_objs))):
               digest['error'].append(platebc)
               continue

            results_ids = [o['id'] for o in res_objs]

            # Delete current results
            for r_id in results_ids:
               if not assert_error(r_id, '[pcrplate={}] avoiding full DELETE, for some reason results_id="". ABORT PLATE'.format(platebc, len(res_objs))):
                  digest['error'].append(platebc)
                  continue
               del_uri = '{}/{}'.format(results_url, r_id)
               lims_request('DELETE', del_uri)
               logging.info('[pcrplate={}/results={}] deleted RESULTS entry in LIMS (uri: {})'.format(platebc,r_id,del_uri))

         ##
         ## GET PCRWELLS
         ##

         # Get all PCRWELL for this PCRPLATE
         r, status = lims_request('GET', url=pcrwell_url, params={'limit': 10000, 'pcr_plate__barcode__exact': platebc})
         if not assert_error(status == 200, '[pcrplate={}/pcrwell] error getting PCRWELLs for this PCRPLATE'.format(platebc)):
            logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
            digest['error'].append(platebc)
            continue

         # PCRWELL position is in A1, A2, B1 format
         pcrwells = r.json()['objects']

         if not assert_warning(len(pcrwells) > 0, '[pcrplate={}] no pcrwells found in LIMS for this pcrplate'.format(platebc)):
            digest['nowells'].append(platebc)
            continue
         else:
            logging.info('[pcrplate={}/pcrwell] len(pcrplate={}/pcrwell) = {}'.format(platebc, platebc, len(pcrwells)))
            
         # Create a lookup table of well_position -> well_id
         pcrwell_pos_to_uri = {p['position'].upper():p['resource_uri'] for p in pcrwells}

         # Create diagnosis for each PCRWELL
         diagnosis = [[None,None,None] for i in range(385)]

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
         ## DIGEST DATA
         ##

         # Prepare digest sample structure
         digest['sample'][platebc]  = [[status_code['EMP']]*12 for x in range(8)]

         # Prepare digest control structure
         digest['control'][platebc] = {ct: list() for ct in control_amplif}


         ##
         ## PARSE PCR OUTPUT FILES
         ##

         # Check that results file exists
         clipped_fname = '{}/{}_clipped.txt'.format(path, platebc)
         if not assert_error(os.path.isfile(fname), '[pcrplate={}] qPCR results file not found: {}'.format(platebc, fname)):
            digest['error'].append(platebc)
            digest['nofile'].append(platebc)
            continue

         if not assert_error(os.path.isfile(clipped_fname), '[pcrplate={}] qPCR clipped file not found: {}'.format(platebc, clipped_fname)):
            digest['error'].append(platebc)
            digest['nofile'].append(platebc)
            continue

         # Check _results.txt file header
         with open(fname) as f:
            firstline = f.readline()
            if not assert_error(re.search('Results',firstline), '[pcrplate={}] SDS Results header not found in: {}'.format(platebc, fname)):
               digest['error'].append(platebc)
               digest['nofile'].append(platebc)
               continue
               
         # Check _clipped.txt file header
         with open(clipped_fname) as f:
            firstline = f.readline()
            if not assert_error(re.search('Clipped',firstline), '[pcrplate={}] SDS Clipped header not found in: {}'.format(platebc, clipped_fname)):
               digest['error'].append(platebc)
               digest['nofile'].append(platebc)
               continue
         
         # Parse results file
         results, run_date = parse_results(fname)

         # Parse clipped file
         rn, drn = parse_rn(clipped_fname)

         # Format results
         results['Ct'] = results['Ct'].apply(rename_Ct)
         results['plate_barcode'] = platebc

         # Format parsed output paths
         results_outfile = '{}/{}_out.tsv'.format(outpath, platebc)
         rn_outfile = '{}/{}_rn.tsv'.format(outpath, platebc)

         fail_flag = False
         for row in results.iterrows():
            i = row[0]
            row = row[1]
            well_num = int(row['Well'])
            pcrwell_pos = chr(65+(well_num-1)//24)+str((well_num-1)%24+1)
            logging.info('[pcrplate={}/pcrwell={}] BEGIN pcrwell processing'.format(platebc, pcrwell_pos))

            if not (pcrwell_pos in pcrwell_pos_to_uri):
               logging.info('[pcrplate={}/pcrwell] well {} not found in LIMS'.format(platebc, pcrwell_pos))
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
               digest['warning'].append(platebc)
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

            # Store amplification in diagnosis table (WARN: ASSUMES LOCAL SINGLEPLEX)
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
               digest['error'].append(platebc)
               fail_flag = True
               break

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
               logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
               digest['error'].append(platebc)
               fail_flag = True
               break
            logging.info('[pcrplate={}/pcrwell={}/results/amplificationdata] patch/post(amplificationdata) = {}'.format(platebc, pcrwell_pos, status))


         if fail_flag:
            continue
         ##
         ## AUTOMATIC DIAGNOSIS (SINGLEPLEX SPECIFIC CODE)
         ##

         for pcrwell in pcrwells:
            dpos = int((ord(pcrwell['position'][0].upper())-65)*24 + int(pcrwell['position'][1:]))
            auto_diagnosis = compute_diagnosis(diagnosis[dpos])
            
            # Find base position, this is the top left well of each singleplexed sample (WARN: ASSUMES LOCAL SINGLEPLEX)
            if ((dpos-1)//24)%2:
               base_pos = dpos-25 if (dpos-1)%2 else dpos-24
            else:
               base_pos = dpos-1 if (dpos-1)%2 else dpos
               
            # Find row/column in 96-well plate
            row = (base_pos-1)//48
            col = ((base_pos-1)%48)//2

            # Check if control well has the expected amplification
            if pcrwell['position'] in control_type:
               pass_fail = diagnosis[dpos] == control_amplif[control_type[pcrwell['position']]]
               pass_fail = 'P' if pass_fail else 'F'

               # Store control status in control check
               w384_pos = chr(65+(base_pos-1)//24)+str((base_pos-1)%24+1)
               digest['control'][platebc][control_type[w384_pos]].append((w384_pos, pass_fail))
               
               # Store control status in sample digest
               digest['sample'][platebc][row][col] = status_code['PCT' if pass_fail == 'P' else 'FCT']
               
            else:
               pass_fail = 'NA'
               # Store sample diagnosis in sample digest
               digest['sample'][platebc][row][col] = status_code['NAD' if auto_diagnosis is None else auto_diagnosis]

            pcrwell['pass_fail'] = pass_fail
            pcrwell['automatic_diagnosis'] = auto_diagnosis


         ##
         ## UPDATE PCRWELL
         ##
         
         # All wells have been processed, PATCH back to API
         _, status = lims_request('PATCH', pcrwell_url, json_data={'objects': pcrwells})
         if not assert_error(status < 300, '[pcrplate={}/pcrwell] error in PATCH request to update pcrwell (autodiagnosis)'.format(platebc, pcrwell_pos)):
            logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
            digest['error'].append(platebc)
            continue
         logging.info('[pcrplate={}/pcrwell] patch/update(pcrwell) = {}'.format(platebc, status))


         ##
         ## CREATE PCR RUN
         ##

         # Now create PCRRUN, this way if we don't reach this point it will trigger
         # resync of the same sample in the next sync job.

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
            'analysis_result_file_path': fname,
            'status': 'R',
            'comments': None
         }

         # POST request (pcrplate)
         r, status = lims_request('POST', pcrrun_url, json_data=pcrrun_data)
         if not assert_error(status == 201, '[pcrplate={}] error creating PCRRUN in LIMS'.format(platebc)):
            logging.info('[pcrplate={}] ABORT pcrplate processing'.format(platebc))
            digest['error'].append(platebc)
            continue

         # Log new element uri
         pcrrun_uri = r.headers['Location']
         logging.info('[pcrplate={}/pcrrun] post(pcrrun) = {} (uri:{})'.format(platebc, status, pcrrun_uri))


         ##
         ## STORE PARSED RESULTS FILE
         ##

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
         digest['success'].append((platebc, resync))

   except AssertionError:
      # Flush log file
      logging.shutdown()
      # Critical assertion failed, print full log
      tb = 'CRITICAL assertion failed, log digest:\n\n'
      with open(logpath) as f:
         tb += f.read()
      print('Critical assertion failed, check logfile for details: {}'.format(logpath))
      
   except:
      # Print traceback
      tb = traceback.format_exc()
      print('Execution exception (sending traceback in e-mail digest):\n{}'.format(tb))
      
   finally:
      # Flush log file
      logging.shutdown()
      # Send digest e-mail if there is something interesting to report
      if tb \
         or len(digest['error']) > 0 \
         or len(digest['warning']) > 0\
         or len(digest['success']) > 0\
         or len(digest['noinfo']) > 0\
         or len(digest['nofile']) > 0\
         or len(digest['nowells']) > 0:

         send_digest(digest, logpath, tb)
