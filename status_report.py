import sys, os, glob
import requests
import logging
import datetime
import argparse
import traceback
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Get environment variables
LIMS_USER       = os.environ.get('LIMS_USER')
LIMS_PASSWORD   = os.environ.get('LIMS_PASSWORD')
EMAIL_SENDER    = os.environ.get('LIMS_EMAIL_ADDRESS')
EMAIL_PASSWORD  = os.environ.get('LIMS_EMAIL_PASSWORD')
EMAIL_RECEIVERS = os.environ.get('LIMS_EMAIL_RECEIVERS')

if LIMS_USER       is None or\
   LIMS_PASSWORD   is None or\
   EMAIL_SENDER    is None or\
   EMAIL_RECEIVERS is None or\
   EMAIL_PASSWORD  is None:
   print("ERROR: define environment variables LIMS_USER, LIMS_PASSWORD, LIMS_EMAIL_ADDRESS, LIMS_EMAIL_PASSWORD, LIMS_EMAIL_RECEIVERS before running this script.")
   sys.exit(1)

job_name  = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
job_start = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

# API DEFINITIONS
base_url           = 'https://orfeu.cnag.crg.eu'
api_root           = '/prbblims/api/covid19/'
project_base       = '{}project/'.format(api_root)
pcrplate_base      = '{}pcrplate/'.format(api_root)
pcrproject_base    = '{}pcrplateproject/'.format(api_root)
pcrrun_base        = '{}pcrrun/'.format(api_root)
pcrwell_base       = '{}pcrwell/'.format(api_root)
detector_base      = '{}detector/'.format(api_root)
results_base       = '{}results/'.format(api_root)
amplification_base = '{}amplificationdata/'.format(api_root)
rnaplate_base      = '{}rnaextractionplate/'.format(api_root)
rnawell_base       = '{}rnaextractionwell/'.format(api_root)
organization_base  = '{}organization/'.format(api_root)

project_url       = '{}{}'.format(base_url, project_base)
pcrplate_url      = '{}{}'.format(base_url, pcrplate_base)
pcrproject_url    = '{}{}'.format(base_url, pcrproject_base)
pcrwell_url       = '{}{}'.format(base_url, pcrwell_base)
pcrrun_url        = '{}{}'.format(base_url, pcrrun_base)
detector_url      = '{}{}'.format(base_url, detector_base)
results_url       = '{}{}'.format(base_url, results_base)
amplification_url = '{}{}'.format(base_url, amplification_base)
rnaplate_url      = '{}{}'.format(base_url, rnaplate_base)
rnawell_url       = '{}{}'.format(base_url, rnawell_base)
organization_url  = '{}{}'.format(base_url, organization_base)

###
### HTML REPORT
###

def html_digest(report, tb):

   # Color definition
   header_color = ' style="background-color:gainsboro;"'
   true_color   = ' style="background-color:rgb(212,239,223);"'
   false_color  = ' style="background-color:lightcoral;"'
   
   html = '<html><head><style>\nth, td { text-align: center; padding: 10px; }\ntable, th, td { border: 1px solid black; border-collapse: collapse;}\n</style></head><body>'
   html_index = '- <a href="#description">Job description</a><br>'

   # Header with ?index?
   html += '<h1>Project status report ({})</h1>\n'.format(datetime.datetime.now().strftime('%d/%m/%Y'))
   
   # PCR in progress
   html += '<br><h2>PCR runs in progress</h2>\n'
   html += '<table style="white-space:nowrap;"><tr>\
   <th{c}>RNA plate</th>\
   <td{c}>Date created</td>\
   <th{c}>PCR in LIMS</th>\
   <td{c}>SDS export</td>\
   <td{c}>LIMS upload</td>\
   <td{c}>PCR verified</td>\
   </tr>'.format(c=header_color)

   # Sort report by RNA date
   report = sorted(report, key = lambda x: x['created'])

   for rna in report:
      if len(rna['pcr']) == 0:
         html += '<tr>'
         html += '<td><b>{}</b></td><td>{}</td>'.format(rna['barcode'], rna['created'].replace('T', ' '))
         html += '<td{c}>❌</td><td{c}>❌</td><td{c}>❌</td><td{c}>❌</td>'.format(c=false_color)
         html += '</tr>'
      else:
         for pcr in rna['pcr']:
            if pcr['verified']:
               continue
            html += '<tr>'
            html += '<td><b>{}</b></td><td>{}</td>'.format(rna['barcode'], rna['created'].replace('T', ' '))
            html += '<td{}><b>{}</b></td><td{}>{}</td><td{}>{}</td><td{}>{}</td>'.format(
               true_color,
               pcr['barcode'],
               true_color if pcr['sdsfile'] else false_color,
               '✅' if pcr['sdsfile'] else '❌',
               true_color if pcr['uploaded'] else false_color,
               '✅' if pcr['uploaded'] else '❌',
               true_color if pcr['verified'] else false_color,
               '✅' if pcr['verified'] == 'OK' else '<b>Failed</b>' if pcr['verified'] == 'F' else '❌'
            )
            html += '</tr>'

   html += '</table>'


   # Diagnosis verification status
   html += '<br><h2>Diagnosis verification status</h2>\n'
   html += '<table style="white-space:nowrap;"><tr>\
   <th{c}>PCR barcode</th>\
   <th{c}>Project</th>\
   <td{c}>Organization</td>\
   <td{c}>Project samples</td>\
   <td{c}>PCR verified</td>\
   <td{c}>Sent for review</td>\
   <td{c}>Reviewed</td>\
   <td{c}>Results sent</td>\
   </tr>'.format(c=header_color)

   # Remove completed diagnosis
   for rna in report:
      for pcr in rna['pcr']:
         for proj in pcr['projects']:
            if proj['done'] or (pcr['verified'] and proj['sent'] == 'F'):
               continue

            html += '<tr>'
            html += '<td><b>{}</b></td><td>{}</td><td>{}</td><td>{}</td><td{}>{}</td><td{}>{}</td><td{}>{}</td><td{}>{}</td>'.format(
               pcr['barcode'],
               proj['name'],
               proj['org'],
               proj['samples'],
               true_color if pcr['verified'] else false_color,
               '✅' if pcr['verified'] == 'OK' else '<b>Failed</b>' if pcr['verified'] == 'F' else '❌',
               true_color if proj['sent'] == 'Y' else '' if proj['sent'] == 'F' else false_color,
               '✅' if proj['sent'] == 'Y' else '&nbsp;' if proj['sent'] == 'F' else '❌',
               true_color if proj['reviewed'] else '' if proj['sent'] == 'F' else false_color,
               '✅' if proj['reviewed'] else '' if proj['sent'] == 'F' else '❌',
               true_color if proj['done'] else '' if proj['sent'] == 'F' else false_color,
               '✅' if proj['done'] else '' if proj['sent'] == 'F' else '❌'
            )

            html += '</tr>'
   html += '</table>'
   
   html += "</body></html>"

   return MIMEText(html, 'html')

###
### EMAIL NOTIFICATIONS
### 

smtp_server     = "smtp.gmail.com"
email_port      = 465
email_receivers = EMAIL_RECEIVERS.split(',')

def send_digest(digest, tb=None):
   message = MIMEMultipart()
   message['From']    = 'PRBB LIMS <{}>'.format(EMAIL_SENDER)
   message['Subject'] = "Project status report ({})".format(datetime.datetime.now().strftime('%d/%m/%Y'))
   message['Bcc']     = ','.join(email_receivers)
   message.attach(html_digest(digest, tb))
   
   context = ssl.create_default_context()
   with smtplib.SMTP_SSL(smtp_server, email_port, context=context) as server:
      server.login(EMAIL_SENDER, EMAIL_PASSWORD)
      server.sendmail(EMAIL_SENDER, email_receivers, message.as_string())

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
### LOGGING
###

def setup_logger(log_path):
   log_level = logging.INFO
   log_file = '{}/{}.log'.format(log_path, job_name)
   log_format = '[%(asctime)s][%(levelname)s]%(message)s'

   logging.basicConfig(level=log_level, filename=log_file, format=log_format)

   return log_file


###
### ARGUMENTS
###

def getOptions(args=sys.argv[1:]):
   parser = argparse.ArgumentParser('lims_sync')
   parser.add_argument('path', help='Input folder (where the *_results.txt and *_clipped.txt files are)')
   parser.add_argument('-l', '--logpath', help='Root folder to store logs', required=True)
   options = parser.parse_args(args)
   return options


###
### MAIN SCRIPT
###

if __name__ == '__main__':
   
   # Parse arguments
   options = getOptions(sys.argv[1:])
   path    = options.path

   # Set up logger
   logpath = setup_logger(options.logpath).replace('//','/')

   # Get rna plates
   r, status = lims_request('GET', rnaplate_url, params={'limit': 10000})
   assert_critical(status < 300, 'Could not retreive rna plates from LIMS')
   rnaplates = r.json()['objects']
   rnaplates = {o['barcode']: o for o in rnaplates}

   # Get pcr plates
   r, status = lims_request('GET', pcrplate_url, params={'limit': 10000})
   assert_critical(status < 300, 'Could not retreive pcr plates from LIMS')
   pcrplates = r.json()['objects']
   pcrplates = {o['barcode']: o for o in pcrplates}

   # Get pcr plate projects
   r, status = lims_request('GET', pcrproject_url, params={'limit': 10000})
   assert_critical(status < 300, 'Could not retreive pcr plate projects from LIMS')
   pcrprojects = r.json()['objects']

   # Get pcr runs
   r, status = lims_request('GET', pcrrun_url, params={'limit': 10000})
   assert_critical(status < 300, 'Could not retreive pcr runs from LIMS')
   pcrruns = r.json()['objects']
   pcrruns = {o['pcr_plate']: o for o in pcrruns}

   # Get projects
   r, status = lims_request('GET', project_url, params={'limit': 10000})
   assert_critical(status < 300, 'Could not retreive projects from LIMS')
   projects = r.json()['objects']
   projects = {o['resource_uri']: o for o in projects if not o['name'] in ['CONTROLS', 'SERRANO_HOSPITAL', 'TESTS']}

   # Get projects
   r, status = lims_request('GET', organization_url, params={'limit': 10000})
   assert_critical(status < 300, 'Could not retreive organizations from LIMS')
   orgs = r.json()['objects']
   orgs = {o['resource_uri']: o for o in orgs}

   # Find all processed samples in path
   flist = glob.glob('{}/*_results.txt'.format(path))
   export_files = [fname.split('/')[-1].split('_results.txt')[0] for fname in flist]

   # Merge information
   report = []
   for rnabcd in rnaplates:
      info = {
         'barcode': rnabcd,
         'created': rnaplates[rnabcd]['date_prepared'],
      }

      pcrs = []
      for pcrbcd in pcrplates:
         # Check if pcr plates exist for this rna plate
         if rnabcd in pcrbcd:
            pcrinfo = {
               'barcode': pcrplates[pcrbcd]['barcode'],
               'sdsfile': pcrbcd in export_files         # Check if files were exported from SDS
            }
            uri = pcrplates[pcrbcd]['resource_uri']

            # Run info
            if uri in pcrruns:
               pcrinfo['uploaded'] = True
               pcrinfo['verified'] = pcrruns[uri]['status'] if (pcrruns[uri]['status'] == 'OK' or pcrruns[uri]['status'] == 'F') else False
            else:
               pcrinfo['uploaded'] = False
               pcrinfo['verified'] = False

            # Project info
            pcrprojinfo = []
            for proj in pcrprojects:
               if proj['pcr_plate'] == uri:
                  projinfo = {}
                  if not proj['project'] in projects:
                     continue
                  p = projects[proj['project']]
                  projinfo['name'] = p['name'] if p else 'UNKNOWN'
                  projinfo['org']  = orgs[p['organization']]['name'] if p['organization'] in orgs else 'UNKNOWN'
                  projinfo['sent'] = proj['results_sent'] # N: Not sent, Y: Sent, F: Never Send
                  projinfo['reviewed'] = proj['diagnosis_completed'] # 0: Not sent, 1; Sent
                  projinfo['done'] = proj['diagnosis_sent'] if projinfo['name'] == 'ORFEU' else projinfo['reviewed'] # 0: Not sent, 1: Sent
                  if not (proj['diagnosis_sent'] or proj['results_sent'] == 'F'):
                     r, status = lims_request('GET', rnawell_url, params= {'limit': 10000, 'sample__project__name__exact': p['name'], 'rna_extraction_plate__barcode__exact': rnabcd})
                     assert_error(status < 300, 'Could not retreive sample count for project {}/ pcrplate {} from LIMS'.format(p['name'], pcrbcd))
                     projinfo['samples'] = r.json()['meta']['total_count']
                  else:
                     projinfo['samples'] = 'NA'

                  if projinfo['samples'] != 0:
                     pcrprojinfo.append(projinfo)
                     
            pcrinfo['projects'] = pcrprojinfo

            pcrs.append(pcrinfo)
      info['pcr'] = pcrs
      report.append(info)

   # Send status report
   send_digest(report)
