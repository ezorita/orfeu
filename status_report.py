import sys, os, glob
import requests
import logging
import datetime
import argparse
import traceback
import smtplib, ssl
import pandas as pd
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
sample_base        = '{}sample/'.format(api_root)
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
sample_url        = '{}{}'.format(base_url, sample_base)
detector_url      = '{}{}'.format(base_url, detector_base)
results_url       = '{}{}'.format(base_url, results_base)
amplification_url = '{}{}'.format(base_url, amplification_base)
rnaplate_url      = '{}{}'.format(base_url, rnaplate_base)
rnawell_url       = '{}{}'.format(base_url, rnawell_base)
organization_url  = '{}{}'.format(base_url, organization_base)

###
### HTML REPORT
###

def html_digest(report, stats, tb):

   # Color definition
   header_color = ' style="background-color:gainsboro;"'
   count_color  = ' style="background-color:ivory;"'
   true_color   = ' style="background-color:rgb(212,239,223);"'
   false_color  = ' style="background-color:lightcoral;"'
   
   html = '<html><head><style>\nth, td { text-align: center; padding: 10px; }\ntable, th, td { border: 1px solid black; border-collapse: collapse;}\n</style></head><body>'

   # Header
   html += '<h1>Project status report ({})</h1>\n'.format(datetime.datetime.now().strftime('%d/%m/%Y %H:%M'))

   # Sample stats
   html += '<br><h2>Sample stats</h2>\n'
   html += '<table style="white-space:nowrap;"><tr>\
   <th{c} rowspan="2">Project</th>\
   <th{c} rowspan="2">In RNA plate</th>\
   <th{c} rowspan="2">In PCR plate</th>\
   <th{c} rowspan="2">Awaiting PCR verif.</th>\
   <th{c} colspan="3">PCR Verified</th>\
   <th{c} rowspan="2">Sent - Awaiting review</th>\
   <th{c} rowspan="2">Reviewed</th>\
   <th{c} rowspan="2">Sent to CTTI/ICS</th>\
   </tr><tr>\
   <th{c}>Failed</th>\
   <th{c}>On Hold</th>\
   <th{c}>Success</th>\
   </tr>'.format(c=header_color)

   list_failed = False
   list_onhold = False
   
   for proj in stats['project'].unique():
      if (sample_stats['project'] == proj).sum() == 0:
         continue
      html += '<tr>'
      html += '<td><b>{}</b></td>'.format(proj)
      html += '<td>{}</td>'.format(((sample_stats['project'] == proj) & (sample_stats['status'] == 'RNA')).sum())
      html += '<td>{}</td>'.format(((sample_stats['project'] == proj) & (sample_stats['status'] == 'PCR')).sum())
      html += '<td>{}</td>'.format(((sample_stats['project'] == proj) & (sample_stats['status'] == 'RUNNING')).sum())
      failed_cnt = ((sample_stats['project'] == proj) & (sample_stats['status'] == 'FAILED')).sum()
      list_failed = True if failed_cnt else list_failed
      html += '<td>{}</td>'.format(failed_cnt if failed_cnt == 0 else '<a href="#failed{}">{}</a>'.format(proj,failed_cnt))
      hold_cnt = ((sample_stats['project'] == proj) & (sample_stats['status'] == 'HOLD')).sum()
      list_onhold = True if hold_cnt else list_onhold
      html += '<td>{}</td>'.format(hold_cnt if hold_cnt == 0 else '<a href="#hold{}">{}</a>'.format(proj, hold_cnt))
      html += '<td>{}</td>'.format(((sample_stats['project'] == proj) & (sample_stats['status'] == 'VERIFIED')).sum())
      html += '<td>{}</td>'.format(((sample_stats['project'] == proj) & (sample_stats['status'] == 'SENT')).sum())
      html += '<td>{}</td>'.format(((sample_stats['project'] == proj) & (sample_stats['status'] == 'REVIEWED')).sum())
      html += '<td{}><b>{}</b></td>'.format(count_color, ((sample_stats['project'] == proj) & (sample_stats['status'] == 'DONE')).sum())
      html += '</tr>'
      
   html += '</table>'
   
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
            if pcr['verified'] in ['OK', 'F']:
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
               true_color if pcr['verified'] == 'OK' else false_color,
               '✅' if pcr['verified'] == 'OK' else '<b>Failed</b>' if pcr['verified'] == 'F' else '<b>On Hold</b>' if pcr['verified'] == 'H' else '❌'
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
            if proj['done'] or ((pcr['verified'] in ['OK', 'F']) and proj['sent'] == 'F'):
               continue

            html += '<tr>'
            html += '<td><b>{}</b></td><td>{}</td><td>{}</td><td>{}</td><td{}>{}</td><td{}>{}</td><td{}>{}</td><td{}>{}</td>'.format(
               pcr['barcode'],
               proj['name'],
               proj['org'],
               proj['samples'],
               true_color if pcr['verified'] == 'OK' else false_color,
               '✅' if pcr['verified'] == 'OK' else '<b>Failed</b>' if pcr['verified'] == 'F' else '<b>On Hold</b>' if pcr['verified'] == 'H' else '❌',
               true_color if proj['sent'] == 'Y' else '' if proj['sent'] == 'F' else false_color,
               '✅' if proj['sent'] == 'Y' else '&nbsp;' if proj['sent'] == 'F' else '❌',
               true_color if proj['reviewed'] else '' if proj['sent'] == 'F' else false_color,
               '✅' if proj['reviewed'] else '' if proj['sent'] == 'F' else '❌',
               true_color if proj['done'] else '' if proj['sent'] == 'F' else false_color,
               '✅' if proj['done'] else '' if proj['sent'] == 'F' else '❌'
            )

            html += '</tr>'
   html += '</table>'

   # List of samples
   if list_failed or list_onhold:
      html += '<br><h2>List of potentially delayed samples</h2>\n'
      if list_failed:
         html += '<h3>Samples on <b>failed</b> PCRs</h3>\n'
         
         for proj in stats['project'].unique():
            failed_samples = sample_stats[((sample_stats['project'] == proj) & (sample_stats['status'] == 'FAILED'))]
            if failed_samples.shape[0]:
               failed_samples = failed_samples.sort_values('pcrplate')
               html += '<h4><a name="failed{}"></a>Samples on Failed PCR (Project: {}, samples: {})</h4>\n'.format(proj,proj,failed_samples.shape[0])
               html += '<p style="font-family:\'Courier New\'">'
               for s in failed_samples.iterrows():
                  html += '{}\t{}<br>'.format(s[1]['pcrplate'], s[1]['sample'])
               html += '</p>'

      if list_onhold:
         html += '<br><h3>Samples <b>on hold</b> in PCRs</h3>\n'
         
         for proj in stats['project'].unique():
            hold_samples = sample_stats[((sample_stats['project'] == proj) & (sample_stats['status'] == 'HOLD'))]
            if hold_samples.shape[0]:
               hold_samples = hold_samples.sort_values('pcrplate')
               html += '<h4><a name="hold{}"></a>Samples in PCR on hold (Project: {}, samples: {})</h4>\n'.format(proj,proj,hold_samples.shape[0])
               html += '<p style="font-family:\'Courier New\'">'
               for s in hold_samples.iterrows():
                  html += '{}\t{}<br>'.format(s[1]['pcrplate'], s[1]['sample'])
               html += '</p>'

   html += "</body></html>"

   return MIMEText(html, 'html')

###
### EMAIL NOTIFICATIONS
### 

smtp_server     = "smtp.gmail.com"
email_port      = 465
email_receivers = EMAIL_RECEIVERS.split(',')

def send_digest(digest, stats, tb=None):
   message = MIMEMultipart()
   message['From']    = 'PRBB LIMS <{}>'.format(EMAIL_SENDER)
   message['Subject'] = "Project status report ({})".format(datetime.datetime.now().strftime('%d/%m/%Y'))
   message['Bcc']     = ','.join(email_receivers)
   message.attach(html_digest(digest, stats, tb))
   
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
### SAMPLE STATUS FILTER
###

def sample_status(group):
   status = 'RNA'
   if group['diagnosis_sent'].any():
      status = 'DONE'
   elif group['diagnosis_completed'].any():
      status = 'REVIEWED'
   elif (group['results_sent'] == 'Y').any():
      status = 'SENT'
   elif (group['status_y'] == 'OK').any():
      status = 'VERIFIED'
   elif (group['status_y'] == 'R').any():
      status = 'RUNNING'
   elif (group['status_y'] == 'H').any():
      status = 'HOLD'
   elif (group['status_y'] == 'F').any():
      status = 'FAILED'
   elif group['pcr_plate'].any():
      status = 'PCR'

   return pd.DataFrame({'sample': [group['sample_bcd'].iloc[0]], 'project': [group['project'].iloc[0]], 'pcrplate': [group['pcr_plate'].iloc[0]], 'status': [status]})


###
### MAIN SCRIPT
###

if __name__ == '__main__':
   
   # Parse arguments
   options = getOptions(sys.argv[1:])
   path    = options.path

   # Set up logger
   logpath = setup_logger(options.logpath).replace('//','/')

   
   ##
   ## OVERALL PROJECT STATUS
   ##

   # Sample info
   # next_url = sample_base 
   # samples = [] 
   # while next_url: 
   #    r, status = lims_request('GET', base_url+next_url, params={'limit': 10000}) 
   #    if not assert_critical(status < 300, 'Could not retreive samples from LIMS'):
   #       break
   #    samples.extend(r.json()['objects']) 
   #    next_url = r.json()['meta']['next']

   # Rna wells
   next_url = rnawell_base 
   rnawells = [] 
   while next_url: 
      r, status = lims_request('GET', base_url+next_url, params={'limit': 1000}) 
      assert_critical(status < 300, 'Could not retreive rna wells from LIMS')
      rnawells.extend(r.json()['objects']) 
      next_url = r.json()['meta']['next']
   
   # Pcr wells
   next_url = pcrwell_base
   pcrwells = [] 
   while next_url: 
      r, status = lims_request('GET', base_url+next_url, params={'limit': 1000}) 
      assert_critical(status < 300, 'Could not retreive pcr wells from LIMS')
      pcrwells.extend(r.json()['objects']) 
      next_url = r.json()['meta']['next']

   # Get pcr plate projects
   next_url = pcrproject_base
   pcrprojects = []
   while next_url:
      r, status = lims_request('GET', base_url+next_url, params={'limit': 1000})
      assert_critical(status < 300, 'Could not retreive pcr plate projects from LIMS')
      pcrprojects.extend(r.json()['objects'])
      next_url = r.json()['meta']['next']

   # Get pcr runs
   next_url = pcrrun_base
   pcrruns_data = []
   while next_url:
      r, status = lims_request('GET', base_url+next_url, params={'limit': 1000})
      assert_critical(status < 300, 'Could not retreive pcr runs from LIMS')
      pcrruns_data.extend(r.json()['objects'])
      next_url = r.json()['meta']['next']
      
   pcrruns = {o['pcr_plate']: o for o in pcrruns_data}

   # Get pcr plates
   next_url  = pcrplate_base
   pcrplates = []
   while next_url:
      r, status = lims_request('GET', base_url+next_url, params={'limit': 1000})
      assert_critical(status < 300, 'Could not retreive pcr plates from LIMS')
      pcrplates.extend(r.json()['objects'])
      next_url = r.json()['meta']['next']
      
   pcrplate_bcd = {o['resource_uri']: o['barcode'] for o in pcrplates}
   pcrplates = {o['barcode']: o for o in pcrplates}


   # Get projects
   r, status = lims_request('GET', project_url, params={'limit': 1000})
   assert_critical(status < 300, 'Could not retreive projects from LIMS')
   projects = r.json()['objects']
   projects = {o['resource_uri']: o for o in projects if not o['name'] in ['CONTROLS', 'SERRANO_HOSPITAL', 'TESTS']}
   

   # Create data frames
#   samples  = pd.DataFrame(samples)
   dfrnawells = pd.DataFrame(rnawells)
   dfpcrwells = pd.DataFrame(pcrwells)
   dfpcrprojs = pd.DataFrame(pcrprojects)
   dfpcrruns  = pd.DataFrame(pcrruns_data)

   dfrnawells['project']    = dfrnawells['sample'].apply(lambda x: x['project'])
   dfrnawells['sample_bcd'] = dfrnawells['sample'].apply(lambda x: x['barcode'])
   
   # Merge tables
   data = dfrnawells.merge(dfpcrwells, how='left', left_on='resource_uri', right_on='rna_extraction_well')
   data = data.merge(dfpcrprojs, how='left', on=['pcr_plate', 'project'])
   data = data.merge(dfpcrruns, how='left', on='pcr_plate')
   
   data['project'] = data['project'].apply(lambda x: projects[x]['name'] if x in projects else None)
   data['pcr_plate'] = data['pcr_plate'].apply(lambda x: pcrplate_bcd[x] if x in pcrplate_bcd else None)

   sample_stats = data.groupby(by='sample_bcd').apply(sample_status)
   
   ##
   ## PCR STATUS INFO
   ##

   # Rna plates
   next_url = rnaplate_base 
   rnaplates = [] 
   while next_url: 
      r, status = lims_request('GET', base_url+next_url, params={'limit': 1000}) 
      assert_critical(status < 300, 'Could not retreive rna plates from LIMS')
      rnaplates.extend(r.json()['objects']) 
      next_url = r.json()['meta']['next']
   rnaplates = {o['barcode']: o for o in rnaplates}   


   # Get organizations
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
               pcrinfo['verified'] = pcrruns[uri]['status']
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
   send_digest(report, sample_stats)
