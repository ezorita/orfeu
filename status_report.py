import sys, os, glob
import requests
import logging
import datetime
import argparse
import traceback
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
pcrplate_base      = '{}pcrplate/'.format(api_root)
pcrrun_base        = '{}pcrrun/'.format(api_root)
pcrwell_base       = '{}pcrwell/'.format(api_root)
detector_base      = '{}detector/'.format(api_root)
results_base       = '{}results/'.format(api_root)p
amplification_base = '{}amplificationdata/'.format(api_root)
rnaplate_base      = '{}rnaextractionplate/'.format(api_root)
organization_base  = '{}organization/'.format(api_root)

pcrplate_url      = '{}{}'.format(base_url, pcrplate_base)
pcrwell_url       = '{}{}'.format(base_url, pcrwell_base)
pcrrun_url        = '{}{}'.format(base_url, pcrrun_base)
detector_url      = '{}{}'.format(base_url, detector_base)
results_url       = '{}{}'.format(base_url, results_base)
amplification_url = '{}{}'.format(base_url, amplification_base)
rnaplate_url      = '{}{}'.format(base_url, rnaplate_base)
organization_url  = '{}{}'.format(base_url, organization_base)

###
### HTML REPORT
###

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

   # ...


###
### EMAIL NOTIFICATIONS
### 

smtp_server     = "smtp.gmail.com"
email_port      = 465
email_receivers = EMAIL_RECEIVERS.split(',')

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
   r, status = lims_request
   
