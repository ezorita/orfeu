import requests
import glob, sys,os

# TODO: How to handle unfinished syncs (i.e. the connection broke during sync) --> verify script
#

# TODO:
# - Parse and sync from the same script.
# - Write all newly created elements in a file (at least their id), for tracking what needs to be deleted from lims in case the whole thing fails.
# - Write output files at the end of syncing


base_url = 'https://orfeu.cnag.crg.eu/prbblims_devel/api/covid19'
pcrplate_url = '{}/pcrplate'


'''
LOGGING / ERROR CONTROL
'''

def assert_error(cond, msg):
   if not cond:
      # LOG MSG
      sys.exit(1)

def assert_warning(cond, msg):
   if not cond:
      # LOG MSG
      # NOTIFY

'''
LIMS METHODS
'''

# Get environment variables
LIMS_USER = os.environ.get('LIMS_USER')
LIMS_PASSWORD = os.environ.get('LIMS_PASSWORD')
assert_error(LIMS_USER is not None and LIMS_PASSWORD is not None, 'LIMS API environment variables not set (set env vars LIMS_USER, LIMS_PASSWORD)')
  

def lims_request(method, url, params=None, data=None, header=req_header):
   # methods: GET, OPTIONS, HEAD, POST, PUT, PATCH, DELETE
   r = requests.request(method, url, params=params, header=header, data=data, verify=False)
   assert_warning(r.status_code > 299,
                  'LIMS request returned non-successful response ({}). Request details: URL={}, PARAMS={}, DATA={}'.format(
                     r.status_code,
                     url,
                     params,
                     data
                  ))
   return r.json(), r.status_code


'''
DATA PARSING METHODS
'''

def parse_rn(clipped_file):
   data = pd.read_csv(clipped_file, sep='\t', skiprows=1, header=0, index_col=False)
   info = pd.DataFrame({'well': data.iloc[:,0], 'rep': data.iloc[:,1]})
   
   # Split Rn and Delta_Rn in two DataFrames
   rn  = pd.concat([info , data.iloc[:,data.columns.get_loc('Rn')+1:data.columns.get_loc('Delta Rn')]], axis=1)
   drn = pd.concat([info, data.iloc[:,data.columns.get_loc('Delta Rn')+1:]], axis=1)
   return rn, drn

def parse_results(results_file):
   with open(results_file) as f_in:
      # Catch header
      for line in f_in:
         if line[0:4] == 'Well':
            colnames = line.split('\t')
            break
      # Regex data rows
      rows = [line for line in f_in if re.match(r'^[0-9]', line)]

   # Create DataFrame
   data = pd.read_csv(io.StringIO('\n'.join(rows)), delim_whitespace=True, names=colnames)
   
   return data

def rename_Ct(x):
   return 'NA' if x in ['Unknown','Undetermined'] else x


'''
MAIN SCRIPT
'''

if __name__ == '__main__':

   # Arguments (TODO: use library for argument parsing)
   if len(sys.argv) != 2:
      print("usage: {} root_path".format(sys.argv[0]))
      sys.exit(1)
   
   # Construct header
   req_header = {'content-type': 'application/json', 'Authorization': 'ApiKey {}:{}'.format(LIMS_USER, LIMS_PASSWORD) };

   path = sys.argv[1]

   # Test LIMS connection
   _, status = lims_request('GET', base_url)
   assert_critical(status != 200, 'Test connection to LIMS API failed')

   # Get list of pcr plates
   # Need a wrapper to check that the request worked
   pcrplates, _ = lims_request('GET', url=pcrplate_url, headers=req_header)
   
   # Find all processed samples in path
   flist = glob.glob('*_results.txt')
   for fname in flist:
      platebc = fname.split('_out.tsv')[0]
      
      # Check if sample is already in LIMS
      if platebc in pcrplates:
         # LOG (DEBUG)
         continue

      # LIMS cache
      lcache = []
      
      # Push new PCR plate to LIMS
      res = lims_request('POST', pcrplate_url, data={'barcode'=platebc, 'plate_name':platebc})
      lcache.append(res.json())

      # Parse results
      results = parse_results(fname)
      rn, drn = parse_rn(platebc+'_clipped.txt')

      # Format results
      results['ct'] = results['ct'].apply(rename_Ct)
      results['plate_barcode'] = platebc
      
      # TODO: Is is possible to introduce multiple entries to LIMS with ONE request? (Is it even possible to introduce anything? lol)
      # Need to link PCR well with RNA well (which is the right correspondence of RNA -> qPCR well?)

      # Store Rn values, linked to each result entry

      # Flag somehow that the sample has been synced

      # Store parsing output
      rn['bcd'] = platebc
      rnlist = rn.melt(id_vars=['bcd', 'well','rep'], var_name='cycle', value_name='Rn')
      rnlist['cycle'] = rnlist['cycle'].astype(int)
      rnlist.columns = ['Plate_Barcode', 'Well', 'Reporter', 'Cycle', 'Rn']
      rnlist = rnlist.sort_values(by=['Well','Cycle'])
      results[['Well','Plate_barcode','Ct']].to_csv('{}_out.tsv'.format(platebc), sep='\t', index=False)
      rnlist.to_csv('{}_rn.tsv'.format(platebc), sep='\t', index=False)
