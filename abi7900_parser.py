import os, io, re, glob
import pandas as pd

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

def join_results(data):
   # Need to merge results here or introduce directly to LIMS?
   # Where will the decision be made?
   pass


def rename_Ct(x):
   return 'NA' if x in ['Unknown','Undetermined'] else x
   

if __name__ == '__main__':
   # Expecting to parse sample name from filename
   flist = glob.glob('*_results.txt')
   for fname in flist:
      # TODO: Check whether sample has been already processed
      platebc = fname.split('_results.txt')[0]
      results = parse_results(fname)
      rn, drn = parse_rn(platebc+'_clipped.txt')

      # Format and store Results
      results['Ct'] = results['Ct'].apply(rename_Ct)
      results['Plate_barcode'] = platebc
      results[['Well','Plate_barcode','Ct']].to_csv('{}_out.tsv'.format(platebc), sep='\t', index=False)

      # Format and store Rn and Delta_Rn
      rn['bcd'] = platebc
      rnlist = rn.melt(id_vars=['bcd', 'well','rep'], var_name='cycle', value_name='Rn')
      rnlist['cycle'] = rnlist['cycle'].astype(int)
      rnlist.columns = ['Plate_Barcode', 'Well', 'Reporter', 'Cycle', 'Rn']
      rnlist = rnlist.sort_values(by=['Well','Cycle'])
            
      rnlist.to_csv('{}_rn.tsv'.format(platebc), sep='\t', index=False)
