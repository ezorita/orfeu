import sys, os, stat, datetime
import argparse
import shutil
import hashlib

job_name  = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
job_start = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')


###
### ARGUMENTS
###

def getOptions(args=sys.argv[1:]):
   parser = argparse.ArgumentParser('sync_folder')
   parser.add_argument('source', help='source folder, where information will be taken from')
   parser.add_argument('dest', help='destination folder, where info will be copied to')
   parser.add_argument('-f', '--force', help='overwrite existing files with different hash')
   options = parser.parse_args(args)
   return options


###
### FILE HASH
###

def md5(fname):
   hash_md5 = hashlib.md5()
   with open(fname, "rb") as f:
      for chunk in iter(lambda: f.read(4096), b""):
         hash_md5.update(chunk)
   return hash_md5.hexdigest()

 
###
### MAIN SCRIPT
###

if __name__ == '__main__':
   
   # Parse arguments
   options = getOptions(sys.argv[1:])
   source  = options.source
   dest    = options.dest

   src_files = [f for f in os.listdir(source) if os.path.isfile(os.path.join(source,f))]
   dst_files = [f for f in os.listdir(dest)   if os.path.isfile(os.path.join(dest,f))]

   for fname in src_files:
      print("[{}] begin processing".format(os.path.join(source,fname)))
      if fname in dst_files:
         print("[{}] file already exists in {}".format(fname, dest))
         # Check if it is the same file
         s_hash = md5(os.path.join(source,fname))
         d_hash = md5(os.path.join(dest,fname))

         if s_hash != d_hash:
            print("[{}] dest file is different, renaming {} to {}".format(fname, os.path.join(dest,fname), os.path.join(dest,fname)+'.'+d_hash))
            # If file already exists, append .[filehash] at the end of the current file name
            shutil.move(os.path.join(dest,fname), os.path.join(dest,fname)+'.'+d_hash)
         else:
            print("[{}] files are equal. md5: {}".format(fname, s_hash))
            # File is in sync: done
            continue

      # Now copy the source file
      print("[{}] copy {} to {}".format(fname, os.path.join(source,fname), os.path.join(dest,fname)))
      shutil.copy(os.path.join(source,fname), os.path.join(dest,fname))
      print("[{}] chown".format(fname))
      os.chmod(os.path.join(dest,fname), 0o644)
      print("[{}] end processing".format(fname))
      
