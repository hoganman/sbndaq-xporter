"""
Usage: python Xporter.py <data directory> <dropbox directory>

program to:
       1)  Check to see if there are new files and if there are:
       2) update run configuration database
       3) create the SAM metadata file
       4) move the data file to the dropbox
"""

# import modules
import glob
import os
import shutil
import sys

import X_SAM_metadata
import filelock


# import psycopg2 # Get database functions

def print_usage():
    """ print Xporter script usage and exit """
    print('Command: python Xporter.py <data directory> <dropbox directory> <"dev"/"prod"/"none">')
    sys.exit(1)


def parse_dir(dirname):
    """
    parse directory names, check if there
        if so, return updated/parsed name
        if not, return empty string
    """
    try:
        os.chdir(dirname)
        if dirname[len(dirname) - 1] != "/":
            dirname = dirname + "/"
        return dirname
    except OSError as err:
        print("Directory: ", dirname, "not found")
        print('Message: \"%s\"' % err.message)
        return ""


def parse_cmdline_inputs(args):
    # parse commandline inputs
    """MHogan: If this needs to be updated, I would recommend using the optparse module instead
    """
    if len(args) != 3 and len(args) != 4:
        print_usage()

    global datadir
    datadir = parse_dir(sys.argv[1])
    if datadir == "":
        sys.exit(1)

    global dropboxdir
    dropboxdir = parse_dir(sys.argv[2])
    if dropboxdir == "":
        sys.exit(1)

    global runconfigdb
    runconfigdb = ""
    if (len(sys.argv) == 4 and sys.argv[3] == "none") or len(sys.argv) == 3:
        runconfigdb = "none"
    elif sys.argv[3] == "dev":
        runconfigdb = "dev"
    elif sys.argv[3] == "prod":
        runconfigdb = "prod"
    else:
        print_usage()

    return datadir, dropboxdir, runconfigdb


def connect_to_runconfigdb(dbname):
    """
    connect to runconfig database. return 0 if ok
    """
    if dbname == "none":
        print "Not connecting to a RunConfigDB"
        return 0

    elif dbname == "dev":
        print "Connecting to development RunConfigDB..."
        # dbvariables.conn = psycopg2.connect(database="lariat_dev", user="randy", host="ifdbdev", port="5441")
        # dbvariables.cur= dbvariables.conn.cursor()
        return 0

    elif dbname == "prod":
        print "Connecting to production RunConfigDB..."

        # ntry = 0
        # nodbconnection = True
        # while nodbconnection:
        # try:
        # dbvariables.conn = psycopg2.connect(database="lariat_prd",
        #                                     user="lariatdataxport",
        #                                     password="lariatdataxport_321",
        #                                     host="ifdbprod2", port="5443")
        # dbvariables.cur=dbvariables.conn.cursor()
        # nodbconnection = False
        # except:
        # ntry +=1
        # if (ntry % 5 == 1): print "Failed to make lariat_prd connection for",ntry,"times... sleep for 5 minutes"
        # time.sleep(300)
        return 0

    else:
        print "Unknown RunConfigDB name: %s" % dbname
        return -1


def obtain_lock(lockname, timeout=5, retries=2):
    flock = filelock.FileLock(lockname + "FileLock")
    ntry = 0
    while ntry <= retries:
        try:
            flock.acquire(timeout=timeout)
            break
        except filelock.Timeout as err:
            print("Could not obtain file lock. Exiting.")
            print('Message: \"%s\"' % err.message)
        ntry += 1

    if ntry > retries:
        print("Never obtained lock %s after %d tries" % (lockname, ntry))
        sys.exit(1)

    return flock


def get_finished_files(dirname, file_pattern="*.root"):
    return glob.glob(dirname + "/" + file_pattern)


def move_files(filenames, destdir, move_file):
    # move files, and return the number moved
    moved_files = 0
    for filename in filenames:
        fname = filename.split("/")[-1]
        print("Will move/copy %s to %s" % (filename, destdir + fname))

        if len(glob.glob(dropboxdir + fname)) > 0:
            print("File %s already in %s" % (fname, destdir))
            continue

        if not move_file:
            shutil.copy(filename, destdir + fname)
        else:
            shutil.move(filename, destdir + fname)
        moved_files += 1

    return moved_files


def write_metadata_files(filenames):
    n_json_written = 0
    for filename in filenames:
        metadata_fname = filename + ".json"
        if len(glob.glob(metadata_fname)) > 0:
            print "JSON file for %s already exists." % filename
            continue

        metadata_json = X_SAM_metadata.SAM_metadata(filename)
        print metadata_json

        print metadata_fname
        with open(metadata_fname, "w") as outfile:
            outfile.write(metadata_json)

        n_json_written += 1
    return n_json_written


# Get directory of Xporter.py
Xporterdir = os.path.dirname(os.path.abspath(__file__))

# parse commandline inputs
datadir, dropboxdir, runconfigdb = parse_cmdline_inputs(sys.argv)

print("Data dir=%s, Dropbox dir=%s, RunConfigDB=%s" % (datadir, dropboxdir, runconfigdb))

# connect to runconfigdb
if connect_to_runconfigdb(runconfigdb) != 0:
    print("Connecting to RunConfigDB %s failed." % runconfigdb)
    sys.exit(1)

#  check for file lock
lock = obtain_lock(datadir + "XporterInProgress")

# get list of finished files
files = get_finished_files(datadir, "data_dl*_run*.root")

print "Found %d files in data dir" % len(files)
for f in files:
    print "\t%s" % f.split("/")[-1]

# for each file, move/copy it to the dropbox
moveFile = False
n_moved_files = move_files(files, dropboxdir, move_file=moveFile)
print "Moved %d / %d files" % (n_moved_files, len(files))

dropbox_files = get_finished_files(dropboxdir, "data_dl*_run*.root")
print "Found %d files in dropbox" % len(dropbox_files)
for f in files:
    print "\t%s" % f.split("/")[-1]

n_json_files_written = write_metadata_files(dropbox_files)
print "Wrote %d / %d metadata files" % (n_json_files_written, len(dropbox_files))

# exit
lock.release()
