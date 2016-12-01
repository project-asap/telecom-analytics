import os
from sys import stderr

def exec_command(command):
    """
    Execute the command and return the exit status.
    """
    import subprocess
    from subprocess import PIPE
    pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)

    stdo, stde = pobj.communicate()
    exit_code = pobj.returncode

    return exit_code, stdo, stde
def exists(hdfs_url):
    """
    Test if the url exists.
    """
    return test(hdfs_url, test='e')
 
def test(hdfs_url, test='e'):
    """
    Test the url.
        
    parameters
    ----------
    hdfs_url: string
        The hdfs url
    test: string, optional
        'e': existence
        'd': is a directory
        'z': zero length

        Default is an existence test, 'e'
    """
    command="""hadoop fs -test -%s %s""" % (test, hdfs_url)

    exit_code, stdo, stde = exec_command(command)

    if exit_code != 0:
        return False
    else:
        return True


def stat(hdfs_url):
    """
    stat the hdfs URL, return None if does not exist.

    Returns a dictionary with keys
        filename: base name of file
        blocks: number of blocks
        block_size: size of each block
        mod_date: last modification
        replication: number of copies in hdfs
    """

    command="""hadoop fs -stat "{'blocks': %b, 'mod_date': '%y', 'replication': %r, 'filename':'%n'}" """
    command += hdfs_url

    exit_code, stdo, stde = exec_command(command)

    if exit_code != 0:
        return None
    else:
        return eval(stdo.strip())


def ls(hdfs_url='', recurse=False):
    """
    List the hdfs URL.  If the URL is a directory, the contents are returned.
    """
    import subprocess
    from subprocess import PIPE
    if recurse:
        cmd='lsr'
    else:
        cmd='ls'

    command = "$HADOOP_HOME/bin/hadoop fs -%s %s" % (cmd,hdfs_url)

    exit_code, stdo, stde = exec_command(command)
    if exit_code != 0:
        raise ValueError("command failed with code %s: %s" % (exit_code,command))

    flist = []
    lines = stdo.split('\n')
    for line in lines:
        ls = line.split()
        if len(ls) == 8:
            # this is a file description line
            fname=ls[-1]
            flist.append(fname)

    return flist

    """
    proc = subprocess.Popen(command, stdout=PIPE, shell=True)

    flist=[]
    while True:
        line = proc.stdout.readline()
        if line != '':
            ls = line.split()
            if len(ls) == 8:
                # this is a file description line
                fname=ls[-1]
                flist.append(fname)
        else:
            break
    print flist
    exit_code = proc.returncode
    if exit_code != 0:
        raise ValueError("command failed with code %s: %s" % (exit_code,command))
    print flist
    """


def lsr(hdfs_url=''):
    """
    Recursively List the hdfs URL.  This is equivalent to hdfs.ls(url, recurse=True)
    """
    ls(hdfs_url, recurse=True)

def read(hdfs_url, reader, verbose=False, **keys):
    with HDFSFile(hdfs_url, verbose=verbose) as fobj:
        return fobj.read(reader, **keys)

def put(local_file, hdfs_url, verbose=False, force=False):
    """
    Copy the local file to the hdfs_url.
    """

    if verbose:
        print >>stderr,'hdfs',local_file,'->',hdfs_url

    if force:
        if exists(hdfs_url):
            rm(hdfs_url, verbose=verbose)

    command = 'hadoop fs -put %s %s' % (local_file, hdfs_url)
    exit_code, stdo, stde = exec_command(command)
    if exit_code != 0:
        raise RuntimeError("Failed to copy to hdfs %s -> %s: %s" % (local_file,hdfs_url,stde))

def opent(hdfs_url, tmpdir=None, verbose=False):
    """
    pipe the file from hadoop fs -cat into a temporary file, and then return
    the file object for the temporary file.

    The temporary file is automatically cleaned up when it is closed.
    """
    import subprocess
    from subprocess import PIPE
    import tempfile
    bname = os.path.basename(hdfs_url)
    temp_file = tempfile.NamedTemporaryFile(prefix='hdfs-', suffix='-'+bname, dir=tmpdir)

    if verbose:
        print >>stderr,'opening: ',hdfs_url,'for reading, staging in temp file:',temp_file.name

    command = 'hadoop fs -cat %s' % hdfs_url
    pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)

    buffsize = 2*1024*1024
    while True:
        data = pobj.stdout.read(buffsize)
        if len(data) == 0:
            break
        temp_file.write(data)

    # we're done, just need to wait for exit
    ret=pobj.wait()
    if ret != 0:
        raise RuntimeError("Failed to copy to hdfs %s -> %s: %s" % (temp_file.name,hdfs_url,pobj.stderr.read()))

    temp_file.seek(0)
    return temp_file

def rm(hdfs_url, recurse=False, verbose=False):
    """
    Remove the specified hdfs url
    """
    mess='removing '+hdfs_url
        

    if recurse:
        cmd='rmr'
        mess+=' recursively'
    else:
        cmd='rm'

    if verbose:
        print >>stderr,mess

    command = 'hadoop fs -%s %s' % (cmd, hdfs_url)
    exit_code, stdo, stde = exec_command(command)
    if exit_code != 0:
        raise RuntimeError("hdfs %s" % stde)

def rmr(hdfs_url, verbose=False):
    """

    Remove the specified hdfs url recursively.  Equivalent to rm(url,
    recurse=True)
    """
    rm(hdfs_url, recurse=True, verbose=verbose)

def mkdir(hdfs_url, verbose=False):
    """
    Equivalent of mkdir -p in unix
    """
    if verbose:
        print >>stderr,'mkdir',hdfs_url

    command = 'hadoop fs -mkdir '+hdfs_url
    exit_code, stdo, stde = exec_command(command)
    if exit_code != 0:
        raise RuntimeError("hdfs %s" % stde)
