import os
import re
import shutil
import sys
from pathlib import Path


class Test:
    s1 = '/home/baitaluk/projects/temp/DATA'
    d1 = '/www/data-import/data/test'

    vn = ['dataset', 'asns', 'orgs', 'links', 'locations']

    def __init__(self):
        if self.check_dir(self.s1) and self.check_dir(self.d1):
            self.start()

    def start(self):
        RE_INT = re.compile(r'^([0-9]\d*)$')
        print("START PROCESS...")
        dsources = [(x, os.path.join(self.s1, x)) for x in os.listdir(self.s1) if RE_INT.match(x)]
        countries = [(x, os.path.join(self.s1, x)) for x in os.listdir(self.s1) if not RE_INT.match(x)]

        for ds in dsources:
            dname, dpath = ds
            dsipv4 = self.create_dir(os.path.join(self.d1, 'ipv4', dname))
            dsipv6 = self.create_dir(os.path.join(self.d1, 'ipv6', dname))

            self.copy_dir(countries[0][1], dsipv4)
            self.copy_dir(countries[0][1], dsipv6)

            self.create_dir(os.path.join(dsipv4, 'global'))
            self.create_dir(os.path.join(dsipv6, 'global'))

            sfiles = [(f, os.path.join(dpath, f)) for f in os.listdir(dpath)]
            for sf in sfiles:
                fname, fpath = sf
                if fname.endswith('.jsonl'):
                    fn = re.sub("^[\d]+\.", "", fname)
                    f4dest = os.path.join(dsipv4, 'global', fn)
                    f6dest = os.path.join(dsipv6, 'global', fn)

                    self.copy_file(fpath, f4dest)
                    print('{} -> {}'.format(fpath, f4dest))

                    self.copy_file(fpath, f6dest)
                    print('{} -> {}'.format(fpath, f6dest))


    @staticmethod
    def check_dir(folder):
        result = False
        try:
            if os.path.exists(folder) and os.path.isdir(folder) and os.access(folder, os.R_OK & os.W_OK):
                result = True
                print('Directory {} is OK.'.format(folder))
            return result
        except IOError as e:
            print(e, file=sys.stderr)
            exit(1)

    @staticmethod
    def check_file(f):
        result = False
        try:
            if os.path.exists(f) and os.path.isfile(f) and os.access(f, os.W_OK):
                result = True
            return result
        except IOError as e:
            print(e, file=sys.stderr)
        return result

    @staticmethod
    def create_dir(src):
        result = None
        try:
            if os.path.exists(src):
                shutil.rmtree(src)
            if not os.path.exists(src):
                os.mkdir(src)
                result = src
        except IOError as e:
            print(e, file=sys.stderr)
        return result

    @staticmethod
    def copy_dir(src, dest):
        try:
            if os.path.exists(dest) and os.path.isdir(dest):
                os.rmdir(dest)
            shutil.copytree(src, dest)
        except shutil.Error as e:
            print('Directory not copied. Error: %s' % e)
        except OSError as e:
            print('Directory not copied. Error: %s' % e)

    @staticmethod
    def copy_file(src, dest):
        try:
            if os.path.exists(dest) and os.path.isfile(dest):
                os.remove(dest)
            shutil.copy(src, dest)
        except shutil.Error as e:
            print('Directory not copied. Error: %s' % e)
        except OSError as e:
                print('Directory not copied. Error: %s' % e)


if __name__ == '__main__':
    try:
        test = Test()
    except Exception as e:
        print(e, file=sys.stderr)
