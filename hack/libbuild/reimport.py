#!/usr/bin/env python

# Needed for antipackage with python 2
from __future__ import absolute_import

import datetime
import fnmatch
import glob
import io
import json
import os
import os.path
import re
import socket
import subprocess
import sys
from collections import OrderedDict
from os.path import expandvars

def reimport(*paths):
    for p in paths:
        if os.path.isfile(p):
            print('Reimporting file: ' + p)
            _reimport(p)
        elif os.path.isdir(p):
            print('Reimporting dir: ' + p)
            for dir, _, files in os.walk(p):
                for f in fnmatch.filter(files, '*.go'):
                    _reimport(dir + '/' + f)
        else:
            for f in glob.glob(p):
                print('Reimporinting file: ' + f)
                _reimport(f)


BEGIN_IMPORT_REGEX = ur'import \(\s*'
END_IMPORT_REGEX = ur'\)\s*'

PKG_MAP = {
    'k8s.io/kubernetes/pkg/api': ['k8s.io/client-go/pkg/api/v1', 'apiv1'],
    'k8s.io/kubernetes/pkg/api/errors': ['k8s.io/apimachinery/pkg/api/errors', 'kerr'],
    'k8s.io/kubernetes/pkg/api/unversioned': ['k8s.io/apimachinery/pkg/apis/meta/v1', 'metav1'],
    'k8s.io/kubernetes/pkg/apimachinery/announced': ['k8s.io/apimachinery/pkg/apimachinery/announced'],
    'k8s.io/kubernetes/pkg/apimachinery/registered': ['k8s.io/client-go/pkg/api', 'kapi'],
    'k8s.io/kubernetes/pkg/apis/apps': ['k8s.io/client-go/pkg/apis/apps/v1beta1', 'apps'],
    'k8s.io/kubernetes/pkg/apis/batch': ['k8s.io/client-go/pkg/apis/batch/v1', 'batch'],
    'k8s.io/kubernetes/pkg/apis/extensions': ['k8s.io/client-go/pkg/apis/extensions/v1beta1', 'extensions'],
    'k8s.io/kubernetes/pkg/apis/storage': ['k8s.io/client-go/pkg/apis/storage/v1', 'storage'],
    'k8s.io/kubernetes/pkg/client/cache': ['k8s.io/client-go/tools/cache'],
    'k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset': ['k8s.io/client-go/kubernetes', 'clientset'],
    'k8s.io/kubernetes/pkg/client/record': ['k8s.io/client-go/tools/record'],
    'k8s.io/kubernetes/pkg/client/restclient': ['k8s.io/client-go/rest'],
    'k8s.io/kubernetes/pkg/client/testing/core': ['k8s.io/client-go/testing'],
    'k8s.io/kubernetes/pkg/client/unversioned/clientcmd': ['k8s.io/client-go/tools/clientcmd'],
    'k8s.io/kubernetes/pkg/fields': ['k8s.io/apimachinery/pkg/fields'],
    'k8s.io/kubernetes/pkg/labels': ['k8s.io/apimachinery/pkg/labels'],
    'k8s.io/kubernetes/pkg/runtime': ['k8s.io/apimachinery/pkg/runtime'],
    'k8s.io/kubernetes/pkg/selection': ['k8s.io/apimachinery/pkg/selection'],
    'k8s.io/kubernetes/pkg/types': ['k8s.io/apimachinery/pkg/types'],
    'k8s.io/kubernetes/pkg/util/intstr':['k8s.io/apimachinery/pkg/util/intstr'],
    'k8s.io/kubernetes/pkg/util/runtime': ['k8s.io/apimachinery/pkg/util/runtime'],
    'k8s.io/kubernetes/pkg/util/sets':['k8s.io/apimachinery/pkg/util/sets'],
    'k8s.io/kubernetes/pkg/util/strategicpatch': ['k8s.io/apimachinery/pkg/util/strategicpatch'],
    'k8s.io/kubernetes/pkg/util/wait': ['k8s.io/apimachinery/pkg/util/wait'],
    'k8s.io/kubernetes/pkg/watch': ['k8s.io/apimachinery/pkg/watch'],
    'k8s.io/kubernetes/pkg/util/wait': ['k8s.io/apimachinery/pkg/util/wait'],
}

def _detect_pkg_alias(line):
    parts = line.strip().split()
    if len(parts) == 2:
        alias = parts[0]
        pkg = parts[1].strip('"')
    else:
        pkg = parts[0].strip('"')
        dirs = pkg.rsplit('/')
        alias = dirs[len(dirs) - 1]
    return pkg, alias

def _reimport(fname):
    with open(fname, 'r+') as f:
        content = f.readlines()
        out = []
        replacements = {}
        import_block = False
        for line in content:
            c = line.strip()
            if import_block:
                if c == '':
                    continue
                elif re.match(END_IMPORT_REGEX, c) is not None:
                    import_block = False
                else:
                    cur_pkg, cur_alias = _detect_pkg_alias(c)
                    if cur_pkg in PKG_MAP:
                        rep = PKG_MAP[cur_pkg]
                        if len(rep) == 2:
                            out.append('%s "%s"\n' % (rep[1], rep[0]))
                            if rep[1] != cur_alias:
                                replacements[cur_alias] = rep[1]
                        else:
                            nu_pkg, nu_alias = _detect_pkg_alias(rep[0])
                            out.append('"%s"\n' % (rep[0]))
                            if nu_alias != cur_alias:
                                replacements[cur_alias] = nu_alias
                        continue
            elif re.match(BEGIN_IMPORT_REGEX, c) is not None:
                    import_block = True
            out.append(line)

        content = out
        out = []

        for line in content:
            c = line.strip()
            if import_block:
                if c == '':
                    continue
                elif re.match(END_IMPORT_REGEX, c) is not None:
                    import_block = False
            elif re.match(BEGIN_IMPORT_REGEX, c) is not None:
                    import_block = True
            else:
                for cur, nu in replacements.iteritems():
                    line = line.replace(cur+'.', nu+'.')
            out.append(line)

        f.seek(0)
        f.writelines(out)
        f.truncate()


if __name__ == "__main__":
    reimport(*sys.argv[1:])
    # if len(sys.argv) > 1:
    #     # http://stackoverflow.com/a/834451
    #     # http://stackoverflow.com/a/817296
    #     globals()[sys.argv[1]](*sys.argv[2:])
    # else:
    #     help()
