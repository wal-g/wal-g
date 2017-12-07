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


REPO_ROOT = ''
BIN_MATRIX = None
BUCKET_MATRIX = None
ENV = os.getenv('APPSCODE_ENV', 'dev').lower()


def _goenv():
    env = {}
    for line in subprocess.check_output(['go', 'env']).split('\n'):
        line = line.strip()
        if len(line) == 0:
            continue
        k, v = line.split('=', 1)
        v = v.strip('"')
        if len(v) > 0:
            env[k] = v
    return env


GOENV = _goenv()
GOPATH = GOENV["GOPATH"]
GOHOSTOS = GOENV["GOHOSTOS"]
GOHOSTARCH = GOENV["GOHOSTARCH"]
GOC = 'go'


def metadata(cwd, goos='', goarch=''):
    md = {
        'commit_hash': subprocess.check_output('git rev-parse --verify HEAD', shell=True, cwd=cwd).strip(),
        'git_branch': subprocess.check_output('git rev-parse --abbrev-ref HEAD', shell=True, cwd=cwd).strip(),
        # http://stackoverflow.com/a/1404862/3476121
        'git_tag': subprocess.check_output('git describe --exact-match --abbrev=0 2>/dev/null || echo ""', shell=True,
                                           cwd=cwd).strip(),
        'commit_timestamp': datetime.datetime.utcfromtimestamp(
            int(subprocess.check_output('git show -s --format=%ct', shell=True, cwd=cwd).strip())).isoformat(),
        'build_timestamp': datetime.datetime.utcnow().isoformat(),
        'build_host': socket.gethostname(),
        'build_host_os': GOENV["GOHOSTOS"],
        'build_host_arch': GOENV["GOHOSTARCH"]
    }
    if md['git_tag']:
        md['version'] = md['git_tag']
        md['version_strategy'] = 'tag'
    elif not md['git_branch'] in ['master', 'HEAD'] and not md['git_branch'].startswith('release-'):
        md['version'] = md['git_branch']
        md['version_strategy'] = 'branch'
    else:
        hash_ver = subprocess.check_output('git describe --tags --always --dirty', shell=True, cwd=cwd).strip()
        md['version'] = hash_ver
        md['version_strategy'] = 'commit_hash'
    if goos:
        md['os'] = goos
    if goarch:
        md['arch'] = goarch
    return md


def read_file(name):
    with open(name, 'r') as f:
        return f.read()
    return ''


def write_file(name, content):
    dir = os.path.dirname(name)
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(name, 'w') as f:
        return f.write(content)


def append_file(name, content):
    with open(name, 'a') as f:
        return f.write(content)


def write_checksum(folder, file):
    cmd = "openssl md5 {0} | sed 's/^.* //' > {0}.md5".format(file)
    subprocess.call(cmd, shell=True, cwd=folder)
    cmd = "openssl sha1 {0} | sed 's/^.* //' > {0}.sha1".format(file)
    subprocess.call(cmd, shell=True, cwd=folder)


# TODO: use unicode encoding
def read_json(name):
    try:
        with open(name, 'r') as f:
            return json.load(f, object_pairs_hook=OrderedDict)
    except IOError:
        return {}


def write_json(obj, name):
    with io.open(name, 'w', encoding='utf-8') as f:
        f.write(unicode(json.dumps(obj, indent=2, separators=(',', ': '), ensure_ascii=False)))


def call(cmd, stdin=None, cwd=None):
    print(cmd)
    return subprocess.call([expandvars(cmd)], shell=True, stdin=stdin, cwd=cwd)


def die(status):
    if status:
        sys.exit(status)


def check_output(cmd, stdin=None, cwd=None):
    print(cmd)
    return subprocess.check_output([expandvars(cmd)], shell=True, stdin=stdin, cwd=cwd)


def deps():
    die(call('go get -u golang.org/x/tools/cmd/goimports'))
    die(call('go get -u golang.org/x/tools/cmd/stringer'))
    die(call('go get -u github.com/Masterminds/glide'))
    die(call('go get -u github.com/sgotti/glide-vc'))
    die(call('go get -u github.com/jteeuwen/go-bindata/...'))
    die(call('go get -u github.com/progrium/go-extpoints'))
    die(call('go get -u github.com/tools/godep'))
    die(call('go get -u github.com/uber/go-torch'))


def to_upper_camel(lower_snake):
    components = lower_snake.split('_')
    # We capitalize the first letter of each component
    # with the 'title' method and join them together.
    return ''.join(x.title() for x in components[:])


# ref: https://golang.org/cmd/go/
def go_build(name, goos, goarch, main):
    linker_opts = []
    if BIN_MATRIX[name].get('go_version', False):
        md = metadata(REPO_ROOT, goos, goarch)
        if md['version_strategy'] == 'tag':
            del md['build_timestamp']
            del md['build_host']
            del md['build_host_os']
            del md['build_host_arch']
        for k, v in md.items():
            linker_opts.append('-X')
            linker_opts.append('main.' + to_upper_camel(k) + '=' + v)

    cgo_env = 'CGO_ENABLED=0'
    cgo = ''
    if BIN_MATRIX[name].get('use_cgo', False):
        cgo_env = "CGO_ENABLED=1"
        cgo = "-a -installsuffix cgo"
        linker_opts.append('-linkmode external -extldflags -static -w')

    ldflags = ''
    if linker_opts:
        ldflags = "-ldflags '{}'".format(' '.join(linker_opts))

    bindir = 'dist/{name}'.format(name=name)
    if not os.path.isdir(bindir):
        os.makedirs(bindir)
    if goos == 'alpine':
        repo_dir = REPO_ROOT[len(GOPATH):]
        uid = check_output('id -u').strip()
        cmd = "docker run --rm -ti -u {uid} -v {repo_root}:/go{repo_dir} -w /go{repo_dir} -e {cgo_env} golang:1.8.3-alpine {goc} build -o {bindir}/{name}-{goos}-{goarch}{ext} {cgo} {ldflags} {main}".format(
            repo_root=REPO_ROOT,
            repo_dir=repo_dir,
            uid=uid,
            name=name,
            goc=GOC,
            goos=goos,
            goarch=goarch,
            bindir=bindir,
            cgo_env=cgo_env,
            cgo=cgo,
            ldflags=ldflags,
            ext='.exe' if goos == 'windows' else '',
            main=main
        )
    else:
        cmd = "GOOS={goos} GOARCH={goarch} {cgo_env} {goc} build -o {bindir}/{name}-{goos}-{goarch}{ext} {cgo} {ldflags} {main}".format(
            name=name,
            goc=GOC,
            goos=goos,
            goarch=goarch,
            bindir=bindir,
            cgo_env=cgo_env,
            cgo=cgo,
            ldflags=ldflags,
            ext='.exe' if goos == 'windows' else '',
            main=main
        )
    die(call(cmd, cwd=REPO_ROOT))
    print('')


def upload_to_cloud(folder, f, version):
    write_checksum(folder, f)
    name = os.path.basename(folder)
    if name not in BIN_MATRIX:
        return
    if ENV == 'prod' and not BIN_MATRIX[name].get('release', False):
        return

    buckets = BUCKET_MATRIX.get(ENV, BUCKET_MATRIX['dev'])
    if not isinstance(buckets, dict):
        buckets = {buckets: ''}
    for bucket, region in buckets.items():
        dst = "{bucket}/binaries/{name}/{version}/{file}".format(
            bucket=bucket,
            name=name,
            version=version,
            file=f
        )
        if bucket.startswith('gs://'):
            upload_to_gcs(folder, f, dst, BIN_MATRIX[name].get('release', False))
        elif bucket.startswith('s3://'):
            upload_to_s3(folder, f, dst, region, BIN_MATRIX[name].get('release', False))


def upload_to_gcs(folder, src, dst, public):
    call("gsutil cp {0} {1}".format(src, dst), cwd=folder)
    call("gsutil cp {0}.md5 {1}.md5".format(src, dst), cwd=folder)
    call("gsutil cp {0}.sha1 {1}.sha1".format(src, dst), cwd=folder)
    if public:
        call("gsutil acl ch -u AllUsers:R {0}".format(dst), cwd=folder)
        call("gsutil acl ch -u AllUsers:R {0}.md5".format(dst), cwd=folder)
        call("gsutil acl ch -u AllUsers:R {0}.sha1".format(dst), cwd=folder)


def upload_to_s3(folder, src, dst, region, public):
    opt_region = ''
    if region:
        opt_region = '--region ' + region
    opt_public = ''
    if public:
        opt_public = "--acl public-read"
    call("aws s3 cp {2} {3} {0} {1}".format(src, dst, opt_region, opt_public), cwd=folder)
    call("aws s3 cp {2} {3} {0}.md5 {1}.md5".format(src, dst, opt_region, opt_public), cwd=folder)
    call("aws s3 cp {2} {3} {0}.sha1 {1}.sha1".format(src, dst, opt_region, opt_public), cwd=folder)


def update_registry(version):
    dist = REPO_ROOT + '/dist'
    bucket = BUCKET_MATRIX.get(ENV, BUCKET_MATRIX['dev'])
    lf = dist + '/latest.txt'
    write_file(lf, version)
    for name in os.listdir(dist):
        if os.path.isfile(dist + '/' + name):
            continue
        if name not in BIN_MATRIX:
            continue
        call("gsutil cp {2} {0}/binaries/{1}/latest.txt".format(bucket, name, lf), cwd=REPO_ROOT)
        if BIN_MATRIX[name].get('release', False):
            call('gsutil acl ch -u AllUsers:R -r {0}/binaries/{1}/latest.txt'.format(bucket, name), cwd=REPO_ROOT)


def ungroup_go_imports(*paths):
    for p in paths:
        if os.path.isfile(p):
            print('Ungrouping imports of file: ' + p)
            _ungroup_go_imports(p)
        elif os.path.isdir(p):
            print('Ungrouping imports of dir: ' + p)
            for dir, _, files in os.walk(p):
                for f in fnmatch.filter(files, '*.go'):
                    _ungroup_go_imports(dir + '/' + f)
        else:
            for f in glob.glob(p):
                print('Ungrouping imports of file: ' + f)
                _ungroup_go_imports(f)


BEGIN_IMPORT_REGEX = ur'import \(\s*'
END_IMPORT_REGEX = ur'\)\s*'


def _ungroup_go_imports(fname):
    with open(fname, 'r+') as f:
        content = f.readlines()
        out = []
        import_block = False
        for line in content:
            c = line.strip()
            if import_block:
                if c == '':
                    continue
                elif re.match(END_IMPORT_REGEX, c) is not None:
                    import_block = False
            elif re.match(BEGIN_IMPORT_REGEX, c) is not None:
                    import_block = True
            out.append(line)
        f.seek(0)
        f.writelines(out)
        f.truncate()
