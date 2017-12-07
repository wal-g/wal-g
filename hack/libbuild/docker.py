#!/usr/bin/env python

# Needed for antipackage with python 2
from __future__ import absolute_import

import base64
import json
import sys
from os.path import expanduser

try:
    from urllib.parse import urlencode, urlparse
    from urllib.request import urlopen, Request, unquote
    from urllib.error import HTTPError
except ImportError:
    from urllib import urlencode, unquote
    from urlparse import urlparse
    from urllib2 import urlopen, Request, HTTPError
    from urllib2 import build_opener, HTTPHandler


# TODO: use unicode encoding
def read_json(name):
    try:
        with open(name, 'r') as f:
            return json.load(f)
    except IOError as err:
        print(err)
        sys.exit(1)

# ref: https://success.docker.com/Cloud/Solve/How_do_I_authenticate_with_the_V2_API%3F
def del_tag(namespace, repo, tag):
    home = expanduser("~")
    config = read_json(home + "/.docker/config.json")
    s = config['auths']['https://index.docker.io/v1/']['auth']

    if sys.version_info[0] >= 3:
        s = bytes(s, 'utf-8')
        credentials = base64.b64decode(s).decode('utf-8')
    else:
        credentials = base64.b64decode(s)

    cred = credentials.split(':')
    username = cred[0]
    password = cred[1]

    url = 'https://hub.docker.com/v2/users/login/'
    data = urlencode({'username': username, 'password': password})
    req = Request(url, data)
    response = urlopen(req)
    try:
        body = response.read().decode('utf-8')
        body = json.loads(body)
        token = body['token']
    except:
        print("Error obtaining token")
        sys.exit(1)

    url = 'https://hub.docker.com/v2/repositories/%s/%s/tags/%s/' % (namespace, repo, tag)
    headers = {
        'Authorization': 'JWT %s' % token
    }
    request = Request(url=url, headers=headers)
    request.get_method = lambda: 'DELETE'
    try:
        opener = build_opener(HTTPHandler)
        opener.open(request)
        print('%s/%s:%s deleted successfully.' % (namespace, repo, tag))
        sys.exit(0)
        # body = response.read().decode('utf-8')
    # If we have an HTTPError, try to follow the response
    except HTTPError as err:
        print("Failed to delete tag %s, exiting." % err)
        sys.exit(1)


def help():
    print('docker.py del_tag appscode voyager <tag>')


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # http://stackoverflow.com/a/834451
        # http://stackoverflow.com/a/817296
        globals()[sys.argv[1]](*sys.argv[2:])
    else:
        help()
