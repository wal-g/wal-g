[![CLA assistant](https://cla-assistant.io/readme/badge/appscode/libbuild)](https://cla-assistant.io/appscode/libbuild)

[Website](https://appscode.com) • [Slack](https://slack.appscode.com) • [Forum](https://discuss.appscode.com) • [Twitter](https://twitter.com/AppsCodeHQ)

Common build scripts used by AppsCode repositories.

## Installing
Use `libbuild` as a git subtree in your project.

## Usage
### Using as git subtree with shell scripts

```sh
# add first time
git subtree add --prefix hack/libbuild https://github.com/appscode/libbuild.git master --squash

# update later
git subtree pull --prefix hack/libbuild https://github.com/appscode/libbuild.git master --squash
```

To learn about `git subtree`, check the following articles:
 * http://blogs.atlassian.com/2013/05/alternatives-to-git-submodule-git-subtree/
 * https://developer.atlassian.com/blog/2015/05/the-power-of-git-subtree/

### Using from python scripts
We recommend using https://github.com/ellisonbg/antipackage to import libbuild.py . First install `antipackage` using pip:

```sh
pip install git+https://github.com/ellisonbg/antipackage.git#egg=antipackage
```

Now, add the following lines into a build script to import libuild.py.
```python
# http://stackoverflow.com/a/14050282
def check_antipackage():
    from sys import version_info
    sys_version = version_info[:2]
    found = True
    if sys_version < (3, 0):
        # 'python 2'
        from pkgutil import find_loader
        found = find_loader('antipackage') is not None
    elif sys_version <= (3, 3):
        # 'python <= 3.3'
        from importlib import find_loader
        found = find_loader('antipackage') is not None
    else:
        # 'python >= 3.4'
        from importlib import util
        found = util.find_spec('antipackage') is not None
    if not found:
        print('Install missing package "antipackage"')
        print('Example: pip install git+https://github.com/ellisonbg/antipackage.git#egg=antipackage')
        from sys import exit
        exit(1)
check_antipackage()

# ref: https://github.com/ellisonbg/antipackage
import antipackage
from github.appscode.libbuild import libbuild
```

## Acknowledgement
- `pydotenv` is a fork of [python-dotenv](https://github.com/theskumar/python-dotenv). This provides support of [antipackage](https://github.com/ellisonbg/antipackage)
for python-dotenv.

## License
`libbuild` is licensed under the Apache 2.0 license. See the LICENSE file for details.
