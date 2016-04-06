import argparse
import json
import requests
import sys

TOKEN_ENDPOINT = '%(host)s/%(api_version)s/token'
STATISTICS_ENDPOINT = ('%(host)s/%(api_version)s'
                       '/observations/asap.weblyzard.com/statistics/geo')
TOKEN = None

def get_access_token(args):
    global TOKEN
    if TOKEN is not None:
        return TOKEN
    r = requests.get(TOKEN_ENDPOINT % args.__dict__,
                     auth=(args.user, args.password))
    if r.status_code == 200:
        TOKEN = r.text
    else:
        print 'Unable to get access token: %s %s' % (r.status_code, r.text)
    return TOKEN


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--host',
                        help='The webLyzard hosting server',
                        default='https://api.weblyzard.com')
    parser.add_argument('--api_version',
                        help='The webLyzard API version',
                        default='0.2')
    parser.add_argument('--user',
                        help='The username used for getting the access token',
                        default='api@asap.weblyzard.com')
    parser.add_argument('json_file',
                        help='The json file to send')
    parser.add_argument('password',
                        help='The password used for getting the access token')
    args = parser.parse_args()

    token = get_access_token(args)
    if token is None:
        sys.exit()
    headers = {'Content-Type': 'application/json',
               'Authorization': 'Bearer %s' % token}
    with open(args.json_file) as fd:
        r = requests.post(STATISTICS_ENDPOINT % args.__dict__, headers=headers,
                          json=json.load(fd))
        print r.status_code, r.text, r.request.headers
