#!/usr/bin/env python3
"""
Google Ads OAuth re-connect (non-interactive save).
Opens the browser for consent, catches the redirect on localhost:8089,
exchanges the code, and writes the new refresh token to .env automatically.
Run when GOOGLE_ADS_REFRESH_TOKEN has expired/been revoked.
"""
import http.server
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import webbrowser

HERE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(HERE, '.env')
REDIRECT_URI = 'http://localhost:8089'
SCOPES = 'https://www.googleapis.com/auth/adwords'
MAX_WAIT_SECONDS = 600


def load_env():
    env = {}
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, _, v = line.partition('=')
                env[k.strip()] = v.strip()
    return env


def save_refresh_token(token):
    with open(ENV_PATH) as f:
        content = f.read()
    lines = content.split('\n')
    replaced = False
    for i, line in enumerate(lines):
        if line.startswith('GOOGLE_ADS_REFRESH_TOKEN='):
            lines[i] = f'GOOGLE_ADS_REFRESH_TOKEN={token}'
            replaced = True
            break
    if not replaced:
        lines.append(f'GOOGLE_ADS_REFRESH_TOKEN={token}')
    with open(ENV_PATH, 'w') as f:
        f.write('\n'.join(lines))


def main():
    env = load_env()
    client_id = env.get('GOOGLE_ADS_CLIENT_ID', '')
    client_secret = env.get('GOOGLE_ADS_CLIENT_SECRET', '')
    if not client_id or not client_secret:
        print('ERROR: client id/secret missing in .env', flush=True)
        sys.exit(1)

    auth_params = urllib.parse.urlencode({
        'client_id': client_id,
        'redirect_uri': REDIRECT_URI,
        'response_type': 'code',
        'scope': SCOPES,
        'access_type': 'offline',
        'prompt': 'consent',
    })
    auth_url = f'https://accounts.google.com/o/oauth2/auth?{auth_params}'

    print('AUTH_URL: ' + auth_url, flush=True)
    print('Opening browser; waiting for you to authorize...', flush=True)

    captured = {'code': None, 'error': None}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if 'code' in params:
                captured['code'] = params['code'][0]
                body = b"<html><body style='font-family:system-ui;text-align:center;padding-top:100px;'><h1 style='color:#22c55e;'>Authorization successful</h1><p>You can close this window.</p></body></html>"
                self.send_response(200)
            elif 'error' in params:
                captured['error'] = params['error'][0]
                body = b"<html><body><h1>Authorization failed</h1></body></html>"
                self.send_response(400)
            else:
                body = b'not found'
                self.send_response(404)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    try:
        webbrowser.open(auth_url)
    except Exception:
        pass

    server = http.server.HTTPServer(('localhost', 8089), Handler)
    server.timeout = 5
    deadline = time.time() + MAX_WAIT_SECONDS
    while captured['code'] is None and captured['error'] is None and time.time() < deadline:
        server.handle_request()
    server.server_close()

    if captured['error']:
        print('ERROR: authorization failed: ' + captured['error'], flush=True)
        sys.exit(1)
    if not captured['code']:
        print('ERROR: timed out waiting for authorization', flush=True)
        sys.exit(1)

    token_data = urllib.parse.urlencode({
        'code': captured['code'],
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code',
    }).encode()
    req = urllib.request.Request('https://oauth2.googleapis.com/token', data=token_data,
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'})
    try:
        with urllib.request.urlopen(req) as resp:
            tokens = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print('ERROR: token exchange failed: ' + e.read().decode()[:300], flush=True)
        sys.exit(1)

    refresh_token = tokens.get('refresh_token', '')
    if not refresh_token:
        print('ERROR: no refresh_token returned: ' + json.dumps(tokens)[:300], flush=True)
        sys.exit(1)

    save_refresh_token(refresh_token)
    print(f'SUCCESS: new refresh token saved to .env ({len(refresh_token)} chars)', flush=True)


if __name__ == '__main__':
    main()
