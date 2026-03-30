#!/usr/bin/env python3
"""
Google Ads OAuth2 Setup Helper
===============================
Run this script ONCE to get your refresh token for the Google Ads API.

Prerequisites:
  1. Enable "Google Ads API" in your Google Cloud project
  2. Create OAuth Desktop credentials (Client ID + Client Secret)
  3. Add Client ID and Client Secret to .env

Usage:
  python google_ads_auth.py

It will open your browser, you log in, and it prints the refresh token.
Paste the token into your .env file as GOOGLE_ADS_REFRESH_TOKEN.
"""

import http.server
import json
import os
import sys
import urllib.parse
import urllib.request
import webbrowser

# Load .env manually (no dependency on python-dotenv for this standalone script)
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    env = {}
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    env[key.strip()] = value.strip()
    return env


def main():
    env = load_env()

    client_id = env.get('GOOGLE_ADS_CLIENT_ID', '')
    client_secret = env.get('GOOGLE_ADS_CLIENT_SECRET', '')

    if not client_id or not client_secret:
        print("\n❌ GOOGLE_ADS_CLIENT_ID and GOOGLE_ADS_CLIENT_SECRET must be set in .env")
        print("\nTo get these:")
        print("  1. Go to console.cloud.google.com")
        print("  2. APIs & Services → Credentials")
        print("  3. Create Credentials → OAuth Client ID → Desktop app")
        print("  4. Copy Client ID and Client Secret into .env")
        sys.exit(1)

    # OAuth2 configuration
    REDIRECT_URI = 'http://localhost:8089'
    SCOPES = 'https://www.googleapis.com/auth/adwords'

    # Build authorization URL
    auth_params = urllib.parse.urlencode({
        'client_id': client_id,
        'redirect_uri': REDIRECT_URI,
        'response_type': 'code',
        'scope': SCOPES,
        'access_type': 'offline',
        'prompt': 'consent',
    })
    auth_url = f'https://accounts.google.com/o/oauth2/auth?{auth_params}'

    print("\n🔐 Google Ads OAuth2 Setup")
    print("=" * 50)
    print(f"\nOpening browser for authorization...")
    print(f"\nIf the browser doesn't open, go to:\n{auth_url}\n")

    # Start a temporary local server to catch the redirect
    auth_code = None

    class AuthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal auth_code
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query)

            if 'code' in params:
                auth_code = params['code'][0]
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()
                self.wfile.write(b"""
                <html><body style="font-family: system-ui; text-align: center; padding-top: 100px;">
                <h1 style="color: #22c55e;">&#x2705; Authorization Successful!</h1>
                <p>You can close this window and go back to the terminal.</p>
                </body></html>
                """)
            elif 'error' in params:
                self.send_response(400)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()
                error = params.get('error', ['unknown'])[0]
                self.wfile.write(f"""
                <html><body style="font-family: system-ui; text-align: center; padding-top: 100px;">
                <h1 style="color: #ef4444;">&#x274c; Authorization Failed</h1>
                <p>Error: {error}</p>
                </body></html>
                """.encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # Suppress server logs

    # Open browser
    webbrowser.open(auth_url)

    # Wait for the callback
    server = http.server.HTTPServer(('localhost', 8089), AuthHandler)
    print("Waiting for authorization (listening on localhost:8089)...")
    server.handle_request()
    server.server_close()

    if not auth_code:
        print("\n❌ No authorization code received. Please try again.")
        sys.exit(1)

    print("\n✅ Authorization code received! Exchanging for refresh token...")

    # Exchange auth code for tokens
    token_data = urllib.parse.urlencode({
        'code': auth_code,
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code',
    }).encode()

    req = urllib.request.Request(
        'https://oauth2.googleapis.com/token',
        data=token_data,
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
    )

    try:
        with urllib.request.urlopen(req) as resp:
            tokens = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"\n❌ Token exchange failed: {body}")
        sys.exit(1)

    refresh_token = tokens.get('refresh_token', '')

    if not refresh_token:
        print("\n❌ No refresh token in response. Try again with 'prompt=consent'.")
        print(f"   Response: {json.dumps(tokens, indent=2)}")
        sys.exit(1)

    # Show the token
    print("\n" + "=" * 50)
    print("🎉 SUCCESS! Here is your refresh token:\n")
    print(f"   {refresh_token}")
    print("\n" + "=" * 50)

    # Offer to update .env automatically
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    print(f"\nWould you like to save this to {env_path}? [Y/n] ", end='')
    choice = input().strip().lower()

    if choice in ('', 'y', 'yes'):
        # Read current .env
        with open(env_path, 'r') as f:
            content = f.read()

        # Replace the empty refresh token line
        if 'GOOGLE_ADS_REFRESH_TOKEN=' in content:
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if line.startswith('GOOGLE_ADS_REFRESH_TOKEN='):
                    lines[i] = f'GOOGLE_ADS_REFRESH_TOKEN={refresh_token}'
                    break
            content = '\n'.join(lines)
        else:
            content += f'\nGOOGLE_ADS_REFRESH_TOKEN={refresh_token}\n'

        with open(env_path, 'w') as f:
            f.write(content)

        print(f"\n✅ Saved to .env! You're all set.")
        print(f"\nTest it by running:")
        print(f"  cd '{os.path.dirname(__file__)}'")
        print(f"  python -c \"from collectors.google_ads_collector import collect_weekly_data; import logging; logging.basicConfig(level=logging.INFO); print(collect_weekly_data())\"")
    else:
        print(f"\nNo problem. Manually add this line to your .env:")
        print(f"  GOOGLE_ADS_REFRESH_TOKEN={refresh_token}")

    print("\n✅ Google Ads API setup complete!")


if __name__ == '__main__':
    main()
