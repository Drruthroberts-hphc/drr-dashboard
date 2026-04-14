"""
Social Media Data Collector
============================
Collects YouTube channel metrics via the YouTube Data API v3.
Collects Facebook Page and Instagram metrics via the Meta Graph API.
Falls back to Google Sheet analytics export when Meta API is unavailable.
"""

import json
import logging
import os
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

from config import (
    YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID,
    META_PAGE_ACCESS_TOKEN, FB_PAGE_ID, IG_ACCOUNT_ID,
    GOOGLE_SHEETS_CREDENTIALS,
)

META_GRAPH_BASE = 'https://graph.facebook.com/v25.0'

logger = logging.getLogger(__name__)

YOUTUBE_BASE = 'https://www.googleapis.com/youtube/v3'

# Google Sheet with post-level analytics (Metricool export)
SM_ANALYTICS_SHEET_ID = '12j6TpP0TuEQmlensYwt8xXZYSe7lmAcTsT9HyQEk-NU'


def _youtube_get(endpoint, params):
    """Make a request to the YouTube Data API v3."""
    params['key'] = YOUTUBE_API_KEY
    url = f"{YOUTUBE_BASE}/{endpoint}?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={
        'Accept': 'application/json',
    })

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')[:300]
        logger.error(f"YouTube API error {e.code} on {endpoint}: {body}")
        return None


def _get_channel_stats():
    """Get current channel statistics."""
    data = _youtube_get('channels', {
        'part': 'statistics,snippet',
        'id': YOUTUBE_CHANNEL_ID,
    })
    if not data or not data.get('items'):
        return {}

    channel = data['items'][0]
    stats = channel.get('statistics', {})

    return {
        'subscribers': int(stats.get('subscriberCount', 0)),
        'total_views': int(stats.get('viewCount', 0)),
        'total_videos': int(stats.get('videoCount', 0)),
    }


def _get_recent_videos(published_after, published_before, max_results=50):
    """Search for videos published in a date range."""
    data = _youtube_get('search', {
        'part': 'id',
        'channelId': YOUTUBE_CHANNEL_ID,
        'type': 'video',
        'publishedAfter': published_after,
        'publishedBefore': published_before,
        'maxResults': max_results,
        'order': 'date',
    })

    if not data:
        return []

    return [item['id']['videoId'] for item in data.get('items', [])]


def _get_video_stats(video_ids):
    """Get statistics for a list of video IDs."""
    if not video_ids:
        return []

    # YouTube API accepts up to 50 IDs at once
    data = _youtube_get('videos', {
        'part': 'statistics,contentDetails',
        'id': ','.join(video_ids[:50]),
    })

    if not data:
        return []

    return data.get('items', [])


def _meta_get(endpoint, params):
    """Make a request to the Meta Graph API."""
    params['access_token'] = META_PAGE_ACCESS_TOKEN
    url = f"{META_GRAPH_BASE}/{endpoint}?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={
        'Accept': 'application/json',
    })

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')[:300]
        logger.error(f"Meta API error {e.code} on {endpoint}: {body}")
        return None


def _parse_duration_seconds(duration_str):
    """Parse ISO 8601 duration (PT1H2M3S) to seconds."""
    import re
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str or '')
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def _get_uploads_playlist_id():
    """Derive the uploads playlist ID from the channel ID."""
    if YOUTUBE_CHANNEL_ID.startswith('UC'):
        return 'UU' + YOUTUBE_CHANNEL_ID[2:]
    # Fallback: query the channel API
    data = _youtube_get('channels', {
        'part': 'contentDetails',
        'id': YOUTUBE_CHANNEL_ID,
    })
    if data and data.get('items'):
        return data['items'][0].get('contentDetails', {}).get(
            'relatedPlaylists', {}
        ).get('uploads', '')
    return ''


def _get_recent_channel_videos(max_results=10):
    """Get recent videos from the channel's uploads playlist with stats."""
    playlist_id = _get_uploads_playlist_id()
    if not playlist_id:
        return []

    data = _youtube_get('playlistItems', {
        'part': 'contentDetails,snippet',
        'playlistId': playlist_id,
        'maxResults': max_results,
    })

    if not data or not data.get('items'):
        return []

    playlist_items = data['items']
    video_ids = [item['contentDetails']['videoId'] for item in playlist_items]

    video_stats = _get_video_stats(video_ids)
    stats_by_id = {v['id']: v for v in video_stats}

    results = []
    for item in playlist_items:
        vid = item['contentDetails']['videoId']
        snippet = item.get('snippet', {})
        vdata = stats_by_id.get(vid, {})
        stats = vdata.get('statistics', {})
        duration_s = _parse_duration_seconds(
            vdata.get('contentDetails', {}).get('duration', '')
        )

        results.append({
            'title': snippet.get('title', '')[:60],
            'published': snippet.get('publishedAt', '')[:10],
            'views': int(stats.get('viewCount', 0)),
            'likes': int(stats.get('likeCount', 0)),
            'comments': int(stats.get('commentCount', 0)),
            'duration_min': round(duration_s / 60, 1),
        })

    return results


def _get_fb_top_posts(week_start, week_ending_date, limit=5):
    """Get top performing Facebook Page posts for the week."""
    if not META_PAGE_ACCESS_TOKEN or not FB_PAGE_ID:
        return []

    try:
        data = _meta_get(f'{FB_PAGE_ID}/posts', {
            'fields': 'message,created_time,shares,permalink_url,'
                      'reactions.summary(true),comments.summary(true)',
            'limit': 25,
            'since': str(week_start),
            'until': str(week_ending_date + timedelta(days=1)),
        })

        if not data or 'data' not in data:
            return []

        posts = []
        for post in data['data']:
            reactions = post.get('reactions', {}).get('summary', {}).get('total_count', 0)
            comments = post.get('comments', {}).get('summary', {}).get('total_count', 0)
            shares = 0
            if isinstance(post.get('shares'), dict):
                shares = post['shares'].get('count', 0)

            msg = (post.get('message') or '')[:80]
            posts.append({
                'message': msg if msg else '(no text)',
                'date': post.get('created_time', '')[:10],
                'reactions': reactions,
                'comments': comments,
                'shares': shares,
                'total_engagement': reactions + comments + shares,
                'link': post.get('permalink_url', ''),
            })

        posts.sort(key=lambda x: x['total_engagement'], reverse=True)
        return posts[:limit]
    except Exception as e:
        logger.error(f"Failed to fetch FB top posts: {e}")
        return []


def _get_ig_top_posts(week_start, week_ending_date, limit=5):
    """Get top performing Instagram posts for the week."""
    if not META_PAGE_ACCESS_TOKEN or not IG_ACCOUNT_ID:
        return []

    try:
        data = _meta_get(f'{IG_ACCOUNT_ID}/media', {
            'fields': 'caption,timestamp,like_count,comments_count,media_type,permalink',
            'limit': 25,
        })

        if not data or 'data' not in data:
            return []

        posts = []
        for post in data['data']:
            post_date = post.get('timestamp', '')[:10]
            try:
                pd = datetime.strptime(post_date, '%Y-%m-%d').date()
            except ValueError:
                continue
            if week_start <= pd <= week_ending_date:
                likes = post.get('like_count', 0)
                comments = post.get('comments_count', 0)
                caption = (post.get('caption') or '')[:80]
                posts.append({
                    'caption': caption if caption else '(no caption)',
                    'date': post_date,
                    'likes': likes,
                    'comments': comments,
                    'media_type': post.get('media_type', ''),
                    'total_engagement': likes + comments,
                    'link': post.get('permalink', ''),
                })

        posts.sort(key=lambda x: x['total_engagement'], reverse=True)
        return posts[:limit]
    except Exception as e:
        logger.error(f"Failed to fetch IG top posts: {e}")
        return []


def _load_previous_social_snapshot(week_ending_date):
    """Load the most recent snapshot's social data before the given date."""
    import os
    snapshot_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'snapshots')
    if not os.path.isdir(snapshot_dir):
        return {}

    prev_social = {}
    for fname in sorted(os.listdir(snapshot_dir)):
        if not fname.startswith('snapshot_') or not fname.endswith('.json'):
            continue
        date_part = fname.replace('snapshot_', '').replace('.json', '')
        if date_part < str(week_ending_date):
            try:
                with open(os.path.join(snapshot_dir, fname), 'r') as f:
                    snap = json.load(f)
                    # Snapshots store data under 'all_data' key
                    all_data = snap.get('all_data', snap)
                    prev_social = all_data.get('social') or {}
            except Exception:
                pass
    return prev_social


def _load_sheet_social_data(week_start, week_ending_date):
    """Load FB/IG/LinkedIn post data from the analytics Google Sheet.

    Returns dict with aggregated weekly metrics and top posts, or None on failure.
    The sheet has tabs: FB, IG, LinkedIn — each with post-level rows including
    Publish time, Description, Trigger, CTA, Views, Reach, Reactions/Comments/Shares, Clicks.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        logger.warning("gspread not installed — cannot read SM analytics sheet")
        return None

    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        creds_file = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                  GOOGLE_SHEETS_CREDENTIALS)
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(SM_ANALYTICS_SHEET_ID)
    except Exception as e:
        logger.warning(f"Cannot open SM analytics sheet: {e}")
        return None

    start_str = str(week_start)
    end_str = str(week_ending_date)

    result = {}

    # ── Facebook tab ─────────────────────────────────────────────────────
    try:
        fb_ws = sheet.worksheet('FB')
        fb_rows = fb_ws.get_all_values()
        fb_headers = fb_rows[0] if fb_rows else []

        # Find column indices
        def col_idx(name):
            for i, h in enumerate(fb_headers):
                if h.strip().lower() == name.lower():
                    return i
            return -1

        date_col = col_idx('Publish time')
        desc_col = col_idx('Description')
        trigger_col = col_idx('TRIGGER')
        views_col = col_idx('Views')
        reach_col = col_idx('Reach')
        engage_col = col_idx('Reactions, Comments and Shares')
        reactions_col = col_idx('Reactions')
        comments_col = col_idx('Comments')
        shares_col = col_idx('Shares')
        clicks_col = col_idx('Total clicks')
        link_col = col_idx('Permalink')

        fb_week_posts = []
        fb_all_posts = []

        for row in fb_rows[1:]:
            if not row[date_col] if date_col >= 0 else True:
                continue
            try:
                raw_date = row[date_col].strip()
                # Parse MM/DD/YYYY H:MM format
                post_dt = datetime.strptime(raw_date, '%m/%d/%Y %H:%M')
                post_date = post_dt.date()
                post_date_str = str(post_date)
            except (ValueError, IndexError):
                continue

            def safe_int(idx):
                if idx < 0 or idx >= len(row):
                    return 0
                v = row[idx].strip().replace(',', '')
                try:
                    return int(float(v)) if v else 0
                except ValueError:
                    return 0

            def safe_str(idx):
                if idx < 0 or idx >= len(row):
                    return ''
                return row[idx].strip()

            post = {
                'date': post_date_str,
                'message': safe_str(desc_col)[:80],
                'trigger': safe_str(trigger_col),
                'views': safe_int(views_col),
                'reach': safe_int(reach_col),
                'reactions': safe_int(reactions_col),
                'comments': safe_int(comments_col),
                'shares': safe_int(shares_col),
                'total_engagement': safe_int(engage_col),
                'total_clicks': safe_int(clicks_col),
                'link': safe_str(link_col),
            }

            fb_all_posts.append(post)
            if start_str <= post_date_str <= end_str:
                fb_week_posts.append(post)

        # Aggregate weekly FB metrics
        fb_week_views = sum(p['views'] for p in fb_week_posts)
        fb_week_reach = sum(p['reach'] for p in fb_week_posts)
        fb_week_likes = sum(p['reactions'] for p in fb_week_posts)
        fb_week_comments = sum(p['comments'] for p in fb_week_posts)
        fb_week_shares = sum(p['shares'] for p in fb_week_posts)
        fb_week_clicks = sum(p['total_clicks'] for p in fb_week_posts)
        fb_week_engagement = sum(p['total_engagement'] for p in fb_week_posts)

        # Top 5 posts by clicks (best indicator of interest)
        fb_top = sorted(fb_week_posts, key=lambda x: x['total_clicks'], reverse=True)[:5]

        result['fb_week_posts_count'] = len(fb_week_posts)
        result['fb_week_views'] = fb_week_views
        result['fb_week_reach'] = fb_week_reach
        result['fb_week_likes'] = fb_week_likes
        result['fb_week_comments'] = fb_week_comments
        result['fb_week_shares'] = fb_week_shares
        result['fb_week_clicks'] = fb_week_clicks
        result['fb_week_engagement'] = fb_week_engagement
        result['fb_top_posts'] = fb_top

        logger.info(f"Sheet FB: {len(fb_week_posts)} posts this week, "
                    f"reach={fb_week_reach:,}, clicks={fb_week_clicks:,}")

    except Exception as e:
        logger.warning(f"Failed to read FB tab from sheet: {e}")

    # ── Instagram tab ────────────────────────────────────────────────────
    try:
        ig_ws = sheet.worksheet('IG')
        ig_rows = ig_ws.get_all_values()
        ig_headers = ig_rows[0] if ig_rows else []

        def ig_col_idx(name):
            for i, h in enumerate(ig_headers):
                if h.strip().lower() == name.lower():
                    return i
            return -1

        ig_date_col = ig_col_idx('Publish time')
        ig_desc_col = ig_col_idx('Description')
        ig_views_col = ig_col_idx('Views')
        ig_reach_col = ig_col_idx('Reach')
        ig_likes_col = ig_col_idx('Likes')
        ig_comments_col = ig_col_idx('Comments')
        ig_link_col = ig_col_idx('Permalink')

        ig_week_posts = []
        for row in ig_rows[1:]:
            if ig_date_col < 0 or not row[ig_date_col].strip():
                continue
            try:
                raw_date = row[ig_date_col].strip()
                post_dt = datetime.strptime(raw_date, '%m/%d/%Y %H:%M')
                post_date_str = str(post_dt.date())
            except (ValueError, IndexError):
                continue

            def safe_int_ig(idx):
                if idx < 0 or idx >= len(row):
                    return 0
                v = row[idx].strip().replace(',', '')
                try:
                    return int(float(v)) if v else 0
                except ValueError:
                    return 0

            def safe_str_ig(idx):
                if idx < 0 or idx >= len(row):
                    return ''
                return row[idx].strip()

            if start_str <= post_date_str <= end_str:
                ig_week_posts.append({
                    'date': post_date_str,
                    'caption': safe_str_ig(ig_desc_col)[:80],
                    'views': safe_int_ig(ig_views_col),
                    'reach': safe_int_ig(ig_reach_col),
                    'likes': safe_int_ig(ig_likes_col),
                    'comments': safe_int_ig(ig_comments_col),
                    'total_engagement': safe_int_ig(ig_likes_col) + safe_int_ig(ig_comments_col),
                    'link': safe_str_ig(ig_link_col),
                })

        ig_week_likes = sum(p['likes'] for p in ig_week_posts)
        ig_week_comments = sum(p['comments'] for p in ig_week_posts)
        ig_top = sorted(ig_week_posts, key=lambda x: x['total_engagement'], reverse=True)[:5]

        result['ig_week_posts_count'] = len(ig_week_posts)
        result['ig_week_likes'] = ig_week_likes
        result['ig_week_comments'] = ig_week_comments
        result['ig_top_posts'] = ig_top

        logger.info(f"Sheet IG: {len(ig_week_posts)} posts this week")

    except Exception as e:
        logger.warning(f"Failed to read IG tab from sheet: {e}")

    # ── LinkedIn tab ─────────────────────────────────────────────────────
    try:
        li_ws = sheet.worksheet('LinkedIn')
        li_rows = li_ws.get_all_values()
        li_headers = li_rows[0] if li_rows else []

        def li_col_idx(name):
            for i, h in enumerate(li_headers):
                if h.strip().lower() == name.lower():
                    return i
            return -1

        li_date_col = li_col_idx('Publish time')
        li_desc_col = li_col_idx('Description')
        li_views_col = li_col_idx('Views')
        li_reach_col = li_col_idx('Reach')
        li_reactions_col = li_col_idx('Reactions')
        li_comments_col = li_col_idx('Comments')
        li_link_col = li_col_idx('Permalink')

        li_week_posts = []
        for row in li_rows[1:]:
            if li_date_col < 0 or not row[li_date_col].strip():
                continue
            try:
                raw_date = row[li_date_col].strip()
                # LinkedIn dates may use different formats
                for fmt in ('%m/%d/%Y %H:%M', '%B %d, %Y %H:%M', '%b %d, %Y %H:%M',
                            '%B %d, %Y', '%b %d, %Y', '%m/%d/%Y'):
                    try:
                        post_dt = datetime.strptime(raw_date, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    continue
                post_date_str = str(post_dt.date())
            except (ValueError, IndexError):
                continue

            def safe_int_li(idx):
                if idx < 0 or idx >= len(row):
                    return 0
                v = row[idx].strip().replace(',', '')
                try:
                    return int(float(v)) if v else 0
                except ValueError:
                    return 0

            def safe_str_li(idx):
                if idx < 0 or idx >= len(row):
                    return ''
                return row[idx].strip()

            if start_str <= post_date_str <= end_str:
                li_week_posts.append({
                    'date': post_date_str,
                    'message': safe_str_li(li_desc_col)[:80],
                    'views': safe_int_li(li_views_col),
                    'reach': safe_int_li(li_reach_col),
                    'reactions': safe_int_li(li_reactions_col),
                    'comments': safe_int_li(li_comments_col),
                    'link': safe_str_li(li_link_col),
                })

        result['li_week_posts'] = li_week_posts
        result['li_week_posts_count'] = len(li_week_posts)
        logger.info(f"Sheet LinkedIn: {len(li_week_posts)} posts this week")

    except Exception as e:
        logger.warning(f"Failed to read LinkedIn tab from sheet: {e}")

    return result if result else None


def collect_weekly_data(week_ending_date=None):
    """
    Collect all social media metrics for a given week.

    Returns:
        dict with all social/YouTube weekly metrics
    """
    if week_ending_date is None:
        today = datetime.utcnow().date()
        days_since_sunday = (today.weekday() + 1) % 7
        week_ending_date = today - timedelta(days=days_since_sunday)

    week_start = week_ending_date - timedelta(days=6)
    start_iso = f"{week_start}T00:00:00Z"
    end_iso = f"{week_ending_date}T23:59:59Z"

    logger.info(f"Collecting social media data for week {week_start} to {week_ending_date}")

    # Load previous snapshot for fallback if API fails
    prev_snap_social = _load_previous_social_snapshot(week_ending_date)

    # ── YouTube Channel Stats ────────────────────────────────────────────
    channel_stats = _get_channel_stats()
    yt_subscribers = channel_stats.get('subscribers', 0)
    yt_total_views = channel_stats.get('total_views', 0)

    # Subscriber growth requires previous week's data (calculated in cross-platform)
    yt_sub_growth = 0

    # ── YouTube Weekly Video Activity ────────────────────────────────────
    video_ids = _get_recent_videos(start_iso, end_iso)
    yt_new_videos = len(video_ids)

    # Get stats for videos published this week
    yt_new_video_views = 0
    yt_new_video_comments = 0

    if video_ids:
        videos = _get_video_stats(video_ids)
        for video in videos:
            stats = video.get('statistics', {})
            yt_new_video_views += int(stats.get('viewCount', 0))
            yt_new_video_comments += int(stats.get('commentCount', 0))

    # ── YouTube Recent Videos (regardless of publish date) ───────────────
    yt_recent_videos = _get_recent_channel_videos(10)
    logger.info(f"YouTube: {len(yt_recent_videos)} recent videos fetched")

    # Sum total comments and compute avg video duration from recent videos
    yt_total_comments = sum(v.get('comments', 0) for v in yt_recent_videos)
    avg_duration_min = 0
    if yt_recent_videos:
        avg_duration_min = sum(v.get('duration_min', 0) for v in yt_recent_videos) / len(yt_recent_videos)

    # Weekly views: will be computed as delta from yt_total_views in dashboard
    # For the collector, store new-video views as a fallback
    yt_views = yt_new_video_views
    yt_comments = yt_new_video_comments

    # Estimate watch hours from total views (delta computed in dashboard)
    # avg_duration_min * 0.4 watch-through rate = avg watch minutes per view
    yt_avg_watch_min = avg_duration_min * 0.4
    yt_watch_hours = 0.0  # Will be estimated in dashboard from weekly views delta

    # ── Facebook Page Metrics (Meta Graph API) ───────────────────────────
    fb_followers = 0
    fb_follower_growth = 0      # WoW calculated from snapshots
    fb_reach = 0
    fb_engagement_rate = 0.0
    fb_messages = 0

    if META_PAGE_ACCESS_TOKEN and FB_PAGE_ID:
        try:
            fb_data = _meta_get(FB_PAGE_ID, {
                'fields': 'fan_count,followers_count,talking_about_count',
            })
            if fb_data and 'error' not in fb_data:
                fb_followers = fb_data.get('followers_count', 0)
                talking = fb_data.get('talking_about_count', 0)
                if fb_followers > 0:
                    fb_engagement_rate = round(talking / fb_followers, 4)
                fb_reach = talking
                logger.info(f"FB: {fb_followers:,} followers, {talking} engaged")
            else:
                logger.warning(f"FB page API error: {fb_data}")
                # Carry forward followers from previous snapshot
                fb_followers = prev_snap_social.get('fb_followers', 0)
                if fb_followers:
                    logger.info(f"FB: carried forward {fb_followers:,} followers from previous snapshot")
        except Exception as e:
            logger.error(f"Facebook collection failed: {e}")
            fb_followers = prev_snap_social.get('fb_followers', 0)

    # ── Instagram Metrics (Meta Graph API) ────────────────────────────────
    ig_followers = 0
    ig_follower_growth = 0      # WoW calculated from snapshots
    ig_engagement_rate = 0.0
    ig_story_views = 0
    ig_dms = 0

    if META_PAGE_ACCESS_TOKEN and IG_ACCOUNT_ID:
        try:
            # Get follower count and media count
            ig_data = _meta_get(IG_ACCOUNT_ID, {
                'fields': 'followers_count,media_count',
            })
            if ig_data and 'error' not in ig_data:
                ig_followers = ig_data.get('followers_count', 0)
                logger.info(f"IG: {ig_followers:,} followers")
            else:
                logger.warning(f"IG API error: {ig_data}")
                ig_followers = prev_snap_social.get('ig_followers', 0)
                if ig_followers:
                    logger.info(f"IG: carried forward {ig_followers:,} followers from previous snapshot")

            # Get recent posts for engagement calculation
            ig_media = _meta_get(f"{IG_ACCOUNT_ID}/media", {
                'fields': 'like_count,comments_count,timestamp',
                'limit': 25,
            })
            if ig_media and 'data' in ig_media:
                week_likes = 0
                week_comments = 0
                week_posts = 0
                for post in ig_media['data']:
                    post_date = post.get('timestamp', '')[:10]
                    try:
                        pd = datetime.strptime(post_date, '%Y-%m-%d').date()
                    except ValueError:
                        continue
                    if week_start <= pd <= week_ending_date:
                        week_likes += post.get('like_count', 0)
                        week_comments += post.get('comments_count', 0)
                        week_posts += 1
                total_interactions = week_likes + week_comments
                if ig_followers > 0 and week_posts > 0:
                    ig_engagement_rate = round(
                        total_interactions / (ig_followers * week_posts), 4
                    )
                logger.info(f"IG week: {week_posts} posts, {week_likes} likes, "
                            f"{week_comments} comments, ER={ig_engagement_rate}")
        except Exception as e:
            logger.error(f"Instagram collection failed: {e}")
            ig_followers = prev_snap_social.get('ig_followers', 0)

    # ── Top Performing Posts (Meta API) ─────────────────────────────────
    fb_top_posts = _get_fb_top_posts(week_start, week_ending_date)
    if fb_top_posts:
        logger.info(f"FB top posts: {len(fb_top_posts)} collected")
    ig_top_posts = _get_ig_top_posts(week_start, week_ending_date)
    if ig_top_posts:
        logger.info(f"IG top posts: {len(ig_top_posts)} collected")

    # Aggregate FB weekly metrics from Meta API top posts
    fb_week_likes = sum(p.get('reactions', 0) for p in fb_top_posts)
    fb_week_comments = sum(p.get('comments', 0) for p in fb_top_posts)
    fb_week_shares = sum(p.get('shares', 0) for p in fb_top_posts)

    # Aggregate IG weekly metrics from Meta API top posts
    ig_week_likes = sum(p.get('likes', 0) for p in ig_top_posts)
    ig_week_comments = sum(p.get('comments', 0) for p in ig_top_posts)

    # ── Google Sheet fallback/supplement ──────────────────────────────────
    # Always try the sheet — it has richer data (views, reach, clicks) than
    # the Meta API, and works even when the Meta token is expired.
    sheet_data = _load_sheet_social_data(week_start, week_ending_date)

    li_week_posts = []
    fb_week_views = 0
    fb_week_reach = 0
    fb_week_clicks = 0
    fb_week_posts_count = 0
    ig_week_posts_count = 0

    if sheet_data:
        # FB: use sheet data if Meta API returned nothing or sheet has more
        fb_week_views = sheet_data.get('fb_week_views', 0)
        fb_week_reach = sheet_data.get('fb_week_reach', 0)
        fb_week_clicks = sheet_data.get('fb_week_clicks', 0)
        fb_week_posts_count = sheet_data.get('fb_week_posts_count', 0)
        ig_week_posts_count = sheet_data.get('ig_week_posts_count', 0)

        if not fb_top_posts and sheet_data.get('fb_top_posts'):
            fb_top_posts = sheet_data['fb_top_posts']
            fb_week_likes = sheet_data.get('fb_week_likes', 0)
            fb_week_comments = sheet_data.get('fb_week_comments', 0)
            fb_week_shares = sheet_data.get('fb_week_shares', 0)
            logger.info(f"FB: using sheet data ({fb_week_posts_count} posts, "
                        f"reach={fb_week_reach:,})")

        if not ig_top_posts and sheet_data.get('ig_top_posts'):
            ig_top_posts = sheet_data['ig_top_posts']
            ig_week_likes = sheet_data.get('ig_week_likes', 0)
            ig_week_comments = sheet_data.get('ig_week_comments', 0)
            logger.info(f"IG: using sheet data ({ig_week_posts_count} posts)")

        # LinkedIn — only from sheet (no Meta API source)
        li_week_posts = sheet_data.get('li_week_posts', [])

        # If Meta API gave us followers=0, use sheet reach as a proxy for activity
        if fb_followers == 0 and fb_week_reach > 0:
            # Carry forward followers from previous snapshot
            fb_followers = prev_snap_social.get('fb_followers', 0)
            fb_reach = fb_week_reach
            if fb_followers > 0 and fb_week_posts_count > 0:
                total_eng = fb_week_likes + fb_week_comments + fb_week_shares
                fb_engagement_rate = round(total_eng / fb_followers, 4)
            logger.info(f"FB: carried forward {fb_followers:,} followers, "
                        f"sheet reach={fb_week_reach:,}")

        if ig_followers == 0:
            ig_followers = prev_snap_social.get('ig_followers', 0)
            if ig_followers:
                logger.info(f"IG: carried forward {ig_followers:,} followers")

    elif not fb_top_posts and prev_snap_social:
        # No sheet and no Meta — carry forward from previous snapshot
        fb_followers = prev_snap_social.get('fb_followers', 0)
        ig_followers = prev_snap_social.get('ig_followers', 0)

    # ── Assemble results ─────────────────────────────────────────────────
    result = {
        'week_ending_date': str(week_ending_date),
        'yt_subscribers': yt_subscribers,
        'yt_sub_growth': yt_sub_growth,
        'yt_views': yt_views,
        'yt_watch_hours': yt_watch_hours,
        'yt_new_videos': yt_new_videos,
        'yt_comments': yt_comments,
        'yt_total_comments': yt_total_comments,
        'yt_avg_watch_min': round(yt_avg_watch_min, 1),
        'fb_followers': fb_followers,
        'fb_follower_growth': fb_follower_growth,
        'fb_reach': fb_reach,
        'fb_engagement_rate': round(fb_engagement_rate, 4),
        'fb_messages': fb_messages,
        'fb_week_likes': fb_week_likes,
        'fb_week_comments': fb_week_comments,
        'fb_week_shares': fb_week_shares,
        'fb_week_views': fb_week_views,
        'fb_week_reach': fb_week_reach,
        'fb_week_clicks': fb_week_clicks,
        'fb_week_posts_count': fb_week_posts_count,
        'ig_followers': ig_followers,
        'ig_follower_growth': ig_follower_growth,
        'ig_engagement_rate': round(ig_engagement_rate, 4),
        'ig_week_likes': ig_week_likes,
        'ig_week_comments': ig_week_comments,
        'ig_week_posts_count': ig_week_posts_count,
        'ig_story_views': ig_story_views,
        'ig_dms': ig_dms,
        'yt_total_views': yt_total_views,
        'yt_recent_videos_json': json.dumps(yt_recent_videos),
        'fb_top_posts_json': json.dumps(fb_top_posts),
        'ig_top_posts_json': json.dumps(ig_top_posts),
        'li_top_posts_json': json.dumps(li_week_posts),
    }

    logger.info(f"Social collection complete: YT={yt_subscribers:,} subs, "
                f"FB={fb_followers:,} followers, IG={ig_followers:,} followers")

    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2))
