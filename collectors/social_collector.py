"""
Social Media Data Collector
============================
Collects YouTube channel metrics via the YouTube Data API v3.
Collects Facebook Page and Instagram metrics via the Meta Graph API.
"""

import json
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

from config import (
    YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID,
    META_PAGE_ACCESS_TOKEN, FB_PAGE_ID, IG_ACCOUNT_ID,
)

META_GRAPH_BASE = 'https://graph.facebook.com/v25.0'

logger = logging.getLogger(__name__)

YOUTUBE_BASE = 'https://www.googleapis.com/youtube/v3'


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
    yt_views = 0
    yt_watch_seconds = 0
    yt_comments = 0

    if video_ids:
        videos = _get_video_stats(video_ids)
        for video in videos:
            stats = video.get('statistics', {})
            yt_views += int(stats.get('viewCount', 0))
            yt_comments += int(stats.get('commentCount', 0))

            # Estimate watch time from duration * views (rough approximation)
            duration_s = _parse_duration_seconds(
                video.get('contentDetails', {}).get('duration', '')
            )
            view_count = int(stats.get('viewCount', 0))
            # Assume average 40% watch-through rate
            yt_watch_seconds += duration_s * view_count * 0.4

    yt_watch_hours = round(yt_watch_seconds / 3600, 1)

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
                # Engagement rate = talking_about / followers
                if fb_followers > 0:
                    fb_engagement_rate = round(talking / fb_followers, 4)
                fb_reach = talking  # best available proxy without read_insights
                logger.info(f"FB: {fb_followers:,} followers, {talking} engaged")
            else:
                logger.warning(f"FB page API error: {fb_data}")
        except Exception as e:
            logger.error(f"Facebook collection failed: {e}")

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

    # ── Assemble results ─────────────────────────────────────────────────
    result = {
        'week_ending_date': str(week_ending_date),
        'yt_subscribers': yt_subscribers,
        'yt_sub_growth': yt_sub_growth,
        'yt_views': yt_views,
        'yt_watch_hours': yt_watch_hours,
        'yt_new_videos': yt_new_videos,
        'yt_comments': yt_comments,
        'fb_followers': fb_followers,
        'fb_follower_growth': fb_follower_growth,
        'fb_reach': fb_reach,
        'fb_engagement_rate': round(fb_engagement_rate, 4),
        'fb_messages': fb_messages,
        'ig_followers': ig_followers,
        'ig_follower_growth': ig_follower_growth,
        'ig_engagement_rate': round(ig_engagement_rate, 4),
        'ig_story_views': ig_story_views,
        'ig_dms': ig_dms,
    }

    logger.info(f"Social collection complete: YT={yt_subscribers:,} subs, "
                f"FB={fb_followers:,} followers, IG={ig_followers:,} followers")

    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2))
