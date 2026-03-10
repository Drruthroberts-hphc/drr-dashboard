"""
Social Media Data Collector
============================
Collects YouTube channel metrics via the YouTube Data API v3.
Facebook and Instagram metrics are Phase 2 (placeholders included).
"""

import json
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

from config import YOUTUBE_API_KEY, YOUTUBE_CHANNEL_ID

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

    # ── Facebook (Phase 2 - placeholders) ────────────────────────────────
    fb_followers = 0
    fb_follower_growth = 0
    fb_reach = 0
    fb_engagement_rate = 0.0
    fb_messages = 0

    # ── Instagram (Phase 2 - placeholders) ───────────────────────────────
    ig_followers = 0
    ig_follower_growth = 0
    ig_engagement_rate = 0.0
    ig_story_views = 0
    ig_dms = 0

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

    logger.info(f"Social collection complete: {yt_subscribers} YT subs, "
                f"{yt_new_videos} new videos, {yt_views} views")

    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2))
