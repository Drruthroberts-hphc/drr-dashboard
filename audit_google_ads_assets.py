#!/usr/bin/env python3
"""
Audit ALL existing assets in Dr. Ruth Roberts' Google Ads account.
Writes full inventory to google_ads_asset_audit.json and prints a summary.
"""

import json
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(__file__))

from config import (
    GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID,
    GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN,
    GOOGLE_ADS_CUSTOMER_ID, GOOGLE_ADS_LOGIN_CUSTOMER_ID,
)
from google.ads.googleads.client import GoogleAdsClient


def build_client():
    cfg = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": GOOGLE_ADS_CLIENT_ID,
        "client_secret": GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        cfg["login_customer_id"] = GOOGLE_ADS_LOGIN_CUSTOMER_ID
    return GoogleAdsClient.load_from_dict(cfg)


def safe_get(obj, *path):
    cur = obj
    for p in path:
        try:
            cur = getattr(cur, p)
        except Exception:
            return None
    return cur


def main():
    client = build_client()
    ga = client.get_service("GoogleAdsService")
    customer_id = GOOGLE_ADS_CUSTOMER_ID

    query = """
        SELECT
            asset.id,
            asset.name,
            asset.type,
            asset.image_asset.full_size.url,
            asset.image_asset.full_size.width_pixels,
            asset.image_asset.full_size.height_pixels,
            asset.youtube_video_asset.youtube_video_id,
            asset.youtube_video_asset.youtube_video_title,
            asset.text_asset.text,
            asset.callout_asset.callout_text,
            asset.sitelink_asset.link_text,
            asset.sitelink_asset.description1,
            asset.sitelink_asset.description2,
            asset.final_urls
        FROM asset
    """

    all_assets = []
    stream = ga.search_stream(customer_id=customer_id, query=query)
    for batch in stream:
        for row in batch.results:
            a = row.asset
            t = a.type_.name if hasattr(a.type_, "name") else str(a.type_)
            rec = {
                "id": a.id,
                "name": a.name,
                "type": t,
            }
            try:
                if a.image_asset.full_size.url:
                    rec["image"] = {
                        "url": a.image_asset.full_size.url,
                        "width": a.image_asset.full_size.width_pixels,
                        "height": a.image_asset.full_size.height_pixels,
                    }
            except Exception:
                pass
            try:
                if a.youtube_video_asset.youtube_video_id:
                    rec["youtube"] = {
                        "video_id": a.youtube_video_asset.youtube_video_id,
                        "title": a.youtube_video_asset.youtube_video_title,
                    }
            except Exception:
                pass
            try:
                if a.text_asset.text:
                    rec["text"] = a.text_asset.text
            except Exception:
                pass
            try:
                if a.callout_asset.callout_text:
                    rec["callout"] = a.callout_asset.callout_text
            except Exception:
                pass
            try:
                if a.sitelink_asset.link_text:
                    rec["sitelink"] = {
                        "link_text": a.sitelink_asset.link_text,
                        "description1": a.sitelink_asset.description1,
                        "description2": a.sitelink_asset.description2,
                    }
            except Exception:
                pass
            try:
                if a.final_urls:
                    rec["final_urls"] = list(a.final_urls)
            except Exception:
                pass
            all_assets.append(rec)

    # Summary
    type_counts = Counter(a["type"] for a in all_assets)
    images = [a for a in all_assets if a["type"] == "IMAGE" and "image" in a]
    videos = [a for a in all_assets if a["type"] == "YOUTUBE_VIDEO" and "youtube" in a]
    texts = [a for a in all_assets if a["type"] == "TEXT" and "text" in a]
    callouts = [a for a in all_assets if a["type"] == "CALLOUT" and "callout" in a]
    sitelinks = [a for a in all_assets if a["type"] == "SITELINK"]

    # Image dimensions
    dims = Counter((a["image"]["width"], a["image"]["height"]) for a in images)

    # Text length buckets (headlines ~30, long headlines ~90, descriptions ~90)
    text_buckets = {"short_headline_<=30": [], "long_headline_31_90": [], "description_>90": []}
    for a in texts:
        n = len(a["text"])
        if n <= 30:
            text_buckets["short_headline_<=30"].append(a["text"])
        elif n <= 90:
            text_buckets["long_headline_31_90"].append(a["text"])
        else:
            text_buckets["description_>90"].append(a["text"])

    summary = {
        "total_assets": len(all_assets),
        "by_type": dict(type_counts),
        "images": {
            "count": len(images),
            "unique_dimensions": [f"{w}x{h} ({c})" for (w, h), c in dims.most_common()],
        },
        "videos": [{"id": v["youtube"]["video_id"], "title": v["youtube"]["title"], "name": v["name"]} for v in videos],
        "text_buckets": {k: len(v) for k, v in text_buckets.items()},
        "text_samples": {k: v[:15] for k, v in text_buckets.items()},
        "callouts": [c["callout"] for c in callouts],
        "sitelinks": [
            {
                "link_text": s.get("sitelink", {}).get("link_text"),
                "urls": s.get("final_urls", []),
                "desc1": s.get("sitelink", {}).get("description1"),
                "desc2": s.get("sitelink", {}).get("description2"),
            }
            for s in sitelinks
        ],
    }

    out_path = os.path.join(os.path.dirname(__file__), "google_ads_asset_audit.json")
    with open(out_path, "w") as f:
        json.dump({"summary": summary, "assets": all_assets}, f, indent=2, default=str)

    print("=" * 70)
    print("GOOGLE ADS ASSET AUDIT — Dr. Ruth Roberts")
    print("=" * 70)
    print(f"Total assets: {summary['total_assets']}")
    print(f"By type: {json.dumps(summary['by_type'], indent=2)}")
    print()
    print(f"IMAGES: {summary['images']['count']}")
    for d in summary["images"]["unique_dimensions"][:20]:
        print(f"  - {d}")
    print()
    print(f"YOUTUBE VIDEOS: {len(videos)}")
    for v in summary["videos"]:
        print(f"  - {v['id']}: {v['title']} (name: {v['name']})")
    print()
    print(f"TEXT ASSETS: {len(texts)}")
    for k, c in summary["text_buckets"].items():
        print(f"  {k}: {c}")
    print()
    print("SHORT HEADLINE samples:")
    for s in summary["text_samples"]["short_headline_<=30"][:15]:
        print(f"  - {s}")
    print()
    print("LONG HEADLINE samples:")
    for s in summary["text_samples"]["long_headline_31_90"][:10]:
        print(f"  - {s}")
    print()
    print("DESCRIPTION samples:")
    for s in summary["text_samples"]["description_>90"][:10]:
        print(f"  - {s}")
    print()
    print(f"CALLOUTS: {len(callouts)}")
    for c in summary["callouts"]:
        print(f"  - {c}")
    print()
    print(f"SITELINKS: {len(sitelinks)}")
    for s in summary["sitelinks"]:
        print(f"  - {s['link_text']} -> {s['urls']}")
        if s["desc1"] or s["desc2"]:
            print(f"      {s['desc1']} / {s['desc2']}")
    print()
    print(f"Saved full audit to: {out_path}")


if __name__ == "__main__":
    main()
