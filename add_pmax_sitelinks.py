#!/usr/bin/env python3
"""
Add 5 SITELINK assets to Performance Max campaign Sales-PMax-2026-04-Relaunch.
"""

import os
import sys
import traceback

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    env = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, _, v = line.partition('=')
                env[k.strip()] = v.strip()
    return env


ENV = load_env()

CUSTOMER_ID = "6868436418"
LOGIN_CUSTOMER_ID = "8693027103"
CAMPAIGN_ID = "23781515935"

SITELINKS = [
    {
        "link_text": "Shop HTBS",
        "description1": "Whole-Body Multivitamin for Pets",
        "description2": "One scoop, total support daily",
        "final_urls": ["https://drruthroberts.com/products/holistic-total-body-support-for-cats-and-dogs"],
    },
    {
        "link_text": "Shop Arthri-Pawz",
        "description1": "Multi-Ingredient Joint Formula",
        "description2": "For active and senior pets",
        "final_urls": ["https://drruthroberts.com/products/best-joint-supplement-for-dogs-and-cats"],
    },
    {
        "link_text": "Shop Omega Pawz",
        "description1": "Liquid Omega-3 for Dogs & Cats",
        "description2": "Coat shine and daily vitality",
        "final_urls": ["https://drruthroberts.com/products/omega-pawz-omega-3-oil-for-dogs-cats"],
    },
    {
        "link_text": "Shop Hista Paws",
        "description1": "Quercetin-Based Pet Support",
        "description2": "Natural plant-based comfort",
        "final_urls": ["https://drruthroberts.com/products/herbal-allergy-support-for-cats-and-dogs"],
    },
    {
        "link_text": "Shop Daily Wellness",
        "description1": "Gentle Daily Nutrition",
        "description2": "For pets with food sensitivities",
        "final_urls": ["https://drruthroberts.com/products/pets-daily-wellness-formula-for-pets-with-pork-beef-allergy"],
    },
]


def validate():
    errors = []
    for i, sl in enumerate(SITELINKS, 1):
        if len(sl["link_text"]) > 25:
            errors.append(f"Sitelink {i} link_text too long: {len(sl['link_text'])}")
        if len(sl["description1"]) > 35:
            errors.append(f"Sitelink {i} description1 too long: {len(sl['description1'])}")
        if len(sl["description2"]) > 35:
            errors.append(f"Sitelink {i} description2 too long: {len(sl['description2'])}")
    return errors


def build_client():
    config = {
        "developer_token": ENV["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": ENV["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": ENV["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": ENV["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": LOGIN_CUSTOMER_ID,
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config)


def main():
    errs = validate()
    if errs:
        print("Validation errors:")
        for e in errs:
            print(f"  - {e}")
        sys.exit(1)
    print("All sitelinks pass character limit validation.")

    client = build_client()
    google_ads_service = client.get_service("GoogleAdsService")
    asset_service = client.get_service("AssetService")
    campaign_asset_service = client.get_service("CampaignAssetService")

    campaign_resource = google_ads_service.campaign_path(CUSTOMER_ID, CAMPAIGN_ID)

    # Build asset operations with temp resource names
    asset_ops = []
    temp_resource_names = []
    for i, sl in enumerate(SITELINKS):
        temp_id = -(i + 1)
        temp_rn = f"customers/{CUSTOMER_ID}/assets/{temp_id}"
        temp_resource_names.append(temp_rn)

        op = client.get_type("MutateOperation")
        asset = op.asset_operation.create
        asset.resource_name = temp_rn
        asset.name = f"Sitelink - {sl['link_text']}"
        asset.type_ = client.enums.AssetTypeEnum.SITELINK
        asset.final_urls.extend(sl["final_urls"])
        sitelink = asset.sitelink_asset
        sitelink.link_text = sl["link_text"]
        sitelink.description1 = sl["description1"]
        sitelink.description2 = sl["description2"]
        asset_ops.append(op)

    # CampaignAsset ops referring to temp asset resource names
    for temp_rn in temp_resource_names:
        op = client.get_type("MutateOperation")
        ca = op.campaign_asset_operation.create
        ca.campaign = campaign_resource
        ca.asset = temp_rn
        ca.field_type = client.enums.AssetFieldTypeEnum.SITELINK
        asset_ops.append(op)

    try:
        response = google_ads_service.mutate(
            customer_id=CUSTOMER_ID,
            mutate_operations=asset_ops,
        )
    except GoogleAdsException as ex:
        print(f"\nGoogleAdsException request_id={ex.request_id}")
        for err in ex.failure.errors:
            print(f"  Error: {err.error_code}")
            print(f"  Message: {err.message}")
            if err.location:
                for fpe in err.location.field_path_elements:
                    print(f"    at {fpe.field_name}[{fpe.index}]")
        sys.exit(1)

    print("\nAtomic mutate succeeded. Created resources:")
    created_asset_rns = []
    created_campaign_asset_rns = []
    for r in response.mutate_operation_responses:
        which = r._pb.WhichOneof("response")
        if which == "asset_result":
            rn = r.asset_result.resource_name
            created_asset_rns.append(rn)
            print(f"  ASSET:          {rn}")
        elif which == "campaign_asset_result":
            rn = r.campaign_asset_result.resource_name
            created_campaign_asset_rns.append(rn)
            print(f"  CAMPAIGN_ASSET: {rn}")

    # Verification query
    print("\nVerifying sitelinks linked to campaign...")
    query = f"""
        SELECT
          campaign.id,
          campaign.name,
          asset.id,
          asset.resource_name,
          asset.sitelink_asset.link_text,
          asset.sitelink_asset.description1,
          asset.sitelink_asset.description2,
          asset.final_urls,
          campaign_asset.field_type,
          campaign_asset.status
        FROM campaign_asset
        WHERE campaign.id = {CAMPAIGN_ID}
          AND campaign_asset.field_type = 'SITELINK'
    """
    rows = google_ads_service.search(customer_id=CUSTOMER_ID, query=query)
    count = 0
    for row in rows:
        count += 1
        a = row.asset
        print(f"  [{count}] {a.sitelink_asset.link_text}")
        print(f"       asset_id={a.id} status={row.campaign_asset.status.name}")
        print(f"       desc1={a.sitelink_asset.description1}")
        print(f"       desc2={a.sitelink_asset.description2}")
        if a.final_urls:
            print(f"       url={a.final_urls[0]}")

    print(f"\nTotal sitelinks linked to campaign {CAMPAIGN_ID}: {count}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e!r}")
        traceback.print_exc()
        sys.exit(1)
