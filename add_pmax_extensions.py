#!/usr/bin/env python3
"""
Add 6 CALLOUT assets and 1 STRUCTURED_SNIPPET asset to
Performance Max campaign Sales-PMax-2026-04-Relaunch (ID 23781515935).
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

CALLOUTS = [
    "Vet-Formulated",
    "30+ Years Experience",
    "Holistic Whole Foods",
    "Free Shipping $75+",
    "Trusted by Pet Parents",
    "Real Ingredients",
]

SNIPPET_HEADER = "Types"
SNIPPET_VALUES = [
    "Multivitamins",
    "Joint Support",
    "Omega-3",
    "Allergy Support",
    "Daily Wellness",
]


def validate():
    errors = []
    for i, c in enumerate(CALLOUTS, 1):
        if len(c) > 25:
            errors.append(f"Callout {i} too long ({len(c)} chars): {c!r}")
    for i, v in enumerate(SNIPPET_VALUES, 1):
        if len(v) > 25:
            errors.append(f"Snippet value {i} too long ({len(v)} chars): {v!r}")
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
    print("All callouts and snippet values pass character limit validation.")

    client = build_client()
    google_ads_service = client.get_service("GoogleAdsService")

    campaign_resource = google_ads_service.campaign_path(CUSTOMER_ID, CAMPAIGN_ID)

    mutate_ops = []
    temp_callout_rns = []

    # Callout assets
    for i, text in enumerate(CALLOUTS):
        temp_id = -(i + 1)
        temp_rn = f"customers/{CUSTOMER_ID}/assets/{temp_id}"
        temp_callout_rns.append(temp_rn)

        op = client.get_type("MutateOperation")
        asset = op.asset_operation.create
        asset.resource_name = temp_rn
        asset.name = f"Callout - {text}"
        asset.type_ = client.enums.AssetTypeEnum.CALLOUT
        asset.callout_asset.callout_text = text
        mutate_ops.append(op)

    # Structured snippet asset
    snippet_temp_id = -(len(CALLOUTS) + 1)
    snippet_temp_rn = f"customers/{CUSTOMER_ID}/assets/{snippet_temp_id}"

    op = client.get_type("MutateOperation")
    asset = op.asset_operation.create
    asset.resource_name = snippet_temp_rn
    asset.name = f"StructuredSnippet - {SNIPPET_HEADER}"
    asset.type_ = client.enums.AssetTypeEnum.STRUCTURED_SNIPPET
    asset.structured_snippet_asset.header = SNIPPET_HEADER
    asset.structured_snippet_asset.values.extend(SNIPPET_VALUES)
    mutate_ops.append(op)

    # CampaignAsset links for callouts
    for temp_rn in temp_callout_rns:
        op = client.get_type("MutateOperation")
        ca = op.campaign_asset_operation.create
        ca.campaign = campaign_resource
        ca.asset = temp_rn
        ca.field_type = client.enums.AssetFieldTypeEnum.CALLOUT
        mutate_ops.append(op)

    # CampaignAsset link for structured snippet
    op = client.get_type("MutateOperation")
    ca = op.campaign_asset_operation.create
    ca.campaign = campaign_resource
    ca.asset = snippet_temp_rn
    ca.field_type = client.enums.AssetFieldTypeEnum.STRUCTURED_SNIPPET
    mutate_ops.append(op)

    try:
        response = google_ads_service.mutate(
            customer_id=CUSTOMER_ID,
            mutate_operations=mutate_ops,
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
    for r in response.mutate_operation_responses:
        which = r._pb.WhichOneof("response")
        if which == "asset_result":
            print(f"  ASSET:          {r.asset_result.resource_name}")
        elif which == "campaign_asset_result":
            print(f"  CAMPAIGN_ASSET: {r.campaign_asset_result.resource_name}")

    # Verification
    print("\nVerifying callouts and snippet linked to campaign...")
    query = f"""
        SELECT
          campaign.id,
          asset.id,
          asset.type,
          asset.callout_asset.callout_text,
          asset.structured_snippet_asset.header,
          asset.structured_snippet_asset.values,
          campaign_asset.field_type,
          campaign_asset.status
        FROM campaign_asset
        WHERE campaign.id = {CAMPAIGN_ID}
          AND campaign_asset.field_type IN ('CALLOUT', 'STRUCTURED_SNIPPET')
    """
    rows = google_ads_service.search(customer_id=CUSTOMER_ID, query=query)
    callout_count = 0
    snippet_count = 0
    for row in rows:
        a = row.asset
        ft = row.campaign_asset.field_type.name
        if ft == "CALLOUT":
            callout_count += 1
            print(f"  CALLOUT   asset_id={a.id} text={a.callout_asset.callout_text!r} status={row.campaign_asset.status.name}")
        elif ft == "STRUCTURED_SNIPPET":
            snippet_count += 1
            vals = list(a.structured_snippet_asset.values)
            print(f"  SNIPPET   asset_id={a.id} header={a.structured_snippet_asset.header!r} values={vals} status={row.campaign_asset.status.name}")

    print(f"\nTotal linked to campaign {CAMPAIGN_ID}: callouts={callout_count}, snippets={snippet_count}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e!r}")
        traceback.print_exc()
        sys.exit(1)
