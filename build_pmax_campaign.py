#!/usr/bin/env python3
"""
Build Performance Max campaign for Dr. Ruth Roberts.
Creates in PAUSED state for manual review before enabling.

Usage: python build_pmax_campaign.py
"""

import os
import sys
import uuid
import traceback
from datetime import date, datetime

# ---- Load .env ----
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    env = {}
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, _, v = line.partition('=')
                    env[k.strip()] = v.strip()
    return env


ENV = load_env()
LOG_LINES = []
LOG_PATH = os.path.join(os.path.dirname(__file__), 'pmax_campaign_build_log.txt')


def log(msg):
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}"
    print(line)
    LOG_LINES.append(line)


def flush_log():
    with open(LOG_PATH, 'w') as f:
        f.write('\n'.join(LOG_LINES) + '\n')


def die(msg, exc=None):
    log(f"FATAL: {msg}")
    if exc is not None:
        log(f"Exception: {exc!r}")
        log(traceback.format_exc())
    flush_log()
    sys.exit(1)


# ---- Google Ads client ----
try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError as e:
    die("google-ads library not installed", e)


CUSTOMER_ID = "6868436418"
LOGIN_CUSTOMER_ID = "8693027103"
MERCHANT_CENTER_ID = 127861877
FINAL_URL = "https://drruthroberts.com"

CAMPAIGN_NAME = "Sales-PMax-2026-04-Relaunch"
BUDGET_NAME = "Sales-PMax-2026-04-Relaunch Budget"
ASSET_GROUP_NAME = "Hero Products \u2014 Wellness Collection"
AUDIENCE_NAME = "Pet Parent Signal"

DAILY_BUDGET_MICROS = 75 * 1_000_000  # $75
TARGET_ROAS = 4.0

SHORT_HEADLINES = [
    "Holistic Pet Wellness",
    "Trusted by Pet Parents",
    "Dr. Ruth Roberts, DVM",
    "30 Years in Pet Health",
    "Whole-Food Pet Formulas",
    "Premium Pet Supplements",
    "Quality Pet Supplements",
    "Holistic, Vet-Formulated",
    "Real Ingredients, Real Care",
    "For Every Life Stage",
    "Natural Support for Pets",
    "Pet Wellness Done Right",
    "Shop the Collection",
    "Vet-Formulated Support",
    "Daily Pet Wellness",
]

LONG_HEADLINES = [
    "Whole-Food Pet Supplements Formulated by a Holistic Vet with 30+ Years Experience",
    "Premium Pet Wellness from Dr. Ruth Roberts, DVM. Shop the Full Collection",
    "Support Your Pet's Vitality with Thoughtfully Formulated Holistic Supplements",
    "Trusted by Thousands of Pet Parents. Real Ingredients. Real Science.",
    "Holistic Pet Care from a DVM. Supplements for Every Life Stage.",
]

DESCRIPTIONS = [
    "Whole-food formulas designed by a holistic vet. Support your pet's vitality today.",
    "Quality supplements for every life stage. Backed by 30 years of veterinary experience.",
    "Real ingredients, real science, real results. Shop premium pet wellness today.",
    "Trusted by pet parents who want more than a basic multivitamin. Shop now.",
    "Holistic pet supplements from Dr. Ruth Roberts, DVM. Shop the collection.",
]

BUSINESS_NAME = "Dr. Ruth Roberts"

URL_EXCLUSIONS = ["/blog/", "/policies/", "/pages/about"]

USER_LIST_NAMES_TO_INCLUDE = [
    "Purchasers of DrRuthRoberts.com - Shopify - GA4",
    "All Users of DrRuthRoberts.com - Shopify - GA4",
    "Shopify Customer Match",
]


def build_client():
    cfg = {
        "developer_token": ENV.get("GOOGLE_ADS_DEVELOPER_TOKEN", ""),
        "client_id": ENV.get("GOOGLE_ADS_CLIENT_ID", ""),
        "client_secret": ENV.get("GOOGLE_ADS_CLIENT_SECRET", ""),
        "refresh_token": ENV.get("GOOGLE_ADS_REFRESH_TOKEN", ""),
        "login_customer_id": LOGIN_CUSTOMER_ID,
        "use_proto_plus": True,
    }
    missing = [k for k in ("developer_token","client_id","client_secret","refresh_token") if not cfg[k]]
    if missing:
        die(f"Missing env keys: {missing}")
    try:
        return GoogleAdsClient.load_from_dict(cfg, version="v23")
    except Exception as e:
        die("Failed to construct GoogleAdsClient", e)


def print_gae(ex: "GoogleAdsException"):
    log(f"GoogleAdsException request_id={ex.request_id} error_code={ex.error.code().name}")
    for err in ex.failure.errors:
        path = ""
        if err.location and err.location.field_path_elements:
            path = ".".join([f"{el.field_name}" + (f"[{el.index}]" if hasattr(el, 'index') and str(el.index) else "") for el in err.location.field_path_elements])
        log(f"  - {err.error_code}: {err.message} (at {path})")


# ------- temp resource ids (negative) -------
_TEMP = {"n": -1}
def temp_id():
    i = _TEMP["n"]
    _TEMP["n"] -= 1
    return i


def lookup_user_list_ids(client, customer_id, names):
    """Return dict name -> resource_name for names that exist."""
    gs = client.get_service("GoogleAdsService")
    # Escape names for query
    quoted = ",".join([f"'{n.replace(chr(39), chr(92)+chr(39))}'" for n in names])
    query = f"""
        SELECT user_list.resource_name, user_list.name
        FROM user_list
        WHERE user_list.name IN ({quoted})
    """
    out = {}
    try:
        stream = gs.search_stream(customer_id=customer_id, query=query)
        for batch in stream:
            for row in batch.results:
                out[row.user_list.name] = row.user_list.resource_name
    except GoogleAdsException as ex:
        log("WARN: user_list lookup failed (continuing without all lists)")
        print_gae(ex)
    return out


def main():
    log("=" * 60)
    log("Performance Max Campaign Build Starting")
    log(f"Customer: {CUSTOMER_ID}  Login: {LOGIN_CUSTOMER_ID}")
    log("=" * 60)

    client = build_client()
    log("Client constructed (v23)")

    gs = client.get_service("GoogleAdsService")

    # Resource name helpers
    budget_rn_tmp = gs.campaign_budget_path(CUSTOMER_ID, temp_id())
    campaign_tmp_id = temp_id()
    campaign_rn_tmp = gs.campaign_path(CUSTOMER_ID, campaign_tmp_id)
    asset_group_tmp_id = temp_id()
    asset_group_rn_tmp = gs.asset_group_path(CUSTOMER_ID, asset_group_tmp_id)

    # Separate buckets; concatenated at the end in the order the API expects:
    # budget -> campaign -> criteria -> assets -> asset_group -> links -> campaign_assets -> audience -> signal
    ops_pre = []          # budget + campaign + criteria
    ops_assets = []       # all asset_operation creates (text + image + cta)
    ops_asset_group = []  # single asset_group create
    ops_links = []        # asset_group_asset links
    ops_camp_assets = []  # campaign_asset links (brand guidelines)
    ops_audience = []     # audience + signal
    operations = ops_pre  # default pointer used below

    # ------- 1) Campaign Budget -------
    budget_op = client.get_type("MutateOperation")
    b = budget_op.campaign_budget_operation.create
    b.resource_name = budget_rn_tmp
    b.name = BUDGET_NAME
    b.amount_micros = DAILY_BUDGET_MICROS
    b.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    # PMax budgets cannot be shared
    b.explicitly_shared = False
    operations.append(budget_op)

    # ------- 2) Campaign -------
    camp_op = client.get_type("MutateOperation")
    c = camp_op.campaign_operation.create
    c.resource_name = campaign_rn_tmp
    c.name = CAMPAIGN_NAME
    c.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.PERFORMANCE_MAX
    c.status = client.enums.CampaignStatusEnum.PAUSED
    c.campaign_budget = budget_rn_tmp
    # Bidding: MaximizeConversionValue with target ROAS
    c.maximize_conversion_value.target_roas = TARGET_ROAS
    # Shopping settings (retail / Merchant Center)
    c.shopping_setting.merchant_id = MERCHANT_CENTER_ID
    c.shopping_setting.feed_label = "US"
    # URL expansion is ON by default for PMax in v23 (no longer settable on Campaign).
    # URL exclusions handled below via negative webpage criteria.
    # Start date = today, no end date
    c.start_date_time = date.today().strftime("%Y-%m-%d 00:00:00")
    # Geo targeting type: PRESENCE
    c.geo_target_type_setting.positive_geo_target_type = (
        client.enums.PositiveGeoTargetTypeEnum.PRESENCE
    )
    c.geo_target_type_setting.negative_geo_target_type = (
        client.enums.NegativeGeoTargetTypeEnum.PRESENCE
    )
    # EU political advertising declaration (required by API for new campaigns)
    c.contains_eu_political_advertising = (
        client.enums.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
    )
    operations.append(camp_op)

    # ------- 3) Campaign Criterion: Geo (US) -------
    geo_op = client.get_type("MutateOperation")
    gc = geo_op.campaign_criterion_operation.create
    gc.campaign = campaign_rn_tmp
    gc.location.geo_target_constant = gs.geo_target_constant_path("2840")
    operations.append(geo_op)

    # ------- 4) Campaign Criterion: Language (English) -------
    lang_op = client.get_type("MutateOperation")
    lc = lang_op.campaign_criterion_operation.create
    lc.campaign = campaign_rn_tmp
    lc.language.language_constant = gs.language_constant_path("1000")
    operations.append(lang_op)

    # ------- 5) URL exclusion list (via AssetSet / campaign-level) -------
    # Simpler: use campaign-level brand_list? The supported mechanism for URL
    # exclusions in PMax is the "excluded_parent_asset_field_types" & URL rules
    # via CustomerAsset/AssetGroupSignal is not it.
    # Correct API: Campaign.url_expansion_opt_out=False +
    # there's no direct URL exclusion field on PMax campaign via the basic API.
    # URL exclusions are set via Campaign-level using
    # "excluded_parent_asset_field_types" -- no that's for field types.
    # Actual mechanism: use "campaign criterion" with WebpageInfo for NEGATIVE.
    for idx, pattern in enumerate(URL_EXCLUSIONS):
        excl_op = client.get_type("MutateOperation")
        ec = excl_op.campaign_criterion_operation.create
        ec.campaign = campaign_rn_tmp
        ec.negative = True
        ec.webpage.criterion_name = f"Exclude {pattern}"
        cond = client.get_type("WebpageConditionInfo")
        cond.operand = client.enums.WebpageConditionOperandEnum.URL
        cond.operator = client.enums.WebpageConditionOperatorEnum.CONTAINS
        cond.argument = pattern
        ec.webpage.conditions.append(cond)
        operations.append(excl_op)

    # ------- 6) Asset Group (goes in its own bucket; emitted AFTER assets) -------
    ag_op = client.get_type("MutateOperation")
    ag = ag_op.asset_group_operation.create
    ag.resource_name = asset_group_rn_tmp
    ag.campaign = campaign_rn_tmp
    ag.name = ASSET_GROUP_NAME
    ag.final_urls.append(FINAL_URL)
    ag.status = client.enums.AssetGroupStatusEnum.PAUSED
    ops_asset_group.append(ag_op)

    # ------- 7) Create text Assets + link to Asset Group -------
    AssetFieldType = client.enums.AssetFieldTypeEnum

    def add_text_asset(text, field_type_enum):
        asset_tmp = temp_id()
        asset_rn = gs.asset_path(CUSTOMER_ID, asset_tmp)
        aop = client.get_type("MutateOperation")
        a = aop.asset_operation.create
        a.resource_name = asset_rn
        a.text_asset.text = text
        ops_assets.append(aop)

        lop = client.get_type("MutateOperation")
        la = lop.asset_group_asset_operation.create
        la.asset_group = asset_group_rn_tmp
        la.asset = asset_rn
        la.field_type = field_type_enum
        ops_links.append(lop)
        return asset_rn

    short_rns = [add_text_asset(t, AssetFieldType.HEADLINE) for t in SHORT_HEADLINES]
    long_rns = [add_text_asset(t, AssetFieldType.LONG_HEADLINE) for t in LONG_HEADLINES]
    desc_rns = [add_text_asset(t, AssetFieldType.DESCRIPTION) for t in DESCRIPTIONS]

    # Business name asset (text asset with field_type=BUSINESS_NAME)
    bn_tmp = temp_id()
    bn_rn = gs.asset_path(CUSTOMER_ID, bn_tmp)
    bn_op = client.get_type("MutateOperation")
    bna = bn_op.asset_operation.create
    bna.resource_name = bn_rn
    bna.text_asset.text = BUSINESS_NAME
    ops_assets.append(bn_op)

    # Link as CampaignAsset (Brand Guidelines requires campaign-level only)
    bn_camp_op = client.get_type("MutateOperation")
    bncl = bn_camp_op.campaign_asset_operation.create
    bncl.campaign = campaign_rn_tmp
    bncl.asset = bn_rn
    bncl.field_type = AssetFieldType.BUSINESS_NAME
    ops_camp_assets.append(bn_camp_op)

    # Image assets (placeholders; user will replace in UI)
    def add_image_asset(img_path, field_type_enum, name):
        with open(img_path, "rb") as f:
            img_bytes = f.read()
        asset_tmp = temp_id()
        asset_rn = gs.asset_path(CUSTOMER_ID, asset_tmp)
        aop = client.get_type("MutateOperation")
        a = aop.asset_operation.create
        a.resource_name = asset_rn
        a.name = name
        a.type_ = client.enums.AssetTypeEnum.IMAGE
        a.image_asset.data = img_bytes
        ops_assets.append(aop)

        lop = client.get_type("MutateOperation")
        la = lop.asset_group_asset_operation.create
        la.asset_group = asset_group_rn_tmp
        la.asset = asset_rn
        la.field_type = field_type_enum
        ops_links.append(lop)
        return asset_rn

    here = os.path.dirname(__file__) or "."
    add_image_asset(os.path.join(here, "pmax_landscape.png"),
                    AssetFieldType.MARKETING_IMAGE, "DRR Placeholder Landscape")
    add_image_asset(os.path.join(here, "pmax_square.png"),
                    AssetFieldType.SQUARE_MARKETING_IMAGE, "DRR Placeholder Square")
    # Logo: create asset only; link to Campaign (not AssetGroup) per Brand Guidelines
    with open(os.path.join(here, "pmax_logo.png"), "rb") as f:
        logo_bytes = f.read()
    logo_tmp = temp_id()
    logo_rn = gs.asset_path(CUSTOMER_ID, logo_tmp)
    logo_asset_op = client.get_type("MutateOperation")
    la_a = logo_asset_op.asset_operation.create
    la_a.resource_name = logo_rn
    la_a.name = "DRR Placeholder Logo"
    la_a.type_ = client.enums.AssetTypeEnum.IMAGE
    la_a.image_asset.data = logo_bytes
    ops_assets.append(logo_asset_op)

    logo_camp_op = client.get_type("MutateOperation")
    lcl = logo_camp_op.campaign_asset_operation.create
    lcl.campaign = campaign_rn_tmp
    lcl.asset = logo_rn
    lcl.field_type = AssetFieldType.LOGO
    ops_camp_assets.append(logo_camp_op)

    # Call to action asset (SHOP_NOW)
    cta_tmp = temp_id()
    cta_rn = gs.asset_path(CUSTOMER_ID, cta_tmp)
    cta_op = client.get_type("MutateOperation")
    ctaa = cta_op.asset_operation.create
    ctaa.resource_name = cta_rn
    ctaa.call_to_action_asset.call_to_action = (
        client.enums.CallToActionTypeEnum.SHOP_NOW
    )
    ops_assets.append(cta_op)

    cta_link_op = client.get_type("MutateOperation")
    ctal = cta_link_op.asset_group_asset_operation.create
    ctal.asset_group = asset_group_rn_tmp
    ctal.asset = cta_rn
    ctal.field_type = AssetFieldType.CALL_TO_ACTION_SELECTION
    ops_links.append(cta_link_op)

    # ------- 8) Audience Signal -------
    # Lookup user lists by name first
    user_list_map = lookup_user_list_ids(client, CUSTOMER_ID, USER_LIST_NAMES_TO_INCLUDE)
    found_lists = [user_list_map[n] for n in USER_LIST_NAMES_TO_INCLUDE if n in user_list_map]
    missing_lists = [n for n in USER_LIST_NAMES_TO_INCLUDE if n not in user_list_map]
    if missing_lists:
        log(f"NOTE: user lists not found (skipped): {missing_lists}")
    log(f"User lists to include in audience signal: {len(found_lists)}")

    if found_lists:
        sig_op = client.get_type("MutateOperation")
        sig = sig_op.asset_group_signal_operation.create
        sig.asset_group = asset_group_rn_tmp
        # Audience signal: build an audience inline via 'audience' field
        # v23 uses AssetGroupSignal.audience (AudienceInfo) OR .search_theme
        # AudienceInfo takes a single audience resource name — for ad-hoc
        # signal we need to create an Audience first.
        # Simpler approach: create Audience resource with user-list dimension,
        # then reference it here. Do that before this operation.
        sig_op = None  # placeholder, we'll insert below

    # Build Audience (create) then AssetGroupSignal referencing it
    audience_rn = None
    if found_lists:
        audience_tmp = temp_id()
        audience_rn = gs.audience_path(CUSTOMER_ID, audience_tmp)
        aud_op = client.get_type("MutateOperation")
        aud = aud_op.audience_operation.create
        aud.resource_name = audience_rn
        aud.name = AUDIENCE_NAME + " " + uuid.uuid4().hex[:6]
        aud.description = "Audience signal for Sales-PMax-2026-04-Relaunch"
        # Add user list dimension
        dim = client.get_type("AudienceDimension")
        for ul_rn in found_lists:
            ul_entry = client.get_type("AudienceSegment")
            ul_entry.user_list.user_list = ul_rn
            dim.audience_segments.segments.append(ul_entry)
        aud.dimensions.append(dim)
        ops_audience.append(aud_op)

        # AssetGroupSignal -> audience
        sig_op = client.get_type("MutateOperation")
        sig = sig_op.asset_group_signal_operation.create
        sig.asset_group = asset_group_rn_tmp
        sig.audience.audience = audience_rn
        ops_audience.append(sig_op)

    # ------- 9) Concatenate in correct order for validation -------
    # assets must exist before asset_group so links can resolve field counts.
    operations = (
        ops_pre
        + ops_assets
        + ops_asset_group
        + ops_links
        + ops_camp_assets
        + ops_audience
    )

    # ------- 10) Submit as single mutate -------
    log(f"Submitting {len(operations)} operations in single atomic mutate...")
    try:
        response = gs.mutate(
            customer_id=CUSTOMER_ID,
            mutate_operations=operations,
        )
    except GoogleAdsException as ex:
        log("Mutate failed with GoogleAdsException:")
        print_gae(ex)
        die("Aborting, no partial state committed (atomic mutate).")
    except Exception as e:
        die("Unexpected error during mutate", e)

    # Parse results
    log("Mutate succeeded. Resource names created:")
    created_campaign_rn = None
    created_asset_group_rn = None
    created_budget_rn = None
    asset_rns = []
    other_rns = []
    for i, r in enumerate(response.mutate_operation_responses):
        kind = r._pb.WhichOneof("response")
        if kind is None:
            continue
        sub = getattr(r, kind)
        rn = getattr(sub, "resource_name", "")
        log(f"  [{i}] {kind}: {rn}")
        if kind == "campaign_result":
            created_campaign_rn = rn
        elif kind == "asset_group_result":
            created_asset_group_rn = rn
        elif kind == "campaign_budget_result":
            created_budget_rn = rn
        elif kind == "asset_result":
            asset_rns.append(rn)
        else:
            other_rns.append((kind, rn))

    # Extract campaign numeric ID for UI URL
    camp_id = created_campaign_rn.split("/")[-1] if created_campaign_rn else "?"
    ui_url = (
        f"https://ads.google.com/aw/campaigns?ocid=&campaignId={camp_id}"
        f"&__u={CUSTOMER_ID}"
    )

    log("")
    log("=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log(f"Budget:        {created_budget_rn}")
    log(f"Campaign:      {created_campaign_rn} (PAUSED)")
    log(f"Asset Group:   {created_asset_group_rn}")
    log(f"Assets:        {len(asset_rns)} created")
    log(f"Other ops:     {len(other_rns)}")
    log(f"UI URL (approx): https://ads.google.com/aw/campaigns?campaignId={camp_id}")
    log("")
    log("Status: Campaign created in PAUSED status. Review in UI before enabling.")
    log("Manual UI steps remaining:")
    log("  - Upload brand images (headshot, logo) to asset group")
    log("  - Optionally upload videos")
    log("  - Verify Merchant Center product feed is linked")
    log("  - Review conversion goal (account default = Purchase)")
    log("  - Final review, then enable the campaign")
    flush_log()


if __name__ == "__main__":
    main()
