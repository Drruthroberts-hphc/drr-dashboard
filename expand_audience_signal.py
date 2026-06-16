#!/usr/bin/env python3
"""
Expand the Audience signal for campaign Sales-PMax-2026-04-Relaunch
by adding two additional user lists to Audience 345895615.

Tries Option A (update Audience.dimensions in place). If that fails
because the field is immutable, falls back to Option B (create new
Audience + new AssetGroupSignal, remove old signal).

Usage: python expand_audience_signal.py
"""

import os
import sys
import traceback

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

try:
    from google.ads.googleads.client import GoogleAdsClient
    from google.ads.googleads.errors import GoogleAdsException
except ImportError as e:
    print(f"FATAL: google-ads not installed: {e}")
    sys.exit(1)


CUSTOMER_ID = "6868436418"
LOGIN_CUSTOMER_ID = "8693027103"
ASSET_GROUP_ID = 6703896280
AUDIENCE_ID = 345895615
EXISTING_SIGNAL_RN = f"customers/{CUSTOMER_ID}/assetGroupSignals/{ASSET_GROUP_ID}~2482839917937"
AUDIENCE_RN = f"customers/{CUSTOMER_ID}/audiences/{AUDIENCE_ID}"

EXISTING_USER_LIST_IDS = [8172773786, 8174063964]
NEW_USER_LIST_IDS = [8955384323, 9371863728]
ALL_USER_LIST_IDS = EXISTING_USER_LIST_IDS + NEW_USER_LIST_IDS


def log(msg):
    print(msg, flush=True)


def build_client():
    cfg = {
        "developer_token": ENV.get("GOOGLE_ADS_DEVELOPER_TOKEN", ""),
        "client_id": ENV.get("GOOGLE_ADS_CLIENT_ID", ""),
        "client_secret": ENV.get("GOOGLE_ADS_CLIENT_SECRET", ""),
        "refresh_token": ENV.get("GOOGLE_ADS_REFRESH_TOKEN", ""),
        "login_customer_id": LOGIN_CUSTOMER_ID,
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(cfg, version="v23")


def print_gae(ex):
    log(f"GoogleAdsException request_id={ex.request_id}")
    for err in ex.failure.errors:
        log(f"  error: {err.error_code}  message: {err.message}")
        if err.location and err.location.field_path_elements:
            for fpe in err.location.field_path_elements:
                log(f"    at field: {fpe.field_name}{('[' + str(fpe.index) + ']') if fpe.index else ''}")


def user_list_rn(uid):
    return f"customers/{CUSTOMER_ID}/userLists/{uid}"


def build_user_list_dimension(client, user_list_ids):
    """Build an AudienceDimension with audience_segments containing user_list segments."""
    dim = client.get_type("AudienceDimension")
    for uid in user_list_ids:
        seg = client.get_type("AudienceSegment")
        seg.user_list.user_list = user_list_rn(uid)
        dim.audience_segments.segments.append(seg)
    return dim


def fetch_current_audience(client):
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT audience.resource_name, audience.id, audience.name,
               audience.description, audience.status, audience.dimensions
        FROM audience
        WHERE audience.id = {AUDIENCE_ID}
    """
    resp = ga_service.search(customer_id=CUSTOMER_ID, query=query)
    for row in resp:
        return row.audience
    return None


def verify(client):
    ga_service = client.get_service("GoogleAdsService")
    query = f"""
        SELECT asset_group_signal.audience.audience,
               asset_group_signal.resource_name
        FROM asset_group_signal
        WHERE asset_group.id = {ASSET_GROUP_ID}
    """
    resp = ga_service.search(customer_id=CUSTOMER_ID, query=query)
    signals = []
    for row in resp:
        signals.append({
            "signal_rn": row.asset_group_signal.resource_name,
            "audience_rn": row.asset_group_signal.audience.audience,
        })
    log(f"Current asset_group_signals for asset group {ASSET_GROUP_ID}:")
    for s in signals:
        log(f"  signal: {s['signal_rn']}")
        log(f"    audience: {s['audience_rn']}")

    # Now fetch the dimensions of whichever audience(s) the signal(s) point to
    for s in signals:
        aud_id = s["audience_rn"].split("/")[-1]
        q2 = f"""
            SELECT audience.resource_name, audience.name, audience.dimensions
            FROM audience
            WHERE audience.id = {aud_id}
        """
        r2 = ga_service.search(customer_id=CUSTOMER_ID, query=q2)
        for row in r2:
            aud = row.audience
            log(f"  audience {aud.resource_name} name='{aud.name}'")
            for d_idx, d in enumerate(aud.dimensions):
                segs = d.audience_segments.segments
                if segs:
                    log(f"    dimension[{d_idx}] audience_segments ({len(segs)} segments):")
                    for seg in segs:
                        which = seg.WhichOneof("dimension") if hasattr(seg, "WhichOneof") else None
                        if seg.user_list.user_list:
                            log(f"      - user_list: {seg.user_list.user_list}")
                        else:
                            log(f"      - other segment type: {which}")


def option_a_update_in_place(client):
    log("=== Option A: update Audience.dimensions in place ===")
    audience_service = client.get_service("AudienceService")
    op = client.get_type("AudienceOperation")
    aud = op.update
    aud.resource_name = AUDIENCE_RN

    dim = build_user_list_dimension(client, ALL_USER_LIST_IDS)
    aud.dimensions.append(dim)

    client.copy_from(op.update_mask, client.get_type("FieldMask")(paths=["dimensions"]))

    resp = audience_service.mutate_audiences(
        customer_id=CUSTOMER_ID,
        operations=[op],
    )
    log(f"Option A SUCCESS. Updated: {resp.results[0].resource_name}")
    return resp.results[0].resource_name


def option_b_recreate(client):
    log("=== Option B: create new Audience + new AssetGroupSignal ===")
    # 1) Fetch original audience for name/description
    original = fetch_current_audience(client)
    base_name = original.name if original else "Pet Parent Signal"
    base_desc = original.description if original and original.description else "Expanded audience"

    # 2) Create new Audience with all 4 user lists
    audience_service = client.get_service("AudienceService")
    op = client.get_type("AudienceOperation")
    new_aud = op.create
    new_aud.name = f"{base_name} (expanded)"
    new_aud.description = base_desc
    new_aud.dimensions.append(build_user_list_dimension(client, ALL_USER_LIST_IDS))
    resp = audience_service.mutate_audiences(
        customer_id=CUSTOMER_ID,
        operations=[op],
    )
    new_audience_rn = resp.results[0].resource_name
    log(f"Created new Audience: {new_audience_rn}")

    # 3) Create new AssetGroupSignal pointing to new audience
    ags_service = client.get_service("AssetGroupSignalService")
    create_op = client.get_type("AssetGroupSignalOperation")
    new_signal = create_op.create
    new_signal.asset_group = f"customers/{CUSTOMER_ID}/assetGroups/{ASSET_GROUP_ID}"
    new_signal.audience.audience = new_audience_rn
    create_resp = ags_service.mutate_asset_group_signals(
        customer_id=CUSTOMER_ID,
        operations=[create_op],
    )
    new_signal_rn = create_resp.results[0].resource_name
    log(f"Created new AssetGroupSignal: {new_signal_rn}")

    # 4) Remove the old signal
    remove_op = client.get_type("AssetGroupSignalOperation")
    remove_op.remove = EXISTING_SIGNAL_RN
    rm_resp = ags_service.mutate_asset_group_signals(
        customer_id=CUSTOMER_ID,
        operations=[remove_op],
    )
    log(f"Removed old AssetGroupSignal: {rm_resp.results[0].resource_name}")

    return {"audience": new_audience_rn, "signal": new_signal_rn}


def main():
    client = build_client()

    log("--- Verification BEFORE ---")
    try:
        verify(client)
    except GoogleAdsException as e:
        print_gae(e)

    approach = None
    result = None
    option_a_error = None

    try:
        result = option_a_update_in_place(client)
        approach = "A"
    except GoogleAdsException as e:
        option_a_error = e
        log("Option A failed with GoogleAdsException:")
        print_gae(e)
        # Detect immutable/field-mask error and fall back
        msgs = " ".join(err.message.lower() for err in e.failure.errors)
        codes = " ".join(str(err.error_code) for err in e.failure.errors)
        log(f"Failure signature: codes={codes}")
        log("Falling back to Option B...")
        try:
            result = option_b_recreate(client)
            approach = "B"
        except GoogleAdsException as e2:
            log("Option B also failed:")
            print_gae(e2)
            raise
    except Exception as e:
        log(f"Option A failed with non-GoogleAdsException: {e!r}")
        log(traceback.format_exc())
        log("Falling back to Option B...")
        result = option_b_recreate(client)
        approach = "B"

    log("")
    log(f"=== Approach used: Option {approach} ===")
    log(f"Result: {result}")

    log("")
    log("--- Verification AFTER ---")
    verify(client)


if __name__ == "__main__":
    try:
        main()
    except GoogleAdsException as e:
        print_gae(e)
        sys.exit(1)
    except Exception as e:
        print(f"FATAL: {e!r}")
        traceback.print_exc()
        sys.exit(1)
