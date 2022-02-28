import pysolr
import boto
from boto.s3.connection import S3Connection
import os

CAPTURE_STREAM_L0_REGEX = "^[0-9]{10}[-_].*l0$"
CAPTURE_STREAM_L1_REGEX = "^[0-9]{10}[-_].*l1-flags$"
CAPTURE_STREAM_L1_REGEX_HISTORIC = "^[0-9]{10}[-_].*l1_flags$"

TELESCOPE = "MeerKATTelescopeProduct"

GENERIC_PLACEHOLDER = "PlaceholderProduct"
PRODUCT_PLACEHOLDER = "MeerKATVisibilityProduct-Placeholder"
FLAGS_PLACEHOLDER = "MeerKATFlagProduct-Placeholder"
DEFUNCT_STATE = "DEFUNCT"

PRODUCT_REDUCTON_OLD = "MeerKATTelescopeProduct"
PRODUCT_NEW = "MeerKATVisibilityProduct"
FLAGS_NEW = "MeerKATFlagProduct"

STARTDATE = os.environ.get("STARTDATE", "2010-01-01T00:00:00Z")
ENDDATE = os.environ.get("ENDDATE", "2021-09-01T00:00:00Z")


def new_product_exists(product, solr_conn):
    block_id = product["CaptureBlockId"]
    results = solr_conn.search(
        f"CaptureBlockId:{block_id} && " f"CAS.ProductTypeName:{PRODUCT_NEW}"
    )
    return results.hits


def get_flag_placeholder_product(product, solr_conn):
    block_id = product["CaptureBlockId"]
    results = solr_conn.search(
        f"CaptureBlockId:{block_id} && "
        f"CAS.ProductTypeName:{GENERIC_PLACEHOLDER} && "
        f"ReductionName:{FLAGS_NEW}"
    )
    return results


def get_product_placeholder_product(product, solr_conn):
    block_id = product["CaptureBlockId"]
    results = solr_conn.search(
        f"CaptureBlockId:{block_id} && "
        f"CAS.ProductTypeName:{GENERIC_PLACEHOLDER} && "
        f"ReductionName:{PRODUCT_REDUCTON_OLD}"
    )
    return results


def create_l0_product_from_placeholder(
    rdb_product, placeholder_product, solr, s3_conn, update_set
):
    product_id = f'{placeholder_product["CaptureBlockId"]}-sdp-l0-visibility'
    if "CaptureStreamId" in product.keys():
        cbid = placeholder_product["CaptureBlockId"]
    else:
        cbid = placeholder_product["Prefix"]
    if "ProductExpireTime" in placeholder_product.keys():
        pxt = product["ProductExpireTime"]
    else:
        pxt = None
    if "ReferenceTapeStore" in placeholder_product.keys():
        pts = placeholder_product["ReferenceTapeStore"]
    else:
        pts = None
    new_met = {
        "id": product_id,
        "CaptureBlockId": cbid,
        "CAS.ProductId": product_id,
        "CAS.ProductName": product_id,
        "CAS.ProductTypeId": f"urn:kat:{PRODUCT_NEW}",
        "CAS.ProductTypeName": f"{PRODUCT_NEW}",
        "CAS.ProductTransferStatus": placeholder_product["CAS.ProductTransferStatus"],
        "ProductExpireTime": pxt,
        "ReferenceTapeStore": pts,
        "Prefix": rdb_product["Prefix"],
    }
    # try:
    #     bucket = s3_conn.get_bucket(f'{product["Prefix"]}-sdp-l0')
    #     key_sizes = [k.size for k in bucket.list()]
    #     if key_sizes:
    #         new_met["size"] = sum(key_sizes)
    #         new_met["num_objects"] = len(key_sizes)
    # except boto.exception.S3ResponseError:
    #     pass
    placeholder_product["CAS.ProductTransferStatus"] = DEFUNCT_STATE
    if update_set:
        update_set.append(placeholder_product)
        update_set.append(new_met)
    else:
        solr.add([placeholder_product], commit=True)
        solr.add([new_met], commit=True)


def create_l1_product_from_placeholder(
    rdb_product, placeholder_product, solr, s3_conn, update_set
):
    product_id = placeholder_product["Prefix"]
    if "CaptureBlockId" in product.keys():
        cbid = product["CaptureBlockId"]
    else:
        cbid = product["Prefix"].split("-")[0]
    new_met = {
        "id": product_id,
        "CaptureBlockId": cbid,
        "CAS.ProductId": product_id,
        "CAS.ProductName": product_id,
        "CAS.ProductTypeId": f"urn:kat:{FLAGS_NEW}",
        "CAS.ProductTypeName": f"{FLAGS_NEW}",
        "CAS.ProductTransferStatus": product["CAS.ProductTransferStatus"],
        "Prefix": product["Prefix"],
    }

    if "ProductExpireTime" in product.keys():
        new_met["ProductExpireTime"] = product["ProductExpireTime"]
    if "ReferenceTapeStore" in product.keys():
        new_met["ReferenceTapeStore"] = product["ReferenceTapeStore"]
    try:
        bucket = s3_conn.get_bucket(f'{product["Prefix"]}-sdp-l0')
        key_sizes = [k.size for k in bucket.list()]
        if key_sizes:
            new_met["size"] = sum(key_sizes)
            new_met["num_objects"] = len(key_sizes)
    except boto.exception.S3ResponseError:
        pass
    placeholder_product["CAS.ProductTransferStatus"] = DEFUNCT_STATE
    update_set.append(placeholder_product)
    update_set.append(new_met)


def create_new_l0_products(product, solr, s3_conn, update_set):
    product_id = f'{product["id"]}-visibility'.replace("_", "-")
    if "Prefix" in product.keys():
        prefix = product["Prefix"]
    else:
        prefix = product["id"]
    new_met = {
        "id": product_id,
        "CaptureBlockId": product["CaptureBlockId"],
        "CAS.ProductId": product_id,
        "CAS.ProductName": product_id,
        "CAS.ProductTypeId": f"urn:kat:{PRODUCT_NEW}",
        "CAS.ProductTypeName": f"{PRODUCT_NEW}",
        "CAS.ProductTransferStatus": product["CAS.ProductTransferStatus"],
        "Prefix": prefix,
    }
    if "ProductExpireTime" in product.keys():
        new_met["ProductExpireTime"] = product["ProductExpireTime"]
    if "ReferenceTapeStore" in product.keys():
        new_met["ReferenceTapeStore"] = product["ReferenceTapeStore"]

    try:
        bucket = s3_conn.get_bucket(f'{product["id"]}')

        key_sizes = [k.size for k in bucket.list()]
        if key_sizes:
            new_met["size"] = sum(key_sizes)
            new_met["num_objects"] = len(key_sizes)
    except:
        pass
    if update_set:
        update_set.append(new_met)
    else:
        solr.add([new_met], commit=True)


def update_prefix_on_rdb_product(product, solr, update_set):
    if "Prefix" in product.keys():
        product["Prefix"] = product["Prefix"].split("_")[0].split("-")[0]
    else:
        product["Prefix"] = product["id"].split("_")[0].split("-")[0]
    product.pop("version")
    if "Observer_lowercase" in product.keys():
        product.pop("Observer_lowercase")
    if update_set:
        update_set.append(product)
    else:
        solr.add([product], commit=True, fieldUpdates={"Prefix": "set"})


def create_new_l1_products(product, solr, s3_conn, update_set):
    product_id = (
        product["id"]
        .replace("sdp_l0", "sdp_l1_flags")
        .replace("sdp-l0", "sdp-l1-flags")
    )
    try:
        bucket = s3_conn.get_bucket(f'{product["id"]}')
    except:
        return
    new_met = {
        "id": product_id,
        "CaptureBlockId": product["CaptureBlockId"],
        "CAS.ProductId": product_id,
        "CAS.ProductName": product_id,
        "CAS.ProductTypeId": f"urn:kat:{FLAGS_NEW}",
        "CAS.ProductTypeName": f"{FLAGS_NEW}",
        "CAS.ProductTransferStatus": product["CAS.ProductTransferStatus"],
        "Prefix": id,
    }

    key_sizes = [k.size for k in bucket.list()]
    if key_sizes:
        new_met["size"] = sum(key_sizes)
        new_met["num_objects"] = len(key_sizes)
    if update_set:
        update_set.append(new_met)
    else:
        solr.add([new_met], commit=True)


solr_conn = pysolr.Solr("http://webtest01.sdp.kat.ac.za:8983/solr/kat_core")
search_query = f"CAS.ProductTypeName:{TELESCOPE}"
query_dict = {
    "q": search_query,
    "cursorMark": "*",
    "fq": [f"StartTime:[{STARTTIME} TO {ENDTIME}"],
}


s3_conn = S3Connection(
    aws_access_key_id="",
    aws_secret_access_key="",
    port=7480,
    host="archive-gw-1.kat.ac.za",
    is_secure=False,
    calling_format=boto.s3.connection.OrdinaryCallingFormat(),
)


cursorMark = "*"
res = solr_conn.search(search_query)
while res.nextCursorMark != cursorMark:
    cursorMark = res.nextCursorMark
    query_dict["cursorMark"] = res.nextCursorMark
    res = solr_conn.search(**query_dict)
    for product in res:
        update_set = []
        print(f'Busy with {product["CaptureBlockId"]}.')
        if new_product_exists(product, solr_conn):
            print(
                f'The product {product["CaptureBlockId"]} already exists. Doing nothing.'
            )
            continue
        placeholder_products = get_product_placeholder_product(product, solr_conn)
        if placeholder_products.hits:
            print(f"found {placeholder_products.hits} VIS placeholder products.")
            p_product = [p for p in placeholder_products][0]
            create_l0_product_from_placeholder(
                product, p_product, solr_conn, s3_conn, update_set
            )
            print(f"Created a VIS product from a placeholder product.")
        else:
            create_new_l0_products(product, solr_conn, s3_conn, update_set)
            print(f"Created a new VIS product (placeholder absent).")
        update_prefix_on_rdb_product(product, solr_conn, update_set)
        print(f"Updating prefix for RDB product.")
        placeholder_products = get_flag_placeholder_product(product, solr_conn)
        if placeholder_products.hits:
            print(f"found {placeholder_products.hits} FLAG placeholder products.")
            p_product = [p for p in placeholder_products][0]
            create_l1_product_from_placeholder(
                product, p_product, solr_conn, s3_conn, update_set, update_set
            )
            print(f"Created a FLAG product from a placeholder product.")
        else:
            create_new_l1_products(product, solr_conn, s3_conn, update_set)
            print(f"Created a new FLAG product (placeholder absent).")
        solr_conn.add(update_set, commit=True)
        print(f"Committed changes")
