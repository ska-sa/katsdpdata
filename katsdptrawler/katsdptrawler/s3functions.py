import boto
import boto.s3.connection
import copy
import json
import logging
import socket


logger = logging.getLogger(__name__)


_S3_CONFIG = {
    "profile_name": "default",
    "host": "localhost",
    "port": 8080,
    "is_secure": False,
    "calling_format": boto.s3.connection.OrdinaryCallingFormat()
}


_S3_BUCKET_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {"Sid": "AddPerm",
         "Effect": "Allow",
         "Principal": "*",
         "Action": ["s3:GetObject", "s3:ListBucket"],
         "Resource": ["PLACEHOLDER"]}
     ]
}


def s3_connect(host, port, profile_name='default'):
    """Test the connection to S3 as described in the args, and return
    the current user id and the connection object.
    In general we are more concerned with informing the user why the
    connection failed, rather than raising exceptions. Users should always
    check the return value and make appropriate decisions.
    If set, fail_on_boto will not suppress boto exceptions. Used when verifying
    credentials.
    Returns
    -------
    s3_conn : S3Connection
        A connection to the s3 endpoint. None if a connection error occurred.
    """
    s3_config = copy.deepcopy(_S3_CONFIG)
    s3_config['host'] = host
    s3_config['port'] = port
    s3_config['profile_name'] = profile_name

    s3_conn = boto.connect_s3(**s3_config)
    try:
        s3_conn.get_canonical_user_id()
        # reliable way to test connection and access keys
        return s3_conn
    except socket.error as e:
        logger.error("Failed to connect to S3 host %s:%i." % (s3_conn.host, s3_conn.port))
        logging.error("Please check network and host address. (%s)" % (e))
        raise
    except boto.exception.S3ResponseError as e:
        if e.error_code == "InvalidAccessKeyId":
            logger.error("Supplied access key %s is not for a valid S3 user." % (s3_conn.access_key))
        if e.error_code == "SignatureDoesNotMatch":
            logger.error("Supplied secret key is not valid for specified user.")
        if e.status == 403 or e.status == 409:
            logger.error("Supplied access key (%s) has no permissions on this server." % (s3_conn.access_key))
        raise
    return None


def s3_create_bucket(s3_conn, bucket_name):
    """Create an s3 bucket. If S3CreateError and the error
    status is 409, return a referece to the bucket as it has
    already been created and is owned by you.
    Returns
    ------
    s3_bucket : boto.s3.bucket.Bucket
        An S3 Bucket object
    """
    try:
        s3_bucket = s3_conn.create_bucket(bucket_name)
    except boto.exception.S3ResponseError as e:
        if e.status == 403 or e.status == 409:
            logger.error("Error status %s." % (e.status))
            logger.error("Supplied access key (%s) has no permissions on this server." % (s3_conn.access_key))
        raise
    except boto.exception.S3CreateError as e:
        if e.status == 409:  # Bucket already exists and you're the ownwer
            s3_bucket = s3_conn.get_bucket(bucket_name)
        else:
            raise
    # create policy
    s3_bucket_policy = copy.deepcopy(_S3_BUCKET_POLICY)
    s3_bucket_policy['Statement'][0]['Resource'] = [
        'arn:aws:s3:::{}/*'.format(bucket_name),
        'arn:aws:s3:::{}'.format(bucket_name)
    ]
    # set policy
    s3_bucket.set_policy(json.dumps(s3_bucket_policy))
    # create acl
    s3_bucket_acl = "public-read"
    # set acl
    s3_bucket.set_acl(s3_bucket_acl)
    return s3_bucket
