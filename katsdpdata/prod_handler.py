import logging
import socket

import boto
import boto.s3.connection

logger = logging.getLogger(__name__)


def make_boto_dict(s3_args):
    """Create a dict of keyword parameters suitable for passing into a boto.connect_s3 call using the supplied args."""
    return {"host": s3_args.s3_host,
            "port": s3_args.s3_port,
            "is_secure": False,
            "calling_format": boto.s3.connection.OrdinaryCallingFormat()}


def get_s3_connection(boto_dict):
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
    s3_conn = boto.connect_s3(**boto_dict)
    try:
        s3_conn.get_canonical_user_id()
        # reliable way to test connection and access keys
        return s3_conn
    except socket.error as e:
        logger.error("Failed to connect to S3 host %s:%i. Please check network and host address. (%s)",
                     s3_conn.host, s3_conn.port, e)
        raise
    except boto.exception.S3ResponseError as e:
        if e.error_code == "InvalidAccessKeyId":
            logger.error("Supplied access key %s is not for a valid S3 user.", redact_key(s3_conn.access_key))
        if e.error_code == "SignatureDoesNotMatch":
            logger.error("Supplied secret key is not valid for specified user.")
        if e.status == 403 or e.status == 409:
            logger.error("Supplied access key (%s) has no permissions on this server.", redact_key(s3_conn.access_key))
        raise
    return None


def redact_key(s3_key):
    redacted_key = s3_key[:3] + "############" + s3_key[-3:]
    return redacted_key
