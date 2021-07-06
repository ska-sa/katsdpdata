import pysolr
import uuid
import os
import urllib.parse
import time
import mimetypes

import logging
logger = logging.getLogger(__name__)


class MetaDataHandlerException(Exception):
    """Handle execptions generated by the metadata handler class"""
    pass


class MetDataHandlerSuper:
    """Class for generating solr metadata that follows a OODT styled data product.

    Parameters
    ----------
    solr_url: string : solr url endpoint for metadata queries.
    product_type: string : A predefined product type for OODT purposes.
    product_name: string : The name of the product.
    product_id: string : Unique identifier for product. If set to none,
                         product will have a self generated uuid.
    """

    def __init__(self, solr_url, product_type, product_name, product_id=None):
        self.solr_url = solr_url
        self.solr = pysolr.Solr(self.solr_url)
        self.product_type = product_type
        self.product_name = product_name
        if product_id:
            self.product_id = product_id
        else:
            self.product_id = str(uuid.uuid4())

    def create_core_met(self):
        raise NotImplementedError

    def get_prod_met(self, prod_id=None):
        """Get the metadata for a product, if prod_id is not specified, use self.prod_id.

        Parameters
        ----------
        prod_id: string : the product id to find.

        Returns
        -------
        met: dict : metadata containing the _version_ for version tracking commits to solr.
        """
        copy_fields = ['Observer_lowercase']
        if not prod_id:
            prod_id = self.product_id
        query = 'id:{}'.format(prod_id)
        res = self.solr.search(query)
        if res.hits == 0:
            return {}
        elif res.hits > 1:
            raise MetaDataHandlerException('{} returned for {}'.format(res.hits, query))
        # filter out copy field
        doc = res.docs[0]
        for c in copy_fields:
            doc.pop(c, None)
        return doc

    def del_prod_met(self, prod_id):
        """Delete product metadata. Use with caution, as will delete product metadata
        from solr.

        Parameters
        ----------
        prod_id: string : the product id to delete.
        """
        met = self.get_prod_met(prod_id)
        self.solr.delete(id=prod_id, commit=True)
        return met

    def set_product_received(self, met):
        """Set product transfer status once received into the backend storage.

        Parameters
        ----------
        met: dict : metadata dict, a local copy of the solr doc to update.

        Returns
        -------
        met: dict : metadata containing the _version_ for version tracking commits to solr.
        """
        met['CAS.ProductReceivedTime'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        met['CAS.ProductTransferStatus'] = 'RECEIVED'
        self.solr.add([met], commit=True)
        return self.get_prod_met(met['id'])   # return with updated _version_

    def set_product_created(self, met):
        """Set product transfer status while transfering to the backend storage.

        Parameters
        ----------
        met: dict : metadata dict, a local copy of the solr doc to update.

        Returns
        -------
        met: dict : metadata containing the _version_ for version tracking commits to solr.
        """
        met['CAS.ProductReceivedTime'] = ''
        met['CAS.ProductTransferStatus'] = 'CREATED'
        self.solr.add([met], commit=True)
        return self.get_prod_met(met['id'])   # return with updated _version_

    def set_product_status(self, status, met=None):
        if not met:
            met = self.get_prod_met()
        if not met:
            met = self.create_core_met()
        met['CAS.ProductTransferStatus'] = status
        self.solr.add(
            [met],
            fieldUpdates={'CAS.ProductTransferStatus': 'set'},
            commit=True)
        return self.get_prod_met(met['id'])

    def add_bucket_stats(self, met, bucket_met):
        met.update(bucket_met)
        self.solr.add([met], commit=True)
        return self.get_prod_met(met['id'])

    def get_state(self):
        met = self.get_prod_met()
        return met.get('CAS.ProductTransferStatus', None)


class MetaDataHandler(MetDataHandlerSuper):
    """keeping the original name of this class to avoid possible import issues
    """
    def __init__(self, *argv, **kwargs):
        super().__init__(*argv, **kwargs)

    def create_core_met(self):
        """Create the core OODT style metadata.

        Returns
        -------
        met: dict : metadata containing the _version_ for version tracking commits to solr.
        """
        new_met = {}
        new_met['id'] = self.product_id
        new_met['CAS.ProductId'] = self.product_id
        new_met['CaptureStreamId'] = self.product_id
        new_met['CAS.ProductName'] = self.product_name
        new_met['CAS.ProductTypeId'] = 'urn:kat:{}'.format(self.product_type)
        new_met['CAS.ProductTypeName'] = self.product_type
        new_met['id'] = self.product_id
        self.solr.add([new_met], commit=True)
        return self.get_prod_met(self.product_id)  # return with _version_

    def add_ref_original(self, met, original_refs):
        """Handle original references for product and decide if its Flat or Hierarchical.

        Parameters
        ----------
        met: dict : metadata dict, a local copy of the solr doc to update.

        Returns
        -------
        met: dict : metadata containing the _version_ for version tracking commits to solr.
        """
        def file_url(file_ref):
            return urllib.parse.urlparse(file_ref)._replace(scheme='file').geturl()

        if len(original_refs) == 0:
            raise MetaDataHandlerException('No product in {}'.format(original_refs))
        elif len(original_refs) == 1:
            met['CAS.ProductStructure'] = 'Flat'
        else:
            met['CAS.ProductStructure'] = 'Hierarchical'
        product_sizes = [os.path.getsize(p) for p in original_refs]
        product_types = [mimetypes.guess_type(p)[0]
                         if mimetypes.guess_type(p)[0] else 'application/x-data'
                         for p in original_refs]
        met['CAS.ReferenceOriginal'] = [file_url(x) for x in sorted(original_refs)]
        met['CAS.ReferenceFileSize'] = product_sizes
        met['CAS.ReferenceMimeType'] = product_types
        self.solr.add([met])
        return self.get_prod_met(met['id'])  # return with updated _version_

    def add_inferred_ref_datastore(self, met):
        """Handle inferred datastore refereces based on teh orginial reference"""
        def replace_file_s3(url):
            url_parts = urllib.parse.urlparse(url)
            s3_path = url_parts.path.replace('/data', '/')
            url_parts = url_parts._replace(scheme='s3', path=s3_path)
            return url_parts.geturl()
        if not 'CAS.ReferenceOriginal' in met:
            return met
        original_refs = met['CAS.ReferenceOriginal']
        datastore_refs = [replace_file_s3(ref) for ref in original_refs]
        met['CAS.ReferenceDatastore'] = datastore_refs
        self.solr.add([met], commit=True)
        return self.get_prod_met(met['id'])  # return with updated _version_

    def add_ref_datastore(self, met, datastore_refs):
        """Handle datastore references for product once ingested into the backend storage.

        Parameters
        ----------
        met: dict : metadata dict, a local copy of the solr doc to update.

        Returns
        -------
        met: dict : metadata containing the _version_ for version tracking commits to solr.
        """
        met['CAS.ReferenceDatastore'] = [urllib.parse.urlparse(x).geturl() for x in sorted(datastore_refs)]
        self.solr.add([met], commit=True)
        return self.get_prod_met(met['id'])  # return with updated _version_

    def add_prod_met(self, met, prod_met):
        """Add the product based metadata to solr.

        Returns
        -------
        met: dict : metadata containing the _version_ for version tracking commits to solr.
        """
        # ProductName is mapped internally by OODT. Pop it if we're going to insert directly into SOLR.
        prod_met.pop('ProductName', None)
        met.update(prod_met)
        self.solr.add([met], commit=True)
        return self.get_prod_met(met['id'])


class ProdMetaDataHandler(MetDataHandlerSuper):
    # TODO: SPR1-1016
    def create_core_met(self):
        """Create the core OODT style metadata.

        Returns
        -------
        met: dict : metadata containing the _version_ for version tracking commits to solr.
        """
        new_met = {}
        new_met['id'] = self.product_id
        new_met['CaptureStreamId'] = self.product_id
        new_met['CAS.ProductId'] = self.product_id
        new_met['CAS.ProductName'] = self.product_name
        new_met['CAS.ProductTypeId'] = 'urn:kat:{}'.format(self.product_type)
        new_met['CAS.ProductTypeName'] = self.product_type  # MeerKATFlagProduct
        self.solr.add([new_met], commit=True)
        return self.get_prod_met(self.product_id)  # return with _version_


        # "CAS.ProductTransferStatus": "ARCHIVED",
        # "CAS.ProductTypeName": "MeerKATFlagProduct", // This is the only thing that will be different between L0 and L1
        # "id": "1521643937_sdp_l1_flags",
        # "CaptureBlockId": "1521643937",
        # "CAS.ProductStructure": "Hierarchical",
        # "CAS.ProductName": "1521643937_sdp_l1_flags",
        # "StreamId": "sdp_l1_flags",

