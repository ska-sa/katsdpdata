"""KAT classes wrapping XML-RPC method calls to OODT services."""

import xmlrpclib
import os
import mimetypes

from string import digits as _digits
from string import letters as _letters
from urlparse import urlparse, urljoin
from glob import glob

#--------------------------------------------------------------------------------------------------
#--- CLASS :  FileMgrClientException
#--------------------------------------------------------------------------------------------------

class FileMgrClientException(Exception):
    """A file manager client exception has occurred."""
    pass

#--------------------------------------------------------------------------------------------------
#--- CLASS :  XMLRPCMethodResponseOODTBase
#--------------------------------------------------------------------------------------------------

class XMLRPCMethodResponseOODTBase(object):
    """ """
    def __init__(self, xml_rpc_method_response):
        for key, value in xml_rpc_method_response.iteritems():
            setattr(self, key, self._recurse(value))

    def _recurse(self, value):
        if isinstance(value, (list)):
            return type(value)([self._recurse(v) for v in value])
        else:
            return XMLRPCMethodResponseOODTBase(value) if isinstance(value, dict) else value

class OODTProductMetadata(XMLRPCMethodResponseOODTBase):
    """ """
    def __init__(self, product_metadata):
        super(OODTProductMetadata, self).__init__(product_metadata)

class OODTProductReferences(XMLRPCMethodResponseOODTBase):
    def __init__(self, product_references):
        super(OODTProductReferences, self).__init__(product_references[0])

class OODTProductPage(XMLRPCMethodResponseOODTBase):
    """ """
    def __init__(self, page_product):
        super(OODTProductPage, self).__init__(page_product)

#--------------------------------------------------------------------------------------------------
#--- CLASS :  OODTClientBase
#--------------------------------------------------------------------------------------------------

class OODTClientBase(object):
    """Abstract class for OODT XML-RPC clients. Subclass before using.

    Parameters
    ----------
    rpc_url : string
        The XML-RPC URL for the intended service

    """

    def __init__(self, rpc_url):
        url_parts = urlparse(rpc_url)
        assert all([url_parts.scheme, url_parts.netloc])
        assert set(url_parts.netloc) <= set(_letters + _digits + '-.:')
        self.rpc_url = rpc_url
        self._rpc_proxy = xmlrpclib.ServerProxy(self.rpc_url)

#--------------------------------------------------------------------------------------------------
#--- CLASS :  FileMgrClient
#--------------------------------------------------------------------------------------------------

class FileMgrClient(OODTClientBase):
    """Client class for communicating with an OODT XML-RPC File Manager.

    Parameters
    ----------
    file_manager_url : string
        The XML-RPC URL for the file manager service

    Example
    -------
    from katood import FileMgrClient
    fm = FileMgrClient("http://localhost:9101")

    """

    def __init__(self, file_manager_url):
        super(FileMgrClient, self).__init__(file_manager_url)

    def _get_product_type_by_name(self, product_type_name):
        """Query the File Manager get the fully quailified description of a product type by its
        name.

        Parameters
        ----------
        product_type_name : string
            The name of the product type.

        Returns
        -------
        product_type : dict
            A dictionary containing the fully qualified product type description.

        """
        return self._rpc_proxy.filemgr.getProductTypeByName(product_type_name)

    def _delete_product(self, product):
        """Delete a product.

        Parameters
        ----------
        product : dict
            A dictionary containing the product information.

        """
        product_refs = self._rpc_proxy.filemgr.getProductReferences(product)
        data_store_ref = urlparse(product_refs[0]['dataStoreReference'])
        if not self._rpc_proxy.filemgr.removeFile(data_store_ref.path):
            raise FileMgrClientException
        self._rpc_proxy.filemgr.removeProduct(product)
        return True

    def _delete_product_by_id(self, product_id):
        """Request the File Manager to remove a product with the given product id.

        Parameters
        ----------
        product_id : string
            The UUID for the product to delete.

        """
        product = self._rpc_proxy.filemgr.getProductById(product_id)
        return self._delete_product(product)

    def get_product_by_name(self, product_name):
        """Query the File Manager for a product's information.

        Parameters
        ----------
        product_name : string
            The name of the product.

        Returns
        -------
        product : dictionary
            A dictionary containing the product information.

        """
        product = self._rpc_proxy.filemgr.getProductByName(product_name)
        return product

    def get_product_by_id(self, product_id):
        """Query the File Manager for a product's information.

        Parameters
        ----------
        product_id: string
            The UUID of the product.

        Returns
        -------
        product : dictionary
            A dictionary containing the product information.

        """
        product = self._rpc_proxy.filemgr.getProductById(product_id)
        return product

    def is_alive(self):
        """Check to see if the File Manager is running.

        Returns
        -------
        status : boolean
            True if the File Manager is running

        """
        return self._rpc_proxy.filemgr.isAlive()


    def get_product_types(self):
        """Query the File Manager to see which product types are supported.

        Returns
        -------
        product_types : dict
            Returns a dictionary of the supported product types

        """
        product_types = self._rpc_proxy.filemgr.getProductTypes()
        return [pt['name'] for pt in product_types]

    def get_num_products(self, product_type_name):
        """Query the File Manager for the number of products in the catalogue
        for a particular product type.

        Parameters
        ----------
        product_type_name : string
            The name of the product type. Supported product types are returned by
            self.get_product_types()

        Returns
        -------
        num_products : integer
            The number of products for product_type_name

        Example
        -------
        from katoodt import FileMgrClient
        fmc = FileMgrClient('http://localhost:9101')
        pt = fmc.get_product_types()

        fmc.get_num_products()

        """
        product_type = self._get_product_type_by_name(product_type_name)
        num_products = self._rpc_proxy.filemgr.getNumProducts(product_type)
        return num_products

    def get_elements_by_product_type(self, product_type_name):
        """Query the File Manager to find the meta data for a product type.

        Parameters
        ----------
        product_type_name : string
            The name of the product type.

        """
        product_type = self._get_product_type_by_name(product_type_name)
        return self._rpc_proxy.filemgr.getElementsByProductType(product_type)

    def has_product(self, product_name):
        """Query the File Manager to see if the catalogue contains a product.

        Parameter
        ---------
        product_name : string
            The name of the product to check against the catalogue.

        Returns
        -------
        status : boolean
            True if product exists in the catalogue.

        """
        return self._rpc_proxy.filemgr.hasProduct(product_name)

    def delete_product(self, product_name):
        """Request the File Manager to remove a product with the given product name.

        Parameters
        ----------
        product_name : string
            The name of the product to delete.

        """
        product = self._rpc_proxy.filemgr.getProductByName(product_name)
        return self._delete_product(product)

    def get_product_references(self, product_name):
        """Query the File Manager for a product's references.

        Parameters
        ----------
        product_name : string
            The name of the product.

        Returns
        -------
        product_info : list
            A list of products references. The list should contain a single dictionary item.

        """
        product = self.get_product_by_name(product_name, thin_call=False)
        #return OODTProductReferences(product['references'])
        return product['references']

    def get_product_metadata(self, product_name):
        """Query the File Manager for a product's metadata.

        Parameters
        ----------
        product_name : string
            The name of the product.

        """
        product = self.get_product_by_name(product_name)
        product_metadata = self._rpc_proxy.filemgr.getMetadata(product)
        # return OODTProductMetadata(product_metadata)
        return product_metadata

    def get_last_page(self, product_type_name):
        """Query the File Manager for last page of archived product of a given product type.

        Parameters
        ----------
        product_type_name : string
            The name of the product type.

        Returns
        -------
        products : dictionary
            A dictionary containing the page product information. This is a thin call, so a
            products references will be an empty list.

        """
        product_type = self._get_product_type_by_name(product_type_name)
        page_product = self._rpc_proxy.filemgr.getLastPage(product_type)
        return page_product
        # return OODTProductPage(page_product)

    def ingest_product(self, name, metadata, structure, product_type, product_reference, client_transfer=False):
        """Ingest a product of a given product type with a given name and client side metadata.

        Parameters
        ----------
        name: string
            The name to set for the product to be ingested.
        metadata: dictionary
            Client side metadata for the product. Keys must be strings, items must be lists of strings.
        structure: string
            The structure type. Either 'Flat' or 'Hierarchical'
        product_type: string
            The product type name. A call to self.get_product_types() will list these.
        product_reference: string
            The full path to the product to be ingested. Either a file (flat) or a directory (hierarchical).
        client_transfer: boolean
            Enable client transfer. Not currently supported.

        Returns
        -------
        products : dictionary
            A dictionary containing the page product information. This is a thin call, so a
            products references will be an empty list.

        Raises
        ------
        FileMgrClientException
        """

        if client_transfer:
            raise FileMgrClientException('Client transfer is not yet supported.')

        if os.path.isfile(product_reference) and structure == 'Flat':
            references = [product_reference]
        elif os.path.isdir(product_reference) and structure == 'Hierarchical':
            references = [product_reference]
            references.extend([g for g in glob(os.path.join(product_reference, '*')) if os.path.isfile(g)])
        else:
            raise FileMgrClientException('product_reference %s and structure %s specified does not match' % (product_reference, structure))

        param_dict = {}
        param_dict['name'] = name
        param_dict['structure'] = structure
        param_dict['type'] = self._rpc_proxy.filemgr.getProductTypeByName(product_type)
        param_dict['references'] = [{
                'origReference': urljoin('file:',r),
                'fileSize': os.path.getsize(r),
                'mimeType': mimetypes.guess_type(r)[0] if mimetypes.guess_type(r)[0] else 'application/octet-stream',
                'dataStoreReference': '',} for r in references]

        #call xmlrpc
        param_dict['id'] = self._rpc_proxy.filemgr.ingestProduct(param_dict, metadata, client_transfer)

        #@todo add functionality for client transferer
        return param_dict['id']

#--------------------------------------------------------------------------------------------------
#--- CLASS :  CrawlerClient
#--------------------------------------------------------------------------------------------------

class CrawlerClient(OODTClientBase):
    """Client class for communicating with an OODT XML-RPC Crawler Daemon.

    Parameters
    ----------
    crawler_daemon_url : string
        The XML-RPC URL for the file manager service

    Example
    -------
    from katood import CrawlerClient
    fm = CrawlerClient("http://localhost:9102")

    """

    def __init__(self, crawler_daemon_url):
        super(CrawlerClient, self).__init__(crawler_daemon_url)

    def is_running(self):
        """Check to see if the Crawler daemon is running.

        Returns
        -------
        status : boolean
            True if the Crawler Daemon is running

        """
        return self._rpc_proxy.crawldaemon.isRunning()

#--------------------------------------------------------------------------------------------------
#--- CLASS :  WorkflowClient
#--------------------------------------------------------------------------------------------------

class WorkflowClient(OODTClientBase):
    """Client class for communicating with an OODT XML-RPC Workflow Manager.

    Parameters
    ----------
    workflow_url : string
        The XML-RPC URL for the workflow manager service

    Example
    -------
    from katood import WorkflowClient
    fm = WorkflowClient("http://localhost:9103")

    """

    def __init__(self, workflow_url):
        super(WorkflowClient, self).__init__(workflow_url)

