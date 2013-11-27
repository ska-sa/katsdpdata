import pysolr
import re
import time

#--------------------------------------------------------------------------------------------------
#--- CLASS :  DataSet
#--------------------------------------------------------------------------------------------------
class SearchResult(object):
    def __init__(self):
        self.hits = None
        self.docs = None

#--------------------------------------------------------------------------------------------------
#--- CLASS :  DataSet
#--------------------------------------------------------------------------------------------------
class KatSdpData(object):
    def __init__(self, url):
        """
        """
        self._url = url
        self.results = SearchResult()
        self.query = None

    def _date_to_ISO8601(self, date_range):
        """
        For the purposes of displaying times in SAST time.
        Parameters
        ----------
        date_range: string
            The date string to parse of the format "day/month/year", eg "01/01/2010"
        """
        self._date_regex = re.compile('[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}|[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}')
        for match in self._date_regex.findall(date_range):
            iso8601_date = self._SAST_to_ISO8601('%s 00:00:00 SAST' % (match))
            date_range = date_range.replace(match, iso8601_date, 1)
        return date_range

    def _SAST_to_ISO8601(self, date_range):
        """For search purposes. Take a local date stamp and format it for searching a solr index that
        has time stamps recorded in iso8601 format."""
        return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(time.mktime(
                                 time.strptime(date_range, '%d/%m/%Y %H:%M:%S SAST'))))

    def _to_solr_date_format(self, date_range):
        """
        '[1/12/2013 TO 1/12/2013+1DAY]'
        '[* TO NOW]'
        '[1/12/2010 TO *]'
        '[1/10/2010 TO 2/12/2010]'
        '[NOW-1YEAR/DAY TO NOW/DAY+1DAY]'
        '[23/03/1976 TO 20/03/1976+1YEAR]'
        '[23/03/1976/YEAR TO 23/03/1976]'
        """
        return self._date_to_ISO8601(date_range)

    def connect(self, url=None):
        if not url:
            self._solr = pysolr.Solr(self._url)
        else:
            # print 'Disconnecting from %s' % (self._url)
            self._url = url
            self._solr = pysolr.Solr(self._url)
            # print 'Connected to %s' % (self.url)
            # print 'Clearing search cache'
            self._results = None

    def search(self, text=None, date_range=None, rows=10, **kwargs):
        """Select subset of data product, based on search filters
        Parameters
        ----------
        text: string
        date_range: string
        """
        CAS = ["ProductId", "ProductTypeName", "ProductReceivedTime", "ProductTransferStatus", "ProductName",
                "ProductTypeId", "ProductStructure", "ReferenceFileSize", "ReferenceMimeType",
                "ReferenceDatastore", "ReferenceOriginal"]
        seach_query = []
        if text:
            seach_query.append('text:%s' % text)
        if date_range:
            if not date_range.startswith('[') and not date_range.endswith(']'):
                date_range = '['+date_range.upper()+']'
            seach_query.append('StartTime:%s' % self._to_solr_date_format(date_range))
        if kwargs:
            for k,v in kwargs.iteritems():
                if k in CAS:
                    seach_query.append('CAS.%s:%s' % (k,v))
                else:
                    seach_query.append('%s:%s' % (k,v))
        self.query = ' '.join(seach_query) if seach_query else '*:*'
        self._results = self._solr.search(self.query, rows=rows)
        self.results.hits = self._results.hits
        self.results.docs = self._results.docs
