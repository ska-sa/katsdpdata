import re
import os


from katsdptrawler import s3functions


class CrawlerBase(object):
    """docstring for CrawlerBase"""
    def __init__(self, regexs):
        self.regexs = regexs
        super(CrawlerBase, self).__init__()

    def _list(self):
        raise NotImplementedError

    def _match_regex(self, contents, regex):
        return sorted([d for d in contents if re.match(regex, d)])

    def list(self):
        contents = self._list()
        matched = {}
        for k,v in self.regexs.items():
            matched[k] = self._match_regex(contents, v)
        return matched


class StreamCrawler(CrawlerBase):
    """docstring for StreamCrawler"""
    def __init__(self):
        regexs = {}
        regexs['head'] = '^[0-9]{10}$'
        regexs['streams'] = '^[0-9]{10}[-_].*$'
        super(StreamCrawler, self).__init__(regexs)

    def list(self):
        sources = super(StreamCrawler, self).list()
        stream_products = []
        for stream in sources['streams']:
            test_head = re.match(self.regexs['head'][0:-1], stream)
            head = test_head.group()
            if head in sources['head']:
                stream_products.append({'head':head, 'stream':stream})
        return stream_products
 
class LocalDirectoryCrawler(StreamCrawler):
    def __init__(self, src):
        self.src = src
        self.root = self.src['config']['trawl_dir']
        super(LocalDirectoryCrawler, self).__init__()

    def _list(self):
        return [d for d in os.listdir(self.root) if os.path.isdir(os.path.join(self.root, d))]


class S3Crawler(StreamCrawler):
    def __init__(self, src):
        self.src = src
        self.root_con = self._s3_connect()
        super(S3Crawler, self).__init__()

    def _s3_connect(self):
        return s3functions.s3_connect(**self.src['config'])

    def _list(self):
        return [b.name for b in self.root_con.get_all_buckets()]

