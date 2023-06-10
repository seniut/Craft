import pytest
import requests
import pandas as pd
from unittest import TestCase
from run import BaseJob
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders


class Testing(TestCase):
    @pytest.mark.skip(reason="To do test of parser() need to play with creation custom WARC file")
    def test_parser(self):
        with open('data/test/test_file.warc.gz', 'wb') as output:
            writer = WARCWriter(output, gzip=True)

            resp = requests.get('http://example.com/',
                                headers={'Accept-Encoding': 'identity'},
                                stream=True)

            # get raw headers from urllib3
            headers_list = resp.raw.headers.items()

            http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

            record = writer.create_warc_record('http://example.com/', 'response',
                                               payload=resp.raw,
                                               http_headers=http_headers)

            writer.write_record(record)

        bj = BaseJob()
        bj.input_file_path = 'data/test/test_file.warc.gz'

        """
        ['fetched_at', "http_code", "domain", "user_agent", "disallow_cnt", "allow_cnt", "run_date"]
        fetched_at,http_code,domain,user_agent,disallow_cnt,allow_cnt
        2023-04-02T10:55:08Z,200,*,young.tonymctony.com,3,0
        2023-04-02T10:55:08Z,200,Mediapartners-Google,young.tonymctony.com,2,0
        """
        df = pd.DataFrame({
            'fetched_at': ['2023-04-02T10:55:08Z', '023-04-02T10:55:08Z'],
            'http_code': [200, 200],
            'domain': ['*', 'Mediapartners-Google'],
            'user_agent': ['young.tonymctony.com', 'young.tonymctony.com'],
            'disallow_cnt': [3, 2],
            'allow_cnt': [0, 0],
            'run_date': [bj.run_date, bj.run_date]
        })
        df2 = bj.parser()
        self.assertTrue(bj.parser().equals(df))

    @pytest.mark.skip(reason="Need to create mock method for with open write")
    def test_gathering_statistics(self):
        self.assertTrue(True)
