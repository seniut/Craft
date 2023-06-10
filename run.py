import argparse
import shutil
import gzip
import pandas as pd
import logging
import sys
import os
from datetime import datetime
from fastwarc.stream_io import GZipStream, FileStream
from fastwarc.warc import ArchiveIterator, WarcRecordType

import boto3
import requests


LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


class BaseJob(object):
    """
    A simple job definition to process Common Crawl data by
    """

    # TODO: Make code refactoring of class attributes

    name = 'BaseJob'

    args = None
    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    run_date = datetime.now().strftime('%Y%m%d')

    bucket = 'commoncrawl'
    prefix = 'crawl-data/CC-MAIN-2023-14/segments/1679296950528.96/robotstxt/'
    base_url = 'https://data.commoncrawl.org/'
    source = "data/source/"
    extracted = "data/extracted/"
    destination = "data/destination/"
    statistics_path = 'data/statistics/'
    list_needed_dir = [source, extracted, destination, statistics_path]

    input_file_path = None
    is_overwriting = True

    columns = ['fetched_at', 'http_code', 'domain', 'User-agent', 'Disallow', 'Allow']
    group_columns = ['fetched_at', 'http_code', 'domain', 'User-agent']
    calc_columns = ['Disallow', 'Allow']
    none_columns = ['User-agent', 'Disallow', 'Allow']
    result_mapping_columns = ['fetched_at', "http_code", "domain", "user_agent", "disallow_cnt", "allow_cnt", "run_date"]
    statistic_columns = ["date", "file_name", "run_date", "total_errors", "total_ok", "total_distinct_ua", "total_allows", "total_disallows"]

    unloading_url = 'https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/segments/1679296950528.96/robotstxt/CC-MAIN-20230402105054-20230402135054-00799.warc.gz'

    processing_files_at_run = 1
    file_mode = 'w+'

    parquet_partition_cols = ['fetched_at']

    def parse_arguments(self):
        """Returns the parsed arguments from the command line"""

        description = self.name
        if self.__doc__ is not None:
            description += " - "
            description += self.__doc__

        arg_parser = argparse.ArgumentParser(prog=self.name,
                                             description=description,
                                             conflict_handler='resolve')

        arg_parser.add_argument("--run_date", help="Date of process",
                                default=self.run_date)

        arg_parser.add_argument("--unloading_url", help="Path to file listing input paths",
                                default=self.unloading_url)
        arg_parser.add_argument("--processing_s3_files_at_run", help="Count of files from s3 to parse at run",
                                default=self.processing_files_at_run)
        arg_parser.add_argument("--is_overwriting", help="Overwriting processed files",
                                default=self.is_overwriting)

        args = arg_parser.parse_args()
        self.init_logging(self.log_level)
        return args

    def init_logging(self, level=None):
        if level:
            self.log_level = level
        else:
            level = self.log_level
        logging.basicConfig(level=level, format=LOGGING_FORMAT)
        logging.getLogger(self.name).setLevel(level)

    def get_logger(self):
        return logging.getLogger(self.name)

    @staticmethod
    def get_all_s3_objects(s3, **base_kwargs):
        continuation_token = None
        while True:
            list_kwargs = dict(MaxKeys=1000, **base_kwargs)
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            response = s3.list_objects_v2(**list_kwargs)
            yield from response.get('Contents', [])
            if not response.get('IsTruncated'):  # At the end of the list?
                break
            continuation_token = response.get('NextContinuationToken')

    def check_statistics(self, filename: str):
        try:
            df = pd.read_csv(f"{self.statistics_path}{self.run_date}.csv")
            if filename in df['file_name'].values:
                self.get_logger().info(f"File '{filename}' was loaded. Stop job...")
                sys.exit()
        except (FileNotFoundError, IOError) as s:
            self.get_logger().info(f"File '{filename}' wasn't loaded yet "
                                   f"or '{self.statistics_path}{self.run_date}.csv' is empty")

    def download_file(self, url):
        filename = url.split('/')[-1]
        local_filename = f"{self.source}{filename}"
        with requests.get(url, stream=True) as r:
            with open(local_filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)

        with gzip.open(local_filename, "rb") as infile:
            # Write the Extracted GZIP file content into outfile using shutil.
            with open(f"{self.extracted}{filename}", "wb") as outfile:
                shutil.copyfileobj(infile, outfile)

        return local_filename, filename

    def gathering_statistics(self, df: pd.DataFrame, file_name: str):
        """
        - date              - date of the fetched_at
        - total_errors      - number of responses that are not 200
        - total_ok          - number of responses that are 200
        - total_distinct_ua - total distinct user agents
        - total_allows      - total sum of allows paths
        - total_disallows   - total sum of disallowed paths
        """
        # TODO: Code refactoring and removing hard codes

        # df = pd.DataFrame({
        #     'fetched_at': ['1111', '1111', '1111', '2222', '2222', '3333'],
        #     'http_code': [200, 200, 400, 200, 200, 500],
        #     'domain': ['aa', 'bb', 'cc', 'dd', 'ff', 'll'],
        #     'user_agent': ['q', 'e', 'q', 'e', 'q', 'e'],
        #     'disallow_cnt': [1, 2, 3, 4, 5, 6],
        #     'allow_cnt': [1, 2, 3, 4, 5, 6]
        # })

        if len(df.index) > 0 and file_name:
            df['file_name'] = file_name

            res = df.groupby(['fetched_at', 'file_name', 'run_date']).agg(
                {'http_code': [('total_errors', lambda x: len(x[x != 200])),
                               ('total_ok', lambda x: len(x[x == 200]))],
                 'user_agent': [('total_distinct_ua', pd.Series.nunique)],
                 'allow_cnt': [('total_allows', 'sum')],
                 'disallow_cnt': [('total_disallows', 'sum')],
                 }).reset_index()

            res.columns = self.statistic_columns

            statistics_file = f'{self.statistics_path}{self.run_date}.csv'
            with open(statistics_file, 'a+') as csv_file:
                res.to_csv(path_or_buf=csv_file, mode=self.file_mode, index=False)

            self.get_logger().info(f"Check statistics file here: '{statistics_file}'")

    def parser(self):
        self.file_mode = 'w+' if self.is_overwriting else 'a+'

        res_df = line_df = record_df = pd.DataFrame(columns=self.columns)

        stream = GZipStream(FileStream(self.input_file_path, 'rb'))
        for record in ArchiveIterator(stream, WarcRecordType.response):
            body = record.reader.read()
            rec_type = record.headers['WARC-Type']
            fetched_at = record.record_date
            http_code = record.http_headers.status_code
            domain = record.headers['WARC-Target-URI']
            data_dict = {}

            for e in self.columns:
                data_dict[e] = None if e in self.none_columns else locals().get(e)

            if http_code != 200:
                record_df = pd.DataFrame([data_dict])
            else:
                try:
                    list_by_lines = body.decode('latin-1').split('\n')
                except TypeError:
                    self.get_logger().info(f"Error though encoding '{self.input_file_path}' file....skip it")
                    # TODO: Need to create handler of encoding error.
                    continue
                for line in list_by_lines:
                    line = line.strip()
                    if line and ":" in line:
                        ll = line.split(':')
                        key = ll[0]
                        val = ll[1] if key not in self.calc_columns else 1
                        if key in self.columns:
                            data_dict[key] = val
                            f = pd.DataFrame([data_dict])
                            line_df = pd.concat([line_df, f], ignore_index=True)

                record_df = pd.concat([record_df, line_df], ignore_index=True)
                record_df = record_df.groupby(self.group_columns, dropna=False)[self.calc_columns].sum().reset_index()

            res_df = pd.concat([res_df, record_df], ignore_index=True)
            res_df = res_df.groupby(self.group_columns, dropna=False)[self.calc_columns]\
                .sum().reset_index()
            res_df['run_date'] = self.run_date
            res_df.columns = self.result_mapping_columns

        return res_df

    def run(self):
        """
        Run the job
        """

        # TODO: Make code refactoring

        self.args = self.parse_arguments()

        for d in self.list_needed_dir:
            if not os.path.exists(d):
                # Create a new directory because it does not exist
                os.makedirs(d)

        self.get_logger().info(f"Run Date: {self.args.run_date}")
        self.get_logger().info("Starting Job...")

        self.run_date = self.args.run_date if self.args.run_date else self.run_date

        self.get_logger().info(f"unloading_url: '{self.args.unloading_url}'")

        res_df = None
        filename = None
        if self.args.unloading_url and self.args.unloading_url != 'None':
            self.input_file_path, filename = self.download_file(self.args.unloading_url)
            self.check_statistics(filename)
            res_df = self.parser()
        else:
            processed_file = 0
            for file in self.get_all_s3_objects(boto3.client('s3'), Bucket=self.bucket, Prefix=self.prefix):
                file_key = file.get('Key')
                if not file_key:
                    self.get_logger().info(f"Problem with File'{file_key}'")
                    continue

                self.check_statistics(file_key.split('/')[-1])

                self.input_file_path, filename = self.download_file(f"{self.base_url}{file_key}")

                res_df = self.parser()

                processed_file += 1

                if processed_file == self.processing_files_at_run:
                    self.get_logger().info(f"Processed number of files '{processed_file}' ==  "
                                           f"processing files at run '{self.processing_files_at_run}'... Break")
                    break

        # Using parquet for better reading/processing performance with partition option
        res_df.to_parquet(path=self.destination,
                          index=False,
                          compression='gzip',
                          engine='pyarrow',
                          partition_cols=self.parquet_partition_cols)

        self.gathering_statistics(res_df, filename)

        if os.path.isfile(self.input_file_path):
            os.remove(self.input_file_path)

        self.get_logger().info(f"Finished processing file: '{filename}'")


if __name__ == '__main__':
    BaseJob().run()
