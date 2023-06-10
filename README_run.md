# Description


## How to work with environment

For running:
- you can build and run docker compose.

```
docker compose up --build
```

- or local run:
```
sh ./run.sh "local_run" "" "https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/segments/1679296950528.96/robotstxt/CC-MAIN-20230402105054-20230402135054-00799.warc.gz" "" ""
```
or
```
sh ./run.sh "local_run" "" "None" "" ""
```

For control loading process we can pass input arguments for app:`

```
--run_date: "Date of process",
--unloading_url: "Path to file listing input paths", default:'https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/segments/1679296950528.96/robotstxt/CC-MAIN-20230402105054-20230402135054-00799.warc.gz'
--processing_s3_files_at_run: "Count of files from s3 to parse at run if unloading_url passed as None", default=1
--is_overwriting: "Overwriting processed files", default=True
```
Run through docker compose Doesn't work for unloading data from CommonCrawl s3 (when unloading_url doesn't pass). Need to have the access to AWS (~/.aws/credentials)

for adjustment in docker compose we can use env_file: `.env`

### If pass --unloading_url argument as `None` the job will unload files from segment: s3://commoncrawl/crawl-data/CC-MAIN-2023-14/segments/1679296950528.96/robotstxt/
### For using unloading_url as None (unload from CommonCrawl s3) need to have the access to AWS (~/.aws/credentials)

Questions to consider when working on your solution:

- We expect the data to grow over time:
    - How do you propose to store data considering that we are expected to filter it by its fetched date?
    - `I used parquet files with partition by fetched_at. The local machine isn't a good place to store a lot of files with fast access to it. Better to use AWS S3 and Athena with Glue Data Datalog for fast extracting data from parquet with partition by fetched_at`
    - `TODO: Need to make adjustment of storing data. Currently created a lot of small parquet files`
- How do you propose to test the script?
    - Bonus points for providing unit tests
    - `Didn't finish. Created only template. Need to create Mocking on call aws, request, reading/writing files function`
- We want to run this script on a daily basis. How do you propose to parameterize it?
    - Bonus points for adjusting the Dockerfile to make this ETL easier to run
    - `Added run.sh script and ability run through docker-compose`
