# coding-interview-data-engineer

# Description

This is coding exercise for Data Engineer position at [Craft.co](http://craft.co/).

## Prerequisities

- Knowledge of Python, SQL, Docker, Git, Unit Testing
- Knowledge of Python libraries such as Pandas, SQLAlchemy, Pytest, AWSWrangler and Boto3

Working environment with Python and Docker.

The coding exercise should not take more than 1 hour. If you are not able to finish it in time, provide a short description of what you would do next.

## How to work with dockerized environment

First you have to build the docker image:

```
docker build -t craft-data-coding .
```

Run the app

```
docker run craft-data-coding
```

For your convenience you can also use the `Makefile` or `docker-compose` - we invite you to customize it to your needs.

## Description of the problem

The goal of this exercise is to parse and extract different statistics from the robots.txt files.
We will use the data from the CommonCrawl project ([https://commoncrawl.org/](https://commoncrawl.org/)).
CommonCrawl is a project that crawls the web and stores the data in a public S3 bucket using WARC format ([https://en.wikipedia.org/wiki/Web_ARChive](https://en.wikipedia.org/wiki/Web_ARChive)).

The robots.txt ([https://en.wikipedia.org/wiki/Robots_exclusion_standard](https://en.wikipedia.org/wiki/Robots_exclusion_standard)) is a file that is used by web crawlers to identify which parts of the website should not be crawled. The file is located in the root of the website and it is expected to be in the following format:

```
User-agent: Googlebot
Disallow: /images
```

Questions to consider when working on your solution:

- We expect the data to grow over time:
    - How do you propose to store data considering that we are expected to filter it by its fetched date?
- How do you propose to test the script?
    - Bonus points for providing unit tests
- We want to run this script on a daily basis. How do you propose to parameterize it?
    - Bonus points for adjusting the Dockerfile to make this ETL easier to run

# Step 0

Unzip the archive that you received by email.
Finish excercise.
Compress it and send it back to our recruiters. Do not include the downloaded data in the archive.

Remember to adjust the requirements.txt by adding all the Python libraries you might need to complete the task.

# Step 1

Create a Python script that does the following:

1. Downloads the WARC file from the following URL: `https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/segments/1679296950528.96/robotstxt/CC-MAIN-20230402105054-20230402135054-00799.warc.gz` and uncompresses it to `data/raw/` folder.

2. Extracts data from that file and saves it to `data/extracted/` directory.

The WARC file contains three types of records which are designated by the `WARC-Type` field. We are interested only in the `response` type (and discard the `request` and `metadata` types).

From the `response` records extract following fields:

- fetched_at   - Timestamp of the response taken from the `WARC-Date` field
- domain       - Domain of the website taken from the `WARC-Target-URI` field
- http_code    - Extracted from the HTTP headers (if the response is not `200`, set all the other fields to null)
- user_agent   - User agent taken from the User-Agent field of the `robots.txt`
- disallow_cnt - Number of disallowed paths
- allow_cnt    - Number of allowed paths

Example Response WARC entry:

```
WARC/1.0
WARC-Type: response
WARC-Date: 2023-04-02T10:55:08Z
WARC-Record-ID: <urn:uuid:f00a8b0a-1761-496e-8539-9c0c56fd1af1>
Content-Length: 363
Content-Type: application/http; msgtype=response
WARC-Warcinfo-ID: <urn:uuid:686eb1aa-5403-454e-8ff9-de49cdb9446e>
WARC-Concurrent-To: <urn:uuid:592c2c04-d54e-4037-8e75-df94224626f3>
WARC-IP-Address: 35:.75.255.9
WARC-Target-URI: <https://young.tonymctony.com/robots.txt>
WARC-Payload-Digest: sha1:FYASLUT3UOF4WSBWKX7KW62HIJIOISMM
WARC-Block-Digest: sha1:AWVNEERB6SYC37BFADZANMLBTKU3ITQ3
WARC-Identified-Payload-Type: text/x-robots

HTTP/1.1 200 OK
Server: nginx
Date: Sun, 02 Apr 2023 10:55:08 GMT
Content-Type: text/plain
Content-Length: 194
Connection: keep-alive
X-Proxy-Revision: 93d1fc7

User-agent: *
Sitemap: <https://young.tonymctony.com/sitemap_index.xml>
Disallow: /api/
Disallow: /draft/
Disallow: /preview

User-agent: Mediapartners-Google
Disallow: /draft/
Disallow: /preview
```

The output file should look like this:

```
fetched_at,http_code,domain,user_agent,disallow_cnt,allow_cnt
2023-04-02T10:55:08Z,200,*,young.tonymctony.com,3,0
2023-04-02T10:55:08Z,200,Mediapartners-Google,young.tonymctony.com,2,0
```

Note: You can choose any file format you are comfortable with just add one comment why you chose it.

# Step 2

We are interested in gathering statistics about the daily runs. Provide the aggregates to a separate file that will contain:

- date              - date of the fetched_at
- total_errors      - number of responses that are not 200
- total_ok          - number of responses that are 200
- total_distinct_ua - total distinct user agents
- total_allows      - total sum of allows paths
- total_disallows   - total sum of disallowed paths

Save the output file to the `data/statistics/` directory.

# Submitting the code

Once completed, send us a compressed archive of your code.
We will also appreciate feedback about this exercise so we can adjust it for future candidates.
