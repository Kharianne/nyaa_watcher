import os
import time
import argparse
from dataclasses import dataclass, field
from typing import List
import requests
from lxml import html
import sys
import traceback
from lib.db import Database
import lib.utils


class Downloader:
    @staticmethod
    def get(url):
        try:
            r = requests.get(url)
            if r.status_code != 200:
                raise Exception(f'Request ended with status code: '
                                f'{r.status_code}')
            return r.text
        except Exception as e:
            raise RuntimeError("Could not connect to host") from e


@dataclass
class TorrentRow:
    category: str
    name: str
    size: str
    magnet: str
    uploaded: int
    created: int
    id: int

    def __str__(self):
        return f'[{self.category}] {self.name} ({self.size}) [{self.id}] {self.magnet}' \
               f' {self.uploaded} {self.created}'


@dataclass
class ParseResult:
    next_page_url: str = field(default=None)
    rows: List[TorrentRow] = field(default_factory=list)
    continue_searching: bool = field(default=True)


class Parser:
    def __init__(self, config):
        self.config = config

    def parse(self, page, latest_id):
        result = ParseResult()

        tree = html.fromstring(page)
        try:
            result.next_page_url = self.config.BASE_URL \
                               + tree.xpath(self.config.NEXT_PAGE_SELECTOR)[0]
        except IndexError:
            pass

        rows = tree.xpath(self.config.ROW_SELECTOR)
        for row in rows:
            if (_id := row.xpath(self.config.DATA_SELECTOR_ID)[0]) == \
                    latest_id:
                result.continue_searching = False
                break
            result.rows.append(TorrentRow(
                category=row.xpath(self.config.DATA_SELECTOR_CATEGORY)[0],
                name=row.xpath(self.config.DATA_SELECTOR_NAME)[0],
                size=row.xpath(self.config.DATA_SELECTOR_SIZE)[0],
                magnet=row.xpath(self.config.DATA_SELECTOR_MAGNET)[0],
                uploaded=int(row.xpath(self.config.DATA_SELECTOR_UPLOADED)[0]),
                created=int(time.time()),
                id=int(_id.split('/')[2]),
            ))
        return result


class Driver:
    def __init__(self, config, query, downloader, parser, db):
        self.config = config
        self.query = query
        self.downloader = downloader
        self.parser = parser
        self.db = db

    def run(self):
        self.db.connect()
        query_id = self.db.get_query_id(self.query)
        latest_id = self.db.get_last_id(query_id)
        url = self.config.first_page(self.query)
        continue_searching = True
        while url and continue_searching:
            page = self.downloader.get(url)
            parse_result = self.parser.parse(page, latest_id)
            self.db.add_torrents_to_query(query_id, parse_result.rows)
            continue_searching = parse_result.continue_searching
            url = parse_result.next_page_url
        return self.db.fetch_all_torrents(query_id), self.db.get_headers()


def run_parsing(config):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    # Search parser group
    search_parser = subparsers.add_parser('search')
    search_parser.add_argument('QUERY', action='store', type=str, default=False)
    search_parser.add_argument('--output', '-o', action='store', required=False,
                               default='json',
                               choices=['tsv', 'json', 'binary'])
    search_parser.add_argument('--columns', '-c', action='store', required=False,
                               help='Coma separated names of columns')

    # Prune parser group
    prune_parser = subparsers.add_parser('prune')
    prune_parser.add_argument('PRUNE', action='store', type=int, default=False)

    args = parser.parse_args()

    try:
        db_path = os.environ['DB_PATH']
    except KeyError:
        print("Env variable DB_PATH is missing!", file=sys.stderr)
        exit(1)

    db = Database(db_path)
    if 'QUERY' in args:
        # Scrape and download
        down = Downloader()
        parser = Parser(config)
        d = Driver(config, args.QUERY, down, parser, db)
        torrents, headers = d.run()

        # Format and print
        fm = lib.utils.Formatter(headers, torrents)
        if args.columns:
            fm.filter_data(args.columns.split(','))
        fm.format_data(args.output)
        fm.print_data()

    elif 'PRUNE' in args:
        # Prune DB
        db.connect()
        db.prune(args.PRUNE, time.time())

        try:
            pass

        except:
            traceback.print_exc()
            exit(1)
