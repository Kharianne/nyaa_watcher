import unittest
import sqlite3
import os
import lib
from lib.db import Database
import time

NOW = time.time()
MOCK_DATA = {
    'queries': [(1, 'a', NOW - 100, 100),
                (2, 'b', NOW - 200, 200),
                (3, 'c', NOW - 300, 300)],
    'query2torrent': [(1, 1),
                      (1, 2),
                      (1, 5),
                      (2, 1),
                      (2, 4),
                      (3, 6)],
    'torrents': [(1, 'test_1'),
                 (2, 'test_2'),
                 (3, 'test_3'),
                 (4, 'test_4'),
                 (5, 'test_5'),
                 (6, 'test_6')]
}


def prepare_database():
    conn = sqlite3.connect(':memory:')
    conn.execute("PRAGMA foreign_keys = 1")
    cur = conn.cursor()
    with open(os.path.join(os.path.dirname(lib.__file__),
                           'migrations/base_migration')) as script:
        cur.executescript(script.read())
    return conn, cur


def populate_db(cur):
    cur.executemany("""
            INSERT INTO queries (id, query, last_used, created) VALUES (?, ?, ?, ?)
        """, MOCK_DATA['queries'])

    cur.executemany("""
            INSERT INTO torrents (id, name) VALUES (?, ?)
        """, MOCK_DATA['torrents'])

    cur.executemany("""
            INSERT INTO query2torrent (query_id, torrent_id) VALUES (?, ?)
        """, MOCK_DATA['query2torrent'])


class TestDBPrune(unittest.TestCase):

    def setUp(self) -> None:
        self.db = Database(':memory:')
        self.db.full_db_path = ':memory:'
        self.db.connect()
        populate_db(self.db.cur)

    def tearDown(self) -> None:
        self.db.close_conn()

    def test_delete_right_query(self):
        self.db.prune(200, NOW)
        self.assertEqual([MOCK_DATA['queries'][0]],
                         self.db.cur.execute('SELECT * FROM queries')
                         .fetchall())

    def test_delete_correct_binding(self):
        self.db.prune(200, NOW)
        self.assertEqual([MOCK_DATA['query2torrent'][0],
                          MOCK_DATA['query2torrent'][1],
                          MOCK_DATA['query2torrent'][2]],
                         self.db.cur.execute('SELECT * FROM query2torrent')
                         .fetchall())

    def test_delete_correct_torrents(self):
        self.db.prune(200, NOW)
        self.assertEqual([MOCK_DATA['torrents'][0],
                          MOCK_DATA['torrents'][1],
                          MOCK_DATA['torrents'][4]],
                         self.db.cur.execute('SELECT id, name FROM torrents')
                         .fetchall())

    def test_do_not_delete(self):
        self.db.prune(301, NOW)
        self.assertEqual(MOCK_DATA['queries'],
                         self.db.cur.execute('SELECT * FROM queries')
                         .fetchall())

