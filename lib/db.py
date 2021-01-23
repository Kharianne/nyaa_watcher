import sqlite3
from sqlite3 import Error
import os.path
import time


class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.db_name = 'nyaa.db'

        self.full_db_path = os.path.join(self.db_path, self.db_name)
        self.existed = self.did_exist()

        self.conn = None
        self.cur = None

    def did_exist(self):
        return os.path.isfile(self.full_db_path)

    def connect(self):
        self.conn = sqlite3.connect(self.full_db_path)
        self.conn.execute("PRAGMA foreign_keys = 1")
        self.cur = self.conn.cursor()

        if not self.existed:
            with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                   'migrations/base_migration')) as script:
                self.cur.executescript(script.read())

    def close_conn(self):
        self.conn.close()

    def get_query_id(self, query):
        now = int(time.time())
        self.cur.execute("""
            INSERT INTO queries (query, last_used, created) VALUES (?, ?, ?)
            ON CONFLICT(query) DO UPDATE SET last_used = excluded.last_used;
        """, (query, now, now))

        self.cur.execute('SELECT id FROM queries WHERE query=(?)', (query,))
        query_id = self.cur.fetchone()[0]

        self.conn.commit()
        return query_id

    def get_last_id(self, query_id):
        self.cur.execute("""
            SELECT torrent_id FROM query2torrent
            WHERE query_id=(?) 
            ORDER BY torrent_id DESC
        """, (query_id,))
        row = self.cur.fetchone()
        return row[0] if row else None

    def add_torrents_to_query(self, query_id, torrents):
        now = int(time.time())
        values = [(torrent.id,
                   torrent.name,
                   torrent.magnet,
                   torrent.category,
                   torrent.size,
                   torrent.uploaded,
                   now
                   ) for torrent in torrents]
        self.cur.executemany("""
            INSERT INTO torrents VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING
        """, values)

        values = [(query_id, torrent.id) for torrent in torrents]
        self.cur.executemany("""
            INSERT INTO query2torrent VALUES (?, ?)
            ON CONFLICT(query_id, torrent_id) DO NOTHING
        """, values)

        self.conn.commit()

    def fetch_all_torrents(self, query_id):
        self.cur.execute("""
            SELECT * FROM torrents LEFT JOIN query2torrent ON
            torrents.id = query2torrent.torrent_id
            WHERE query2torrent.query_id=(?)
            ORDER BY torrents.id DESC
        """, (query_id,))
        return self.cur.fetchall()

    def get_headers(self):
        self.cur.execute('PRAGMA table_info(torrents)')
        return [column[1] for column in self.cur.fetchall()]

    def prune(self, secs, now):
        diff = now - secs

        self.cur.execute("""
            SELECT query FROM queries
            WHERE queries.last_used <= ?
        """, (diff,))
        queries_to_delete = self.cur.fetchall()

        if queries_to_delete:
            # Delete from query table; ON CASCADE deletes from query2torrent table
            self.cur.execute("""
               DELETE FROM queries WHERE queries.last_used <= ?
            """, (diff,))

            # Delete from torrents table
            self.cur.execute("""
                DELETE FROM torrents WHERE id IN
                    (SELECT t.id FROM torrents as t LEFT JOIN query2torrent as qt 
                    ON t.id = qt.torrent_id
                    WHERE qt.query_id IS NULL)
            """)
            deleted_torrents = self.cur.rowcount
            print(f"{len(queries_to_delete)} row/s were deleted:")
            for query in queries_to_delete:
                print(';'.join(map(str, query)))
            print(f"Number of deleted torrents: {deleted_torrents}")
        else:
            print("Nothing to prune")

