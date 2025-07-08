from asyncio import CancelledError
from contextlib import suppress

from aiosqlite import Row, connect

from ..custom import PROJECT_ROOT

__all__ = ["Database"]


class Database:
    __FILE = "DouK-Downloader.db"

    def __init__(
        self,
    ):
        self.file = PROJECT_ROOT.joinpath(self.__FILE)
        self.database = None
        self.cursor = None

    async def __connect_database(self):
        self.database = await connect(self.file)
        self.database.row_factory = Row
        self.cursor = await self.database.cursor()
        await self.__create_table()
        await self.__write_default_config()
        await self.__write_default_option()
        await self.database.commit()

    async def __create_table(self):
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS config_data (
            NAME TEXT PRIMARY KEY,
            VALUE INTEGER NOT NULL CHECK(VALUE IN (0, 1))
            );"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS download_data (
            ID TEXT PRIMARY KEY, 
            PUBLISH_TIME TEXT, 
            UID TEXT, 
            MARK TEXT, 
            DIGG_COUNT INTEGER, 
            COMMENT_COUNT INTEGER, 
            COLLECT_COUNT INTEGER, 
            SHARE_COUNT INTEGER, 
            PLAY_COUNT INTEGER, 
            PATH TEXT);"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS mapping_data (
        ID TEXT PRIMARY KEY,
        NAME TEXT NOT NULL,
        MARK TEXT NOT NULL
        );"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS option_data (
        NAME TEXT PRIMARY KEY,
        VALUE TEXT NOT NULL
        );"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS upload_data (
            ACCOUNT_UID TEXT,
            ACCOUNT_MARK TEXT,   
            VIDEO_ID TEXT,
            FROM_ACCOUNT_UID TEXT,
            FROM_ACCOUNT_MARK TEXT,
            UPLOAD_TIME TEXT,
            PRIMARY KEY (ACCOUNT_UID,ACCOUNT_MARK,VIDEO_ID)
            );"""
        )

    async def __write_default_config(self):
        await self.database.execute(
            """INSERT OR IGNORE INTO config_data (NAME, VALUE)
                            VALUES ('Record', 1),
                            ('Logger', 0),
                            ('Disclaimer', 0);"""
        )

    async def __write_default_option(self):
        await self.database.execute(
            """INSERT OR IGNORE INTO option_data (NAME, VALUE)
                            VALUES ('Language', 'zh_CN');"""
        )

    async def read_config_data(self):
        await self.cursor.execute("SELECT * FROM config_data")
        return await self.cursor.fetchall()

    async def read_option_data(self):
        await self.cursor.execute("SELECT * FROM option_data")
        return await self.cursor.fetchall()

    async def update_config_data(
        self,
        name: str,
        value: int,
    ):
        await self.database.execute(
            "REPLACE INTO config_data (NAME, VALUE) VALUES (?,?)", (name, value)
        )
        await self.database.commit()

    async def update_option_data(
        self,
        name: str,
        value: str,
    ):
        await self.database.execute(
            "REPLACE INTO option_data (NAME, VALUE) VALUES (?,?)", (name, value)
        )
        await self.database.commit()

    async def update_mapping_data(self, id_: str, name: str, mark: str):
        await self.database.execute(
            "REPLACE INTO mapping_data (ID, NAME, MARK) VALUES (?,?,?)",
            (id_, name, mark),
        )
        await self.database.commit()

    async def read_mapping_data(self, id_: str):
        await self.cursor.execute(
            "SELECT NAME, MARK FROM mapping_data WHERE ID=?", (id_,)
        )
        return await self.cursor.fetchone()

    async def has_download_data(self, id_: str) -> bool:
        await self.cursor.execute("SELECT ID FROM download_data WHERE ID=?", (id_,))
        return bool(await self.cursor.fetchone())

    async def read_download_data(
        self,
        id_: str = None,
        uid: str = None,
        mark: str = None,
        publish_time: str = None,
    ):
        sql = "SELECT * FROM download_data WHERE 1=1 "
        args = ()
        if id_:
            sql += "AND ID=? "
            args += (id,)
        if uid:
            sql += "AND UID=? "
            args += (uid,)
        if mark:
            sql += "AND MARK=? "
            args += (mark,)
        if publish_time:
            sql += "AND date(PUBLISH_TIME)>=date(?) "
            args += (publish_time,)
        sql += "ORDER BY DIGG_COUNT DESC"
        await self.cursor.execute(sql, args)
        return await self.cursor.fetchall()

    async def write_download_data(
        self,
        id_: str,
        publish_time: str,
        uid: str,
        mark: str,
        digg_count: int,
        comment_count: int,
        collect_count: int,
        share_count: int,
        play_count: int,
        path: str,
    ):
        await self.database.execute(
            "INSERT OR IGNORE INTO download_data (ID, PUBLISH_TIME, UID, MARK, DIGG_COUNT, COMMENT_COUNT, COLLECT_COUNT, SHARE_COUNT, PLAY_COUNT, PATH) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            (
                id_,
                publish_time,
                uid,
                mark,
                digg_count,
                comment_count,
                collect_count,
                share_count,
                play_count,
                path,
            ),
        )
        await self.database.commit()

    async def delete_download_data(self, ids: list | tuple | str):
        if not ids:
            return
        if isinstance(ids, str):
            ids = [ids]
        [await self.__delete_download_data(i) for i in ids]
        await self.database.commit()

    async def __delete_download_data(self, id_: str):
        await self.database.execute("DELETE FROM download_data WHERE ID=?", (id_,))

    async def delete_all_download_data(self):
        await self.database.execute("DELETE FROM download_data")
        await self.database.commit()

    async def write_upload_data(
        self,
        account_uid: str,
        account_mark: str,
        video_id: str,
        from_account_uid: str,
        from_mark: str,
    ):
        await self.database.execute(
            "INSERT OR IGNORE INTO upload_data (ACCOUNT_UID, ACCOUNT_MARK, VIDEO_ID, FROM_ACCOUNT_UID, FROM_ACCOUNT_MARK, UPLOAD_TIME) VALUES (?, ?, ?, ?, ?, datetime('now'));",
            (
                account_uid,
                account_mark,
                video_id,
                from_account_uid,
                from_mark,
            ),
        )
        await self.database.commit()

    async def has_upload_data(self, account_uid: str, id_: str) -> bool:
        await self.cursor.execute(
            "SELECT VIDEO_ID FROM upload_data WHERE ACCOUNT_UID=? AND VIDEO_ID=?",
            (account_uid, id_),
        )
        return bool(await self.cursor.fetchone())

    async def __aenter__(self):
        await self.__connect_database()
        return self

    async def close(self):
        with suppress(CancelledError):
            await self.cursor.close()
        await self.database.close()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
