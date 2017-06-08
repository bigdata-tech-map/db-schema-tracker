#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
from utils.logger import logger
from model.TableLogModel import TableLogModel
from model.ColumnLogModel import ColumnLogModel
import sys

reload(sys)
sys.setdefaultencoding('utf8')

"""快照一个数据库的元数据"""

SQL_SCHEMA_TABLES = u"""
select
TABLE_SCHEMA,
TABLE_NAME,
ENGINE,
TABLE_ROWS,
TABLE_COLLATION,
TABLE_COMMENT
from information_schema.TABLES
where TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA = '%s';
"""

SQL_TABLE_DDL = u"""
show create table %s.%s;
"""

SQL_SCHEMA_COLUMNS = u"""
select
TABLE_SCHEMA,
TABLE_NAME,
COLUMN_NAME,
ORDINAL_POSITION,
COLUMN_DEFAULT,
IS_NULLABLE,
DATA_TYPE,
CHARACTER_MAXIMUM_LENGTH,
CHARACTER_OCTET_LENGTH,
NUMERIC_PRECISION,
NUMERIC_SCALE,
DATETIME_PRECISION,
CHARACTER_SET_NAME,
COLLATION_NAME,
COLUMN_TYPE,
COLUMN_KEY,
EXTRA,
COLUMN_COMMENT
from information_schema.COLUMNS
where TABLE_SCHEMA = '%s' and TABLE_NAME in (%s);
"""


class DBSnapshot(object):
    @staticmethod
    def get_tables(cursor, db):
        cursor.execute(SQL_SCHEMA_TABLES % db)
        return cursor.fetchall()

    @staticmethod
    def get_table_ddl(curosr, db, table):
        curosr.execute(SQL_TABLE_DDL % (db, table))
        return curosr.fetchall()

    @staticmethod
    def get_columns(cursor, tables, db):
        table_names = map(lambda e: e[1], tables)
        logger.info("**** Table Names [%d]: %s" % (len(table_names), ','.join(table_names)))
        table_names = map(lambda e: "'" + e + "'", table_names)
        names_str = ','.join(table_names)
        cursor.execute(SQL_SCHEMA_COLUMNS % (db, names_str))
        return cursor.fetchall()

    @staticmethod
    def snap_shot_tables(session, version, cursor, tables):
        logs = []

        for e in tables:
            log = TableLogModel()
            log.version_id = version.id

            log.TABLE_SCHEMA = e[0]
            log.TABLE_NAME = e[1]
            log.ENGINE = e[2]
            log.TABLE_ROWS = e[3]
            log.TABLE_COLLATION = e[4]
            log.TABLE_COMMENT = e[5]

            ddls = DBSnapshot.get_table_ddl(cursor, log.TABLE_SCHEMA, log.TABLE_NAME)
            if ddls is not None and len(ddls) > 0:
                log.create_ddl = ddls[0][1]

            logs.append(log)

        session.add_all(logs)

    @staticmethod
    def snap_shot_columns(session, version, cursor, columns):
        logs = []

        for e in columns:
            log = ColumnLogModel()
            log.version_id = version.id

            log.TABLE_SCHEMA = e[0]
            log.TABLE_NAME = e[1]
            log.COLUMN_NAME = e[2]
            log.ORDINAL_POSITION = e[3]
            log.COLUMN_DEFAULT = e[4]
            log.IS_NULLABLE = e[5]
            log.DATA_TYPE = e[6]
            log.CHARACTER_MAXIMUM_LENGTH = e[7]
            log.CHARACTER_OCTET_LENGTH = e[8]
            log.NUMERIC_PRECISION = e[9]
            log.NUMERIC_SCALE = e[10]
            log.DATETIME_PRECISION = e[11]
            log.CHARACTER_SET_NAME = e[12]
            log.COLLATION_NAME = e[13]
            log.COLUMN_TYPE = e[14]
            log.COLUMN_KEY = e[15]
            log.EXTRA = e[16]
            log.COLUMN_COMMENT = e[17]

            logs.append(log)
            session.add(log)

        session.flush()
        session.commit()
        # session.add_all(logs)

    @staticmethod
    def snap_shot_mysql(session, version, db_conf):
        logger.info("********************************************************************")
        logger.info("**** To Track DB: %s", db_conf['db'])
        # 获取库中所有表属性
        connection = MySQLdb.connect(**db_conf)
        cursor = connection.cursor()
        tables = DBSnapshot.get_tables(cursor, db_conf['db'])

        # 获取所有表名
        if len(tables) > 0:
            DBSnapshot.snap_shot_tables(session, version, cursor, tables)
            columns = DBSnapshot.get_columns(cursor, tables, db_conf['db'])
            logger.info('**** Columns Count: %d' % len(columns))
            DBSnapshot.snap_shot_columns(session, version, cursor, columns)
        else:
            logger.warn('**** Table count is ZERO')

        cursor.close()
        connection.close()
        logger.info("********************************************************************")
        pass


