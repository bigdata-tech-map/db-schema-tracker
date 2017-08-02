#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import Column, DateTime, String, Text, BigInteger
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text
from bin.ConfigLoader import ConfigLoader
from bin.DiffUtil import DiffUtil
import sys

reload(sys)
sys.setdefaultencoding('utf8')

BaseModel = declarative_base()

TABLE_SCHEMA = u"""
CREATE TABLE if not exists `tracker_column_log` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `version_id` int(11) unsigned NOT NULL COMMENT '版本ID',
  `TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',
  `TABLE_NAME` varchar(64) NOT NULL DEFAULT '',
  `COLUMN_NAME` varchar(64) NOT NULL DEFAULT '',
  `ORDINAL_POSITION` bigint(21) unsigned NOT NULL DEFAULT '0',
  `COLUMN_DEFAULT` longtext,
  `IS_NULLABLE` varchar(3) NOT NULL DEFAULT '',
  `DATA_TYPE` varchar(64) NOT NULL DEFAULT '',
  `CHARACTER_MAXIMUM_LENGTH` bigint(21) unsigned DEFAULT NULL,
  `CHARACTER_OCTET_LENGTH` bigint(21) unsigned DEFAULT NULL,
  `NUMERIC_PRECISION` bigint(21) unsigned DEFAULT NULL,
  `NUMERIC_SCALE` bigint(21) unsigned DEFAULT NULL,
  `DATETIME_PRECISION` bigint(21) unsigned DEFAULT NULL,
  `CHARACTER_SET_NAME` varchar(32) DEFAULT NULL,
  `COLLATION_NAME` varchar(32) DEFAULT NULL,
  `COLUMN_TYPE` longtext NOT NULL,
  `COLUMN_KEY` varchar(3) NOT NULL DEFAULT '',
  `EXTRA` varchar(30) NOT NULL DEFAULT '',
  `COLUMN_COMMENT` varchar(1024) NOT NULL DEFAULT '',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  key `idx_version_id` (`version_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='字段日志表';
"""

SHOWED_COLUMNS_NAMES = ["id", "version_id",
                        "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE",
                        "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION",
                        "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE",
                        "COLUMN_KEY", "EXTRA", "COLUMN_COMMENT"]


def FormatHtmlTableTd(rows, if_th=False, emphasized_fields=None):
    td_template = """<td style="background:#eee;">%s</td>"""
    td_emphasized_template = """<td style="background:#eee; color:red;">%s</td>"""

    if if_th:
        td_template = """<th style="background:#ccc;">%s</th>"""

    tr_str = ''
    for e in rows:
        td_array = []
        for f in SHOWED_COLUMNS_NAMES:
            td_array.append((td_emphasized_template if emphasized_fields is not None and f in emphasized_fields else td_template) % getattr(e, f, ""))
        tr_str += "\n<tr>%s</tr>" % ("".join(td_array))

    return tr_str


class ColumnLogModel(BaseModel):
    __tablename__ = 'tracker_column_log'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }

    def __repr__(self):
        return '%s(id=%s, version_id=%s, ' \
               'TABLE_SCHEMA="%s", TABLE_NAME="%s", COLUMN_NAME="%s", ORDINAL_POSITION=%s, COLUMN_DEFAULT="%s", ' \
               'IS_NULLABLE="%s", DATA_TYPE="%s", CHARACTER_MAXIMUM_LENGTH=%s, CHARACTER_OCTET_LENGTH=%s,' \
               'NUMERIC_PRECISION=%s, NUMERIC_SCALE=%s, DATETIME_PRECISION=%s, CHARACTER_SET_NAME="%s",' \
               'COLLATION_NAME="%s", COLUMN_TYPE="%s", COLUMN_KEY="%s", EXTRA="%s", COLUMN_COMMENT="%s")' \
               % (self.__class__.__name__, self.id, self.version_id,
                  self.TABLE_SCHEMA, self.TABLE_NAME, self.COLUMN_NAME, self.ORDINAL_POSITION, self.COLUMN_DEFAULT,
                  self.IS_NULLABLE, self.DATA_TYPE, self.CHARACTER_MAXIMUM_LENGTH, self.CHARACTER_OCTET_LENGTH,
                  self.NUMERIC_PRECISION, self.NUMERIC_SCALE, self.DATETIME_PRECISION, self.CHARACTER_SET_NAME,
                  self.COLLATION_NAME, self.COLUMN_TYPE, self.COLUMN_KEY, self.EXTRA, self.COLUMN_COMMENT)

    id = Column(INTEGER(unsigned=True), primary_key=True)
    version_id = Column(INTEGER(unsigned=True))

    TABLE_SCHEMA = Column(String(64))
    TABLE_NAME = Column(String(64))
    COLUMN_NAME = Column(String(64))
    ORDINAL_POSITION = Column(BigInteger)
    COLUMN_DEFAULT = Column(Text)
    IS_NULLABLE = Column(String(3))
    DATA_TYPE = Column(String(64))
    CHARACTER_MAXIMUM_LENGTH = Column(BigInteger)
    CHARACTER_OCTET_LENGTH = Column(BigInteger)
    NUMERIC_PRECISION = Column(BigInteger)
    NUMERIC_SCALE = Column(BigInteger)
    DATETIME_PRECISION = Column(BigInteger)
    CHARACTER_SET_NAME = Column(String(32))
    COLLATION_NAME = Column(String(32))
    COLUMN_TYPE = Column(Text)
    COLUMN_KEY = Column(String(3))
    EXTRA = Column(String(30))
    COLUMN_COMMENT = Column(String(1024))

    created_at = Column(DateTime, server_default="true")
    updated_at = Column(DateTime, server_default="true")

    @staticmethod
    def CreateTable(session):
        session.execute(text(TABLE_SCHEMA))

    @staticmethod
    def GetHtmlTrs(rows):
        dics = {}
        for x in SHOWED_COLUMNS_NAMES:
            dics[x] = x
        th_obj = ColumnLogModel(**dics)

        print th_obj
        tr_array = [FormatHtmlTableTd([th_obj], True)]
        for e in rows:
            tr_array.append(FormatHtmlTableTd([e]))

        return "".join(tr_array)

    @staticmethod
    def GetHtmlDiffTrs(diffx):
        dics = {}
        for x in SHOWED_COLUMNS_NAMES:
            dics[x] = x
        th_obj = ColumnLogModel(**dics)

        tr_array = [FormatHtmlTableTd([th_obj], True)]
        for e in diffx:
            tr_array.append(FormatHtmlTableTd(e['diff_rows'], False, e['diff_fields']))
        return "".join(tr_array)

    @staticmethod
    def Diff(session, version_id_l, version_id_r, table=None):
        if version_id_l is None or version_id_r is None:
            raise ValueError("diff version_id is error!!!")

        results_l = session.query(ColumnLogModel).filter(ColumnLogModel.version_id == version_id_l).all()
        results_r = session.query(ColumnLogModel).filter(ColumnLogModel.version_id == version_id_r).all()

        (columns, primary_keys) = ConfigLoader.diffColumns()
        return DiffUtil.diff(results_l, results_r, columns, primary_keys)
