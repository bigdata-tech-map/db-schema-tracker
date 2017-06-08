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
CREATE TABLE if not exists `tracker_table_log` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `version_id` int(11) unsigned NOT NULL COMMENT '版本ID',
  `TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',
  `TABLE_NAME` varchar(64) NOT NULL DEFAULT '',
  `ENGINE` varchar(64) DEFAULT NULL,
  `TABLE_ROWS` bigint(21) unsigned DEFAULT NULL,
  `TABLE_COLLATION` varchar(32) DEFAULT NULL,
  `TABLE_COMMENT` varchar(2048) NOT NULL DEFAULT '',
  `create_ddl` text COLLATE utf8_unicode_ci COMMENT '建表语句',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='表变更记录';
"""

SHOWED_COLUMNS_NAMES = ["id", "version_id",
                        "TABLE_SCHEMA", "TABLE_NAME", "ENGINE", "TABLE_COLLATION", "TABLE_COMMENT"]


def FormatHtmlTableTd(rows, if_th=False, emphasized_fields=None):
    td_template = """<td style="background:#eee;">%s</td>"""
    td_emphasized_template = """<td style="background:#eee; color:red;">%s</td>"""

    if if_th:
        td_template = """<th style="background:#ccc;">%s</th>"""

    td_array = []
    for e in rows:
        for f in SHOWED_COLUMNS_NAMES:
            td_array.append((td_emphasized_template if emphasized_fields is not None and f in emphasized_fields else td_template) % getattr(e, f, ""))

    tr_str = "\n<tr>%s</tr>" % ("".join(td_array))
    return tr_str


class TableLogModel(BaseModel):
    __tablename__ = 'tracker_table_log'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }

    def __repr__(self):
        return '%s(id=%s, version_id=%s, ' \
               'TABLE_SCHEMA="%s", TABLE_NAME="%s", ENGINE="%s", TABLE_ROWS=%s, TABLE_COLLATION="%s", TABLE_COMMENT="%s")' \
               % (self.__class__.__name__, self.id, self.version_id,
                  self.TABLE_SCHEMA,self.TABLE_NAME, self.ENGINE, self.TABLE_ROWS, self.TABLE_COLLATION, self.TABLE_COMMENT)

    id = Column(INTEGER(unsigned=True), primary_key=True)
    version_id = Column(INTEGER(unsigned=True))

    TABLE_SCHEMA = Column(String(64))
    TABLE_NAME = Column(String(64))
    ENGINE = Column(String(64))
    TABLE_ROWS = Column(BigInteger)
    TABLE_COLLATION = Column(String(32))
    TABLE_COMMENT = Column(String(2048))

    create_ddl = Column(Text)
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
        th_obj = TableLogModel(**dics)

        tr_array = [FormatHtmlTableTd([th_obj], True)]
        for e in rows:
            tr_array.append(FormatHtmlTableTd([e]))

        return "".join(tr_array)

    @staticmethod
    def GetHtmlDiffTrs(diffx):
        dics = {}
        for x in SHOWED_COLUMNS_NAMES:
            dics[x] = x
        th_obj = TableLogModel(**dics)

        tr_array = [FormatHtmlTableTd([th_obj, th_obj], True)]
        for e in diffx:
            tr_array.append(FormatHtmlTableTd(e['diff_rows'], False, e['diff_fields']))
        return "".join(tr_array)

    @staticmethod
    def Diff(session, version_id_l, version_id_r, table=None):
        if version_id_l is None or version_id_r is None:
            raise ValueError("diff version_id is error!!!")

        results_l = session.query(TableLogModel).filter(TableLogModel.version_id == version_id_l).all()
        results_r = session.query(TableLogModel).filter(TableLogModel.version_id == version_id_r).all()

        (columns, primary_keys) = ConfigLoader.diffTableColumns()
        return DiffUtil.diff(results_l, results_r, columns, primary_keys)
