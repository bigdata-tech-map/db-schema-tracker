#!/usr/bin/env python
# -*- coding: utf-8 -*-


from utils.logger import logger
from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text
from sqlalchemy import desc
import sys

reload(sys)
sys.setdefaultencoding('utf8')

BaseModel = declarative_base()

TABLE_SCHEMA = u"""
CREATE TABLE if not exists `tracker_version` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `tag` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'tag, 直接用日期格式',
  `note` varchar(256) NOT NULL DEFAULT '' COMMENT '备注',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='版本记录表';
"""


class TrackVersionModel(BaseModel):
    __tablename__ = 'tracker_version'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }

    def __repr__(self):
        return '%s(id=%s, tag="%s")' % (self.__class__.__name__, self.id, self.tag)

    id = Column(INTEGER(unsigned=True), primary_key=True)
    tag = Column(DateTime, server_default="true")
    note = Column(String(256), server_default="true")
    created_at = Column(DateTime, server_default="true")
    updated_at = Column(DateTime, server_default="true")

    @staticmethod
    def CreateTable(session):
        session.execute(text(TABLE_SCHEMA))

    @staticmethod
    def InsertOneVersion(session, note=None):
        version = TrackVersionModel()
        version.note = note
        session.add(version)
        session.commit()
        return version

    @staticmethod
    def QueryWithTag(session, tag):
        results = session.query(TrackVersionModel.id, TrackVersionModel.tag).filter(TrackVersionModel.tag == tag).all()
        logger.debug('QueryWithTag: %s', results)
        if len(results) == 1:
            return results

    @staticmethod
    def QueryWithLastOrder(session, order):
        results = session.query(TrackVersionModel.id, TrackVersionModel.tag).order_by(desc(TrackVersionModel.id)).slice(order, order+1).all()
        logger.debug('QueryWithLastOrder: %s', results)
        if len(results) == 1:
            return results




