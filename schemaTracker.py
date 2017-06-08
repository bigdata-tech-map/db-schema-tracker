#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
from optparse import OptionParser
from optparse import OptionGroup
from bin.ConfigLoader import ConfigLoader
from bin.DBSnapshot import DBSnapshot
from bin.OutputDiff import OutputDiff
from utils.logger import logger
from model.TrackVersionModel import TrackVersionModel
from model.TableLogModel import TableLogModel
from model.ColumnLogModel import ColumnLogModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys

reload(sys)
sys.setdefaultencoding('utf8')


def Loader_DB_Configs():
    ConfigLoader.shareInstance().load_configs()
    logger.debug(ConfigLoader.shareInstance().metadata_db)


def Get_One_Session(db_conf):
    db_configs = (db_conf['user'], db_conf['passwd'], db_conf['host'], db_conf['port'], db_conf['db'])
    engine = create_engine('mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8' % db_configs, echo=False, encoding=db_conf['charset'])
    Session = sessionmaker(bind=engine)

    session = Session()
    return session


def Init_Meta_Db():
    Loader_DB_Configs()
    (tracking_dbs, metadata_db) = (ConfigLoader.shareInstance().tracking_dbs, ConfigLoader.shareInstance().metadata_db)
    session = Get_One_Session(metadata_db)

    try:
        TrackVersionModel.CreateTable(session)
        TableLogModel.CreateTable(session)
        ColumnLogModel.CreateTable(session)
    except BaseException, se:
        logger.error(se)
        session.rollback()
        session.commit()

    session.close()
    pass


def Track_One_Version(note):
    logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    # get db configs needed to track
    Loader_DB_Configs()
    (tracking_dbs, metadata_db) = (ConfigLoader.shareInstance().tracking_dbs, ConfigLoader.shareInstance().metadata_db)
    session = Get_One_Session(metadata_db)

    version = TrackVersionModel.InsertOneVersion(session, note)
    logger.info("** {}".format(version))

    try:
        for db_conf in tracking_dbs:
            DBSnapshot.snap_shot_mysql(session, version, db_conf)

        session.commit()
    except BaseException, se:
        logger.error(se)
        session.rollback()
        session.delete(version)
        session.commit()

    session.close()
    logger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    pass


def Diff_Versions_With_Versions(session, ver1, ver2, emails):
    if ver1 is not None and len(ver1) == 1 and ver2 is not None and len(ver2) == 1:
        logger.info("Tag %s, version_id is %s" % (ver1[0].tag, ver1[0].id))
        logger.info("Tag %s, version_id is %s" % (ver2[0].tag, ver2[0].id))
        diff_table_results = TableLogModel.Diff(session, ver1[0].id, ver2[0].id)
        diff_column_results = ColumnLogModel.Diff(session, ver1[0].id, ver2[0].id)
        out_path = OutputDiff.Output(ver1[0], ver2[0], diff_table_results, diff_column_results)
        logger.info("Diff Out File: %s" % out_path)
        subject = "Tracked DB Diff Results: [%s-%s]" % (ver1[0].id, ver2[0].id)
        OutputDiff.SendEmail(subject, out_path, emails)
    else:
        raise ValueError("not found version ['%s'] or ['%s']" % (ver1[0].tag, ver2[0].tag))
    pass


def Diff_Versions_With_Tags(left, right, emails):
    # get db configs needed to track
    Loader_DB_Configs()
    (tracking_dbs, metadata_db) = (ConfigLoader.shareInstance().tracking_dbs, ConfigLoader.shareInstance().metadata_db)
    session = Get_One_Session(metadata_db)

    ver1 = TrackVersionModel.QueryWithTag(session, left)
    ver2 = TrackVersionModel.QueryWithTag(session, right)
    Diff_Versions_With_Versions(session, ver1, ver2, emails)

    session.close()
    pass


def Diff_Versions_With_Last_Number(last_1, last_2, emails):
    # get db configs needed to track
    Loader_DB_Configs()
    (tracking_dbs, metadata_db) = (ConfigLoader.shareInstance().tracking_dbs, ConfigLoader.shareInstance().metadata_db)
    session = Get_One_Session(metadata_db)

    ver1 = TrackVersionModel.QueryWithLastOrder(session, last_1)
    ver2 = TrackVersionModel.QueryWithLastOrder(session, last_2)
    Diff_Versions_With_Versions(session, ver1, ver2, emails)

    session.close()
    pass

if __name__ == '__main__':
    parser = OptionParser()

    group0 = OptionGroup(parser, "Init Db")
    group0.add_option("-I", "--Init", action="store_true", help="Init meta tables")
    parser.add_option_group(group0)

    group1 = OptionGroup(parser, "Track Options")
    group1.add_option("-t", "--track", action="store_true", help="Track a version now!!!")
    group1.add_option("--note", type="string", help="Note for the tracking version.")
    parser.add_option_group(group1)

    group2 = OptionGroup(parser, "Diff Options")
    group2.add_option("-d", "--diff", type="string",
                      help="""Diff between two versions formated json array,
                      like: '["2017-01-01 01:00:00", "2017-02-01 01:00:00"]'.""")
    # group2.add_option("--only-table", type="string", help="Diff the specified table")
    group2.add_option("--with-last-number", action="store_true", dest="diff_with_last_number",
                      default=False, help="Diff with numbers that counted backward")
    group2.add_option("--email-to", type="string",
                      help="Email receivers, separated by ';', 'default' will use receivers in email.yaml.")
    parser.add_option_group(group2)

    (options, args) = parser.parse_args()
    if options.Init is not None:
        Init_Meta_Db()
    elif options.track is not None:
        Track_One_Version(options.note)
    elif options.diff is not None:
        try:
            versions = json.loads(options.diff)
            emails = []
            if options.email_to is not None and len(options.email_to) > 0:
                emails = options.email_to.split(";")
            if len(versions) <> 2:
                print "diff action need two versions."
            else:
                if options.diff_with_last_number:
                    Diff_Versions_With_Last_Number(versions[0], versions[1], emails)
                else:
                    Diff_Versions_With_Tags(versions[0], versions[1], emails)
        except BaseException, be:
            print "diff options error:", be
            print "enter -h for help"

