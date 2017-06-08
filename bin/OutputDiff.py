#!/usr/bin/env python
# -*- coding: utf-8 -*-

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from utils.logger import logger
from bin.ConfigLoader import ConfigLoader
from model.TableLogModel import TableLogModel
from model.ColumnLogModel import ColumnLogModel
import sys

reload(sys)
sys.setdefaultencoding('utf8')

PROJECT_PATH = sys.path[0]

DIFF_TEMPLATE = PROJECT_PATH + '/diff_results/results_template.html'
DIFF_RESULT = PROJECT_PATH + '/diff_results/[{id_l}-{id_r}]_[({tag_l})-({tag_r})].html'


class OutputDiff(object):
    @staticmethod
    def Output(ver_l, ver_r, diff_table_results, diff_column_results):
        f = open(DIFF_TEMPLATE, 'r')
        template = f.read()
        f.close()

        print diff_table_results
        print diff_column_results
        table_in_A_not_in_B = TableLogModel.GetHtmlTrs(diff_table_results.exist_l)
        table_in_B_not_in_A = TableLogModel.GetHtmlTrs(diff_table_results.exist_r)
        table_differences = TableLogModel.GetHtmlDiffTrs(diff_table_results.diff_x)

        column_in_A_not_in_B = ColumnLogModel.GetHtmlTrs(diff_column_results.exist_l)
        column_in_B_not_in_A = ColumnLogModel.GetHtmlTrs(diff_column_results.exist_r)
        column_differences = ColumnLogModel.GetHtmlDiffTrs(diff_column_results.diff_x)

        title = """Track Diff Results Between: [%s]("%s") And [%s]("%s")""" % (ver_l.id, ver_l.tag, ver_r.id, ver_r.tag)
        version_l = "[id=%s, tag='%s']" % (ver_l.id, ver_l.tag)
        version_r = "[id=%s, tag='%s']" % (ver_r.id, ver_r.tag)
        content = template.format(
            title=title,
            version_l=version_l,
            version_r=version_r,
            table_in_A_not_in_B=table_in_A_not_in_B,
            table_in_B_not_in_A=table_in_B_not_in_A,
            table_differences=table_differences,
            column_in_A_not_in_B=column_in_A_not_in_B,
            column_in_B_not_in_A=column_in_B_not_in_A,
            column_differences=column_differences
        )

        out_path = DIFF_RESULT.format(id_l=ver_l.id, id_r=ver_r.id, tag_l=ver_l.tag, tag_r=ver_r.tag)
        w = open(out_path, 'w')
        w.write(content)
        w.close()
        return out_path

    @staticmethod
    def SendEmail(subject, path, emails):
        email_conf = ConfigLoader.shareInstance().email_conf
        receivers = []

        if len(emails) > 0 and emails[0].lower() == 'default':
            receivers = email_conf['receivers']
        else:
            receivers = emails

        if len(receivers) == 0:
            return

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = email_conf['smtp_server']['user']
        msg['To'] = ",".join(receivers)

        msg.add_header('Content-Disposition', 'attachment', filename="")

        fp = open(path)
        content = MIMEText(fp.read(), _subtype='html', _charset='utf-8')
        fp.close()
        msg.attach(content)

        try:
            server = smtplib.SMTP()
            server.connect(email_conf['smtp_server']['host'])
            server.login(email_conf['smtp_server']['user'], email_conf['smtp_server']['pass'])
            logger.info('Receivers：{}'.format(receivers))
            result = server.sendmail(email_conf['smtp_server']['user'], receivers, msg.as_string())
            if len(result) == 0:
                logger.info('Email send Succeed!😃')
            else:
                logger.error('Email send error: {}'.format(result))
            server.close()
            return True
        except Exception, e:
            print str(e)
        pass
