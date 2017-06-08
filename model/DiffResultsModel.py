#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

reload(sys)
sys.setdefaultencoding('utf8')


class DiffResultsModel(object):
    def __init__(self, l, r, x):
        self.exist_l = l
        self.exist_r = r
        self.diff_x = x

    def __repr__(self):
        return 'exist_l:{}, exist_r:{}, diff_x:{}'.format(self.exist_l, self.exist_r, self.diff_x)
