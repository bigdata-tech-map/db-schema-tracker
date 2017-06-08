#!/usr/bin/env python
# -*- coding: utf-8 -*-


from model.DiffResultsModel import DiffResultsModel
import sys

reload(sys)
sys.setdefaultencoding('utf8')


class DiffUtil(object):
    @staticmethod
    def diff(results_l, results_r, diff_columns, primary_keys):
        for e in results_l:
            keys = map(lambda k: getattr(e, k, ""), primary_keys)
            setattr(e, 'group_key', "__".join(keys))

        for e in results_r:
            keys = map(lambda k: getattr(e, k, ""), primary_keys)
            setattr(e, 'group_key', "__".join(keys))

        grouped_l = {}
        for e in results_l:
            if not hasattr(grouped_l, e.group_key):
                grouped_l[e.group_key] = []
            grouped_l[e.group_key].append(e)

        grouped_r = {}
        for e in results_r:
            if not hasattr(grouped_r, e.group_key):
                grouped_r[e.group_key] = []
            grouped_r[e.group_key].append(e)

        # in left, not in right
        exist_l = []
        for k in grouped_l:
            if not (k in grouped_r):
                exist_l.append(grouped_l[k][0])

        # in right, not in left
        exist_r = []
        for k in grouped_r:
            if not (k in grouped_l):
                exist_r.append(grouped_r[k][0])

        # diff between left and right
        diff_x = []
        for k in grouped_l:
            have_diff = False
            one_diff = {
                "diff_fields": [],
                "diff_rows": []
            }
            if k in grouped_r:
                obj_l = grouped_l[k][0]
                obj_r = grouped_r[k][0]

                for f in diff_columns:
                    if getattr(obj_l, f) != getattr(obj_r, f):
                        one_diff["diff_fields"].append(f)
                        have_diff = True

                if have_diff:
                    one_diff["diff_rows"].append(obj_l)
                    one_diff["diff_rows"].append(obj_r)
            if have_diff:
                diff_x.append(one_diff)

        return DiffResultsModel(exist_l, exist_r, diff_x)

