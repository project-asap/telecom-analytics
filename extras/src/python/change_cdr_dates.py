#!/usr/bin/python

import random
import sys

from datetime import datetime, timedelta

if __name__ == '__main__':
    delim = " ; "
    date_format = '%Y-%m-%d'
    from_ = sys.argv[1]
    to_ = sys.argv[2]
    with open(from_, "r") as fr:
        with open(to_, "w") as fw:
            for l in fr:
                cols = l.split(delim)
                start_date = datetime.strptime(cols[3], date_format)
                plus = random.randint(0, 31)
                new_date = start_date + timedelta(plus)
                cols[3] = new_date.strftime(date_format)
                ll = delim.join(cols)
                fw.write(ll)
