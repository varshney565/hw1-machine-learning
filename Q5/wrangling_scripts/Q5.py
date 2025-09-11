"""
Q5.py - utilities to supply data to the templates.

This file contains a pair of functions for retrieving and manipulating data
that will be supplied to the template for generating the table.
"""
import csv


def username():
    return '</>'


def data_wrangling(filter_class: str = ""):
    import csv, os
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.normpath(os.path.join(here, "..", "data", "q5.csv"))

    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                c = int(str(r["count"]).strip())
            except Exception:
                continue 
            rows.append([r["species"], r["class"], c])
    option_list = sorted({klass for _, klass, _ in rows})
    data = [rec for rec in rows if (not filter_class or rec[1] == filter_class)]
    data.sort(key=lambda rec: rec[2], reverse=True)
    top10 = data[:10]
    header = ["species", "class", "count"]
    table = [[sp, klass, str(cnt)] for sp, klass, cnt in top10]
    return header, table, option_list
