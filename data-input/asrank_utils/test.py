# -*- coding: utf-8 -*-
from pprint import pprint

__author__ = 'baitaluk'

import random

random.seed()

relationship = ['provider', 'peer', 'customer']
rank = [1, 10, 8, 13, 34, 5, 6, 9, 12, 7]

data = []
count = 0
while count < 10:
    el = {
        "relationship": random.choice(relationship),
        "rank": rank[count]
    }
    count += 1
    data.append(el)

pprint(data)
# sorting

data.sort(key=lambda e: (e['relationship'], -e['rank']))
print("SORTED")
pprint("-"*10)
pprint(data)

data.sort(key=lambda e: (e['relationship'], -e['rank']), reverse=True)
pprint("-"*10)
pprint(data)

