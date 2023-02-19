import time
from datetime import datetime
from math import ceil

timezone = -1
date_ts = 1676494800
days_offset = ceil((date_ts - time.time() - timezone * 3600) / (24 * 3600)) - 1

print(days_offset)

a = int('f')
print(a)

