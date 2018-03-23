---
layout: post
title: Basic ETL with Python
categories:
- blog
---

ETL, or Extract-Transform-Load, is the workhorse procedure of Data Engineering. If you want to make [government systems better, you need to get good at ETL](http://daguar.github.io/2014/03/17/etl-for-america/).

ETL is all about [building pipes](https://sunlightfoundation.com/2014/03/21/data-plumbers/). In this module, we'll go over how to built a simple pipeline using the Python programming langague.


# Extract 

```

import requests

requests.get('URL')

data = r.json()

```
# Transform 

```
import pandas as pd 
df = pd.from_dict(data)
```

# Load

```
import sqlite3

conn = sqlite3.conn("./database.db")
df.to_sql(df, conn)
```


