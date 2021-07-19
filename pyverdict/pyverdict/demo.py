import decimal

import pyverdict

verdict_conn = pyverdict.mysql(
    host='localhost',
    user='root',
    password='yizhouyan',
    port=3306,
    enable_dp="true",
    delta=str(0.000001),
    epsilon=str(0.01)
)
# verdict_conn.sql('CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales')

df = verdict_conn.sql(
    """
        select data
        from `verdictdbmeta`.`verdictdbmeta`;
    """
)
from pandas.core.common import flatten

print(df)
import pandas as pd

# print(pd.to_numeric([x for x in df.values.flatten() if isinstance(x, decimal.Decimal)]))

verdict_conn.close()

print(df)
