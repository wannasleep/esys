import requests
import pymongo
import pandas as pd 

machine_ids = ['14658', '14651', '11027', '9048', '8863']

df = pd.DataFrame()
dates = pd.date_range('02.01.2021', '03.01.2021', freq='2880min')
df['dates'] = dates.strftime('%Y-%m-%d').to_list()
df['times'] = dates
df['hour']= df['times'].dt.hour.apply(str)
df['minute']= df['times'].dt.minute.apply(str)
df['second'] = df['times'].dt.second.apply(str)
df['date'] = df['dates'] + 'T' + df['times'].dt.strftime('%H') + '%3A' + df['times'].dt.strftime('%M') + '%3A' +df['times'].dt.strftime('%S') + 'Z'
print(df['date'])

session = requests.Session()
session.headers.update({"Accept": "application/json"})
session.headers.update({'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjYzZjEwOWRlNDlmZWM2YTdjMWJjODU3MzUwODQwMzdlMjhlM2Q3M2ZlNTkyYjIxYzFlZDNmMTc4MTk0YTQ3NDdhMGYzMzgzZTJhNTIwNDBmIn0.eyJhdWQiOiI5IiwianRpIjoiNjNmMTA5ZGU0OWZlYzZhN2MxYmM4NTczNTA4NDAzN2UyOGUzZDczZmU1OTJiMjFjMWVkM2YxNzgxOTRhNDc0N2EwZjMzODNlMmE1MjA0MGYiLCJpYXQiOjE2MTU4MDUwMTYsIm5iZiI6MTYxNTgwNTAxNiwiZXhwIjoxNjE2NDA5ODE2LCJzdWIiOiIxODQiLCJzY29wZXMiOlsib3BlbmlkIl19.hL07SOYjw8EH9ZlDWSTML3zKvOJH9MfgzSm9nptTiqXHdCxmZrBpsDEh2xW-ZF_DKVrxT5JomJnwnWrSp339rYus_MtAwrzU78fafBRNWxmk0ZBVLp51jruRc2dT5b7TuvwC4IyB14H4XFDnWnMn9g_UwTDKUvvLJQLlhW9qz8V2QRy90E-_SBxxwscFdB8evS0lb7JM5TXOdM6K1Zn0BC3A3rkY9d6v-SsycWvt1W59NZlrB-jx3HN1lwJQCTtD1fSsWWpfmX9EJWJgaALStM_a5AAAcsEqTWId-VlPqZL61MIK9cimOfNlU1oAiPCzIIxrktUsCP0HhsuEsF78Tb85PrAmH60lTaQqveSZtfBQRPdToMpzdTpWmBmrO3NebjjYQGG2xTmDQeMtc1geyMwWd7h_J9WZkqW4Mo4V5UoEr2KRPPCQzVWsut-sIXnb1vlm5vt2h9pRnJS2ooGpLydDysnMgJuYtkCDBgy05vDXPLiaW5R88JyaNdfg2r8n2AGOf5847G-PkjZVRCbG_jwcUa2nOX6yA16gaCndurju92Y1A0kntJo3SBAQIwWjrMYei43Xn9ZWGpZXYUVBvnA1uSPrZBpdHZMs_TrDa1NML3IdCS3XshnVXOfBOvhjAZNETV91UpzlkgXRmAb7bvRm62HLujpWdJgdKooW9Vw'})


client = pymongo.MongoClient('localhost', 27017)  
db = client['eversys']
collection_machines = db['product-history-dodo']

for first, second in zip(df['date'], df['date'][1:]):
    response = session.get('https://api.eversys-telemetry.com/v3/machines/328/products?st1={}&st2={}'.format(first, second))
    result = response.json()
    collection_machines.insert_many(result)
    print(len(result))

client.close()