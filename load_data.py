import requests
import pymongo
import pandas as pd 

def load_machhines(db, session):
    db.drop_collection('machines')
    colection = db['machines']
    page_num=0
    response = session.get("https://api.eversys-telemetry.com/v3/machines?offset={}".format(page_num))
    collection_machines.insert_many(response.json)
    while(len(response.json > 0)):
        page_num += 1
        response = session.get("https://api.eversys-telemetry.com/v3/machines?offset={}".format(page_num))
        collection_machines.insert_many(response.json)


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
session.headers.update({'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjY2ZGI2Y2E2MWM2MTM0Y2FiNzQ2Mjk5ODAyNmY4OTMxNmQ4NDljZmIwYjU5YzdjOWYzYTkxNDUzZTgwYmI0ZTI1MGU1OGJkZGU0NWY2MjkyIn0.eyJhdWQiOiI5IiwianRpIjoiNjZkYjZjYTYxYzYxMzRjYWI3NDYyOTk4MDI2Zjg5MzE2ZDg0OWNmYjBiNTljN2M5ZjNhOTE0NTNlODBiYjRlMjUwZTU4YmRkZTQ1ZjYyOTIiLCJpYXQiOjE2MTYzMjA3MDQsIm5iZiI6MTYxNjMyMDcwNCwiZXhwIjoxNjE2OTI1NTA0LCJzdWIiOiIxODQiLCJzY29wZXMiOlsib3BlbmlkIl19.Zy6iBBFPm7gEPy5n4tk6-XHPIQyQwrqV7hp82fsN8cl_1RLQ3iJmoVHmq5q_fL2P8JeRb9BJAVsONb9WVZiVe2gyaamJzCwo0FrHdF18iCM9ZuZev6rWL_sUmOCC8Xa8dHcMq5LJ6SFZUs_jtQI_ezk8tZ4KXfGVzI74DAkUmK8YPpHO2sB-yTbPD0cq_RG3vSYDuAFGLHNtgkw3jmL3CuJLDX6weySHo9oZULr_H124IrW4JpPF8fjj04m2XjtEQ26RPAaHQ9FxwG-PT97rGse98nLNncLYTO2tJuCK90y8AvZNp179h_A1mliNbiX8W4q1fM1eYAHDmI4ewbTQMyLx-pyyMn6Xr36yeC-UqTrVQMvs5-X88cOW-dpIQVjFZYd7xm9Ejaxgl4MjOSsRadhrGoV7uGbY1VsswMiAhGwwHBKZK_l-ZxLEcYaXA3xd1amYceHrW6qFU6lzq3gq3mBNiY6xF1oGz3sKVrI-v9_BQB-CeD4kLcoV_Xxr-go6Aho1ii9zalb_6P8IDg5KIUVfD69QaiI_YCRUV8vus9oCiIgjOrRIGhZWYa9z7e3sToiaD9NLtpHivCluhgLGbNxoaxVSQiOfIl585hMHe-XSY0SjsXJOjmM1wz6j1zqr47EAFv3jjkOqCTswXQCEyslMwihZPUGmL7HP0ugzdKs'})


client = pymongo.MongoClient('localhost', 27017)  
db = client['eversys']
collection_machines = db['product-history-dodo']

for first, second in zip(df['date'], df['date'][1:]):
    response = session.get('https://api.eversys-telemetry.com/v3/machines/328/products?st1={}&st2={}'.format(first, second))
    result = response.json()
    collection_machines.insert_many(result)
    print(len(result))

client.close()