#Verkehrsanzeige-Influx
#
from influxdb import InfluxDBClient
import json


# example data
'''
	2020-01-13	22:56	49		-
	2020-01-13	23:05	17	17	+
	2020-01-13	23:06	39		-
	2020-01-13	23:07	33	33	+
'''


with open('settings.json', 'r') as f:
    settings = json.load(f)

INFLUX_USER = settings['influx_user']
INFLUX_PASSWORD = settings['influx_password']
INFLUX_HOST = settings['influx_host']
DATABSE_NAME = settings['database_name']

client = InfluxDBClient(host=INFLUX_HOST, port=8086, username=INFLUX_USER, password=INFLUX_PASSWORD) 

## client.get_list_database()
## client.create_database('verkehrsmessung') 

client.switch_database(DATABSE_NAME) 

var_limit=50
var_ort='Murstetten'
var_beschreibung='Juli-August24'
filename='Murstetten-2024-24-juli-bis-26-aug.txt'

def genertate_json_body_item(var_ort,var_beschreibung,var_limit,var_richtung,var_geschwindigkeitok,var_time,var_messung1,var_messung2,var_ueberschreitung,var_reduktion):
    json_body_item = {
            "measurement": "Messungen",
            "tags": {
                "Ortschaft": var_ort,
                "Beschreibung": var_beschreibung,
                "Limit": var_limit,
                "Richtung": var_richtung,
                "Geschwindigkeiteingehalten": var_geschwindigkeitok, 
                "Reduktion":var_reduktion
            },
            "time": var_time,
            "fields": {
                "Messung1": var_messung1,
                "Messung2": var_messung2,
                "Ueberschreitung": var_ueberschreitung     
            }
        }
    return json_body_item 



'''  
    {
        "measurement": "brushEvents",
        "tags": {
            "user": "Carol",
            "brushId": "6c89f539-71c6-490d-a28d-6c5d84c0ee2f"
        },
        "time": "2018-03-29T8:04:00Z",
        "fields": {
            "duration": 132
        }
    },
    }
    var_limit,var_ort,var_beschreibung,,var_richtung,var_geschwindigkeitok,var_time,var_messung1,var_messung2,var_ueberschreitung,var_reduktion  
'''
with open(f'messungen/{filename}', 'r') as f:
    content = f.readlines()



json_body=[]
for i in content:
    var_reduktion='unknown'
    var_messung2=0
    a=i.strip()
    b=a.split('\t')
    var_time=b[0]+'T'+b[1]+':00Z'
    var_messung1=int(b[2])
    if b[4]=='+':
        var_richtung='normal'
        var_messung2=int(b[3])
    else:
        var_richtung='gegenrichtung' 
    print('ok')
    if var_messung1 > var_limit:
        var_ueberschreitung=int(b[2])-int(var_limit)
        var_geschwindigkeitok='nok'
        if b[4]=='+':
            if var_messung1 > var_messung2:
                var_reduktion='ok'
            else:
                var_reduktion='nok'
    else: 
        var_geschwindigkeitok='ok'
        var_ueberschreitung=0     
#    print(i)
#    print(str(var_limit)+' - '+var_ort+' - '+var_beschreibung+' - '+var_richtung+' - '+var_geschwindigkeitok+' - '+var_time+' - '+str(var_messung1)+' - '+str(var_messung2)+' - '+str(var_ueberschreitung)+' - '+var_reduktion)
    result=genertate_json_body_item(var_ort,var_beschreibung,var_limit,var_richtung,var_geschwindigkeitok,var_time,var_messung1,var_messung2,var_ueberschreitung,var_reduktion)
#    print(result)
#    print('#------------#-----------#----------#------------#')
    json_body.append(result)

client.write_points(json_body)




