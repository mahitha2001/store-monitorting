import concurrent.futures
from flask import Flask, jsonify, Response
import random
import mysql.connector
import datetime
import io
import csv

app = Flask(__name__)

mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3306",
    user="root",
    passwd="password@123"
)
cursor = mydb.cursor()
cursor.execute("USE stores")

queryForDistinctStoreID ="""
    SELECT distinct(store_id) from store_status
"""
cursor.execute(queryForDistinctStoreID )
listOfStores=cursor.fetchall()

def calculate_uptime_last_hour(store_id, current_time, timezone):
    print("uptime")
    last_hour = current_time - datetime.timedelta(hours=1)

    before_last_hour = last_hour - datetime.timedelta(hours=1)
    
    query="""
        SELECT timestamp_utc, status from store_status where store_id=%s and timestamp_utc > %s and timestamp_utc<=%s order by timestamp_utc DESC;
    """
    cursor.execute(query, (store_id, last_hour, current_time))
    rows_c_l = cursor.fetchall()


    query="""
        SELECT timestamp_utc, status from store_status where store_id=%s and timestamp_utc>%s and timestamp_utc<=%s order by timestamp_utc DESC; 
    """
    cursor.execute(query, (store_id, before_last_hour, last_hour))
    rows_l_b = cursor.fetchall()
    
    if(rows_c_l==[] and rows_l_b==[]):
        return 0
    
    max_bound=None
    min_bound=None

    if(rows_c_l!=[] and rows_c_l[0][1]=='active'):
        max_bound=current_time
    elif(len(rows_c_l)>1 and rows_c_l[0][1]=='inactive'):
        max_bound=rows_c_l[0][0]
    else:
        max_bound=None

    if(rows_l_b==[] and rows_c_l[len(rows_c_l)-1][1]!='inactive'):
        min_bound=rows_c_l[len(rows_c_l)-1][0]
    elif(rows_l_b!=[] and rows_l_b[0][1]=='active'):
        min_bound=last_hour
    else:
        min_bound=None

    if(min_bound==None or max_bound==None):
        return 0

    query = """
        SELECT convert_tz(%s, 'UTC', %s), convert_tz(%s, 'UTC', %s)
    """
    cursor.execute(query, (min_bound, timezone, max_bound, timezone))
    (min_bound_l, max_bound_l)=cursor.fetchall()[0]

    max_bound_ltd=datetime.datetime.combine(datetime.date.min, max_bound_l.time()) - datetime.datetime.min
    min_bound_ltd=datetime.datetime.combine(datetime.date.min, min_bound_l.time()) - datetime.datetime.min


    query = """
        SELECT start_time_local, end_time_local from menu_hours where store_id=%s and dayOfWeek=%s
    """

    cursor.execute(query, (store_id, current_time.weekday()))
    (open_hours,close_hours)=cursor.fetchall()[0]

    if min_bound_ltd<=close_hours and min_bound_ltd>=open_hours and max_bound_ltd<=close_hours and max_bound_ltd>=open_hours:
        min_=min_bound_ltd
        max_=max_bound_ltd
    elif min_bound_ltd<=close_hours and min_bound_ltd>=open_hours and max_bound_ltd>close_hours and max_bound_ltd>=open_hours:
        min_=min_bound_ltd
        max_=close_hours
    elif min_bound_ltd<=close_hours and min_bound_ltd<open_hours and max_bound_ltd<=close_hours and max_bound_ltd>=open_hours:
        min_=open_hours
        max_=max_bound_ltd
    elif min_bound_ltd<=close_hours and min_bound_ltd>open_hours and max_bound_ltd<close_hours and max_bound_ltd>=open_hours:
        min_=open_hours
        max_=close_hours
    else:
        min_=0
        max_=0
    return max_ - min_

#assuming that inactive time comes only once per day

def calculate_downtime_last_hour(store_id, current_time, timezone):
    last_hour = current_time - datetime.timedelta(hours=1)

    before_last_hour = last_hour - datetime.timedelta(hours=1)
    
    query ="""
        select timestamp_utc from store_status where store_id=%s and status='inactive' and timestamp_utc<=%s and timestamp_utc>%s;
    """
    cursor.execute(query, (store_id, current_time, before_last_hour))
    inactive_timestamp=cursor.fetchall()

    if inactive_timestamp==[]:
        return 0

    inactive_timestamp=inactive_timestamp[0][0]
    query = """
        SELECT start_time_local, end_time_local from menu_hours where store_id=%s and dayOfWeek=%s
    """
    cursor.execute(query, (store_id,current_time.weekday()))
    (open_hours,close_hours)=cursor.fetchall()[0]
    
    query = """
        SELECT convert_tz(%s, 'UTC', %s), convert_tz(%s, 'UTC', %s), convert_tz(%s, 'UTC', %s)
    """
    cursor.execute(query, (current_time, timezone, inactive_timestamp, timezone, last_hour, timezone))
    (current_time_l, inactive_time_l, last_hour_l)=cursor.fetchall()[0]
    
    curr_time_ltd=datetime.datetime.combine(datetime.date.min, current_time_l.time()) - datetime.datetime.min
    inactive_time_ltd=datetime.datetime.combine(datetime.date.min, inactive_time_l.time()) - datetime.datetime.min
    last_hour_ltd=datetime.datetime.combine(datetime.date.min, last_hour_l.time()) - datetime.datetime.min
    
    print("curr_time_ltd: ",curr_time_ltd)
    print("inactive_time_ltd: ",inactive_time_ltd)
    print("last_hour_ltd: ",last_hour_ltd)
    max_bound=curr_time_ltd
    min_bound=None
    if inactive_time_ltd<last_hour_ltd:
        min_bound=last_hour_ltd
    else:
        min_bound=inactive_time_ltd
    print("min_bound: ", min_bound)
    print("max_bound: ", max_bound)
    
    if min_bound<=close_hours and min_bound>=open_hours and max_bound<=close_hours and max_bound>=open_hours:
        print("case 0")
        min_=min_bound
        max_=max_bound
    elif min_bound<=close_hours and min_bound>=open_hours and max_bound>close_hours and max_bound>=open_hours:
        print("case1")
        min_=min_bound
        max_=close_hours
    elif min_bound<=close_hours and min_bound<open_hours and max_bound<=close_hours and max_bound>=open_hours:
        print("case2")        
        min_=open_hours
        max_=max_bound
    elif min_bound<=close_hours and min_bound<open_hours and max_bound>close_hours and max_bound>=open_hours:
        print("case3") 
        min_=open_hours
        max_=close_hours
    else:
        print("case4") 
        min_=0
        max_=0
    #downtime_last_hour=max_bound-min_bound
    print(max_,min_)
    return max_-min_
#print(downtime_last_hour)


def calculate_for_last_hour(current_time, store_id, timezone):
  uptime=calculate_uptime_last_hour(store_id, current_time, timezone)

  downtime=calculate_downtime_last_hour(store_id, current_time, timezone)
  
  if uptime==0 or uptime>datetime.timedelta(days=1):
    uptime_last_hour=0
  else:
    uptime_last_hour=float(uptime.total_seconds())/60
  
  if downtime==0 or downtime>datetime.timedelta(days=1):
    downtime_last_hour=0
  else:
    downtime_last_hour=float(downtime.total_seconds())/60
  return {"uptime_last_hour": uptime_last_hour, "downtime_last_hour": downtime_last_hour}

def calculate_for_last_day(current_time, store_id, timezone):
  uptime=float(0)
  downtime=float(0)

  last_day=current_time - datetime.timedelta(days=1)
  temp_time=current_time

  while(temp_time>last_day):
    temp=calculate_for_last_hour(temp_time, store_id, timezone)
    uptime=uptime+temp.get('uptime_last_hour')
    downtime=downtime+temp.get('downtime_last_hour')
    temp_time=temp_time - datetime.timedelta(hours=1)
  uptime_last_day=float(uptime)/60
  downtime_last_day=float(downtime)/60
  return {"uptime_last_day":uptime_last_day, "downtime_last_day": downtime_last_day}

def calculate_for_last_week(current_time, store_id, timezone):
  uptime=float(0)
  downtime=float(0)
  last_week=current_time - datetime.timedelta(days=7)
  temp_time=current_time
  while temp_time>last_week:
    print(temp_time)
    temp=calculate_for_last_hour(temp_time, store_id, timezone)
    uptime=uptime+temp.get('uptime_last_hour')
    downtime=downtime+temp.get('downtime_last_hour')
    temp_time=temp_time - datetime.timedelta(hours=1)
  uptime_last_week = float(uptime)/60
  downtime_last_week = float(downtime)/60
  return {"uptime_last_week":uptime_last_week, "downtime_last_week": downtime_last_week}

def asyncGenerateReport(store_id):
    print("storeid:", store_id)
    query="""
      SELECT max(timestamp_utc) from store_status;
    """
    cursor.execute(query)
    current_time=cursor.fetchall()[0][0]
    print(current_time)

    query = """
      SELECT timezone_str from timezones where store_id=%s;
    """

    cursor.execute(query, (store_id,))
    timezone = cursor.fetchall()[0][0]
    print("hey timezone: ", timezone)
    last_hour=calculate_for_last_hour(current_time, store_id, timezone)
    print("last_hour: ", last_hour)
    last_day=calculate_for_last_day(current_time,store_id, timezone)
    print("last_day: ", last_day)
    last_week=calculate_for_last_week(current_time, store_id, timezone)
    #last_week={"uptime_last_week":0, "downtime_last_week": 0}
    print("last_week: ", last_week)
    return [last_hour, last_day , last_week ]
results = {}  # Store results in a dictionary
random_store_id = None  # To store the current store index



@app.route('/trigger_report', methods=['GET'])
def triggerReport():
  global random_store_id
  random_store_id = random.randint(0, len(listOfStores) - 1)
  #random_store_id = 2970450527428041505
  with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(asyncGenerateReport, listOfStores[random_store_id][0])
    results['report'] = future
  
  return jsonify({"Triggering report for the random store ID ": listOfStores[random_store_id][0]})

@app.route('/generate_report', methods=['GET'])
def generateReport():
  global random_store_id
  
  if 'report' in results and results['report'].done():
    result = results.pop('report').result()
    message="Report generated for the storeID="+str(listOfStores[random_store_id][0])
    csv_data=[{
        "uptime last hour(in minutes)": result[0].get('uptime_last_hour'),
        "downtime last hour(in minutes)": result[0].get('downtime_last_hour'),
        "uptime last day(in hours)": result[1].get('uptime_last_day'),
        "downtime last day(in hours)": result[1].get('downtime_last_day'),
        "uptime last week(in hours)": result[2].get('uptime_last_week'),
        "downtime last week(in hours)": result[2].get('downtime_last_week')
      }]
    csv_output = io.StringIO()
    csv_writer = csv.DictWriter(csv_output, fieldnames=['uptime last hour(in minutes)', 
                                                        'downtime last hour(in minutes)',
                                                        'uptime last day(in hours)',
                                                        'downtime last day(in hours)',
                                                        'uptime last week(in hours)',
                                                        'downtime last week(in hours)'])
    csv_writer.writeheader()
    csv_writer.writerows(csv_data)

    result_final=Response("Complete!!!", content_type='text/csv')
    result_final.headers['Content-Disposition'] = 'attachment; filename=store_data.csv'
    result_final.data = csv_output.getvalue()
    return result_final
  
  
  return jsonify({"message": "Running..."})

if __name__ == '__main__':
    app.run(debug=True)
