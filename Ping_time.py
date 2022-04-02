#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 17:11:43 2022

@author: Lucas Finco

GNU General Public License 3.0

Ping Time Monitor
Airflow process for monitoring ping times on an hourly basis, saving data, and plotting results.

"""

from datetime import timedelta, datetime
import subprocess
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
 
# import pyping
 
# Default args for DAG
default_args = {
    'description': 'Periodically check ping times',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 30),
    'catchup': False,
    'email': ['youremail@server.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1)
}
 
# Python function for the ping_ip_task.
def ping_site(data_interval_start=None, data_interval_end=None, **kwargs) -> None:
    """ Ping an internet server and save the response times (in msec) """
    
    print("Start: " + str(data_interval_start)+ " End: " + str(data_interval_end))
    address = ""  # Server I can ping regularly
    count = 8     # Number of times to ping server
    pause = 1     # Pause time between pings
    
    cmd = "ping -c {} -W {} {}".format(count, pause, address).split(' ')
    
    print ("Pinging {}".format(address))
    try:
        output = subprocess.check_output(cmd).decode().strip()
    except Exception as e:
        print("Exception pinging " + address)
        print(e)
        return False
    
    print("Extracting times")
    lines = output.split("\n")
    timing = lines[-1].split()[3].split('/')
    
    min_time = float(timing[0])
    avg_time = float(timing[1])
    max_time = float(timing[2])
    mdev_time = float(timing[3])
    
    date1 = datetime.now()
    
    print("\n *** Average ping time was [" + timing[1] + "] at " + date1.isoformat() + " ***")
    
    output_file = "ping.csv"
    
    print("Writing to file " + output_file)
    
    try:
        f = open(output_file, 'a')
        f.write("\n")
        f.write(date1.isoformat())
        f.write(",")
        f.write("{0:.3f}".format(avg_time))
        f.write(",")
        f.write("{0:.3f}".format(min_time))
        f.write(",")
        f.write("{0:.3f}".format(max_time))
        f.write(",")
        f.write("{0:.3f}".format(mdev_time))
        f.close()
    except Exception as e:
        print("Exception thrown writing results " + output_file)
        print(e)
        return False
    
    print("Data write complete")
    return

# Python function to write the plots
def write_plots(**kwargs) -> None:
    """ Read ping data, produce plots and save them as images """

    path = ""
    
    print("loading ping log file")
    try:
        png = pd.read_csv(path + "ping.csv", parse_dates=[0])
    except Exception as e:
        print("failed to load ping log file")
        print(e)
        return False
    
    # add colummns for datetime breakouts
    png['doy'] = png.datetime.dt.day_of_year
    png['hour'] = png.datetime.dt.hour
    png['year'] = png.datetime.dt.year

    # convert to integer for heatmap
    png['apint'] = png.avg_ping.apply(int)
    
    # only use trailing year data in heat map so it doesn't overlap
    today = datetime.now()
    timed = timedelta(days=350)
    
    print("producing time series plot")
    ax = png.plot(x='datetime', y=['avg_ping','min_ping','max_ping']).legend(loc='upper left', fontsize=10)
    ax.figure.savefig(path + 'avg_ping_plot.png', dpi=120)
    ax.figure.clear()
    
    print("producing histogram")
    ax3 = sns.histplot(png[['avg_ping', 'max_ping', 'min_ping']], bins=50).set_title("Ping time Histogram")
    ax3.figure.savefig(path + 'ping_histogram.png', dpi=120)
    ax3.figure.clear()
    
    print("producing heat map")
    pvt = png[png['datetime']>(today - timed)].pivot_table(values='apint', index='doy', columns='hour', aggfunc='mean')
    #png.style.background_gradient(cmap='coolwarm').set_properties(**{'font-size': '20px'})
    f2, ax2 = plt.subplots(figsize=(6, 10))
    ax2 = sns.heatmap(pvt, cmap='coolwarm', linewidths=0.30, annot=False).set_title("Average Ping Time")
    ax2.figure.savefig(path + 'ping_heatmap.png', dpi=120)

    print("Ping time plots complete")
    return

# Open DAG, give it an ID, schedule, default_args.
with DAG(dag_id='Ping_Time_Monitor', 
         schedule_interval =  '14 * * * *', # Once an hour on the 14th minute
         default_args=default_args,
         tags = ['network'],
         catchup=False
         ) as dag:
    # Define a Dummy task that does nothing.
    start_task = DummyOperator(task_id='Start', retries=1)
    
    # Define a task that writes the average ping time to an Address.
    ping_address_task = PythonOperator(task_id='ping_site', python_callable=ping_site, retries=1)
    
    # Define a task that writes the average ping time to an Address.
    plot_ping_times = PythonOperator(task_id='plot_ping_times', python_callable=write_plots, retries=1)
    
    end_task = DummyOperator(task_id='End', retries=1)
    
    # Declare order of task execution.
    start_task >> ping_address_task >> plot_ping_times >> end_task
