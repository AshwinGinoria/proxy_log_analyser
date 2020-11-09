#import ray
import pandas as pd
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt
from datetime import datetime

# Returns log file data as a Dataframe
def ReadLog(filepath):
    cols = [
        "Timestamp",
        "Elapsed Time",
        "Client",
        "Log Tag & HTTP Code",
        "Size",
        "Method",
        "URI",
        "UserID",
        "Hierarchy & Hostname",
        "Content type",
    ]

    df = pd.read_csv(filepath, names=cols, delim_whitespace=True, header=None)

    df[["Log Tag", "HTTP Code"]] = df[cols[3]].str.split("/", expand=True)
    df[["Hierarchy", "Hostname"]] = df[cols[8]].str.split("/", expand=True)

    # Extracting Domain from URI
    m = df["URI"].str.extract("(?<=http://)(.*?)(?=/)|(?<=https://)(.*?)(?=/)")
    m = m[0].fillna(m[1])
    df["Domain Name"] = m
    df["Domain Name"].fillna(df["URI"].str.extract("()(.*?)(?=:)")[1], inplace=True)

    # Dropping Useless Data to reduce RAM usage
    df = df.drop([cols[3], cols[7], cols[8]], axis=1)

    # Dropping un-important websites
    df = df.drop(df[df["Domain Name"] == "gateway.iitmandi.ac.in"].index)
    df = df.drop(df[df["Domain Name"] == "ak.staticimgfarm.com"].index)

    return df


def CountWebsite(dataFrame):

    columnList = dataFrame["Domain Name"].values
    counterObj = Counter(columnList)

    elementList=[]
    for key, value in counterObj.items():
        elementList.append([value,key])

    elementList.sort(key=lambda x: int(x[0]))
    elementList.reverse()

    mostVisited = elementList[0][1]
    leastVisited = elementList[-1][1]

    websites=[row[1] for row in elementList[:20]]
    frequency=[row[0] for row in elementList[:20]]

    plt.bar(websites,frequency)
    plt.title("Most Frequently Visited Websites")
    plt.xlabel("Domain Name")
    plt.ylabel("Frequency")
    plt.xticks(rotation=90)
    plt.subplots_adjust(bottom=0.3)
    plt.show()

    return (mostVisited, mostVisited)

def PlotAcceptedDeniedCount(dataFrame):
    countAccepted = [0]*24
    countDenied = [0]*24
    time = dataFrame["Timestamp"].values
    logTag = dataFrame["Log Tag"].values
    for i in range(len(time)):
        hr = datetime.fromtimestamp(time[i]).hour
        if logTag[i]=="TCP_DENIED":
            countDenied[hr]+=1
        else:
            countAccepted[hr]+=1
    barWidth = 0.25
    r1 = np.arange(len(countAccepted))
    r2 = [x + barWidth for x in r1]
    plt.bar(r1, countAccepted, color='blue', width=barWidth, edgecolor='white', label='Acepted')
    plt.bar(r2, countDenied, color='red', width=barWidth, edgecolor='white', label='Denied')
    plt.xlabel('Time', fontweight='bold')
    plt.xticks([r + barWidth for r in range(len(countAccepted))], [str(x) for x in range(1,25)])
 
    plt.legend()
    plt.show()

def MinMaxTrafficTime(dataFrame):
    count_entry = [0] * 24
    time = list(dataFrame["Timestamp"])
    for i in time:
        temp = datetime.fromtimestamp(i).hour
        count_entry[temp] += 1
    max_traffic_hour = []
    min_traffic_hour = []
    max_traffic = max(count_entry)
    min_traffic = min(count_entry)
    x = []
    for i in range(24):
        x.append(i)
        if count_entry == max_traffic:
            max_traffic_hour.append(i)
        if count_entry == min_traffic:
            min_traffic_hour.append(i)

    plt.scatter(x, count_entry, color="red")
    plt.plot(x, count_entry, color="blue")
    plt.xlabel("Hours")
    plt.ylabel("Traffic")
    plt.show()

    return [max_traffic_hour, min_traffic_hour, max_traffic, min_traffic]

def GetTopTenClients(dataFrame):
    clientsRequestCounts = dataFrame['Client'].value_counts()

    data = {'Clients': 'Number of Requests'}
    for i in range(10):
        client = clientsRequestCounts.keys()[i]
        
        data[client] = clientsRequestCounts[client]

    return data

