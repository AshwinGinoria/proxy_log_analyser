# import ray
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

    elementList = []
    for key, value in counterObj.items():
        elementList.append([value, key])

    elementList.sort(key=lambda x: int(x[0]))
    elementList.reverse()

    mostVisited = elementList[0][1]
    leastVisited = elementList[-1][1]

    websites = [row[1] for row in elementList[:20]]
    frequency = [row[0] for row in elementList[:20]]

    plt.bar(websites, frequency)
    plt.title("Most Frequently Visited Websites")
    plt.xlabel("Domain Name")
    plt.ylabel("Frequency")
    plt.xticks(rotation=90)
    plt.subplots_adjust(bottom=0.3)
    plt.show()

    return (mostVisited, mostVisited)


def PlotAcceptedDeniedCount(dataFrame):
    countAll = [0] * 24
    countDenied = [0] * 24
    time = dataFrame["Timestamp"].values
    logTag = dataFrame["Log Tag"].values
    for i in range(len(time)):
        hr = datetime.fromtimestamp(time[i]).hour
        if logTag[i] == "TCP_DENIED":
            countDenied[hr] += 1
        
        countAll[hr] += 1
    barWidth = 0.25
    r1 = np.arange(len(countAll))
    r2 = [x + barWidth for x in r1]
    plt.ylabel("Number of Requests",fontweight = "bold")
    plt.bar(
        r1,
        countAll,
        color="blue",
        width=barWidth,
        edgecolor="white",
        label="All",
    )
    plt.bar(
        r2, countDenied, color="red", width=barWidth, edgecolor="white", label="Denied"
    )
    plt.xlabel("Time(Hours of day)", fontweight="bold")
    plt.xticks(
        [r + barWidth for r in range(len(countAll))],
        [str(x) for x in range(1, 25)],
    )

    plt.legend()
    plt.show()
    
def GetTopTenClients(dataFrame):
    clientsRequestCounts = dataFrame["Client"].value_counts()

    data = {"Clients": "Number of Requests"}
    for i in range(10):
        client = clientsRequestCounts.keys()[i]

        data[client] = clientsRequestCounts[client]

    return data

def GetNumberOfWebsitesVisited(time1, time2):
    #     sample formats
    #     time1 = "24/12/12 12:33:22"
    #     time2 = "25/12/20 12:12:12"
    d=set()
    start = datetime. strptime(time1, '%d/%m/%y %H:%M:%S')
    end = datetime. strptime(time2, '%d/%m/%y %H:%M:%S')
    times = data["Timestamp"].values
    names = data["URI"].values
    for i in range(len(time)):
        hr = datetime.fromtimestamp(times[i])
        if hr<=end and hr>=start :
            d.add(hr)
    print("number of websites visited between %s and %s : %s" %(start, end, len(d)) )
