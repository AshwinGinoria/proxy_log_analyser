#import ray
import pandas as pd
import numpy as np
from collections import Counter
import matplotlib.pyplot as plt
from datetime import datetime

# Returns log file data as a Dataframe
#
def ReadLog(filepath):
    cols = [
        "Timestamp",
        "Elapsed Time",
        "Client",
        "Log Tag & HTTP Code",
        "Size",
        "Method",
        "URL",
        "UserID",
        "Hierarchy & Hostname",
        "Content type",
    ]

    df = pd.read_csv(filepath, names=cols, delim_whitespace=True, header=None)

    df[["Log Tag", "HTTP Code"]] = df[cols[3]].str.split("/", expand=True)
    df[["Hierarchy", "Hostname"]] = df[cols[8]].str.split("/", expand=True)

    df = df.drop([cols[3], cols[8]], axis=1)
    return df


# returns the number of occurrence of each unique element in a dataframe column
def FindCount(dataFrame, columnName):
    columnList = dataFrame[columnName].values
    counterObj = Counter(columnList)
    elementDictionary = {}
    for key, value in counterObj.items():
        elementDictionary[key] = value

    return elementDictionary


# plots the histogram given a dictionary of key-value pairs
def PlotHistogram(dataFrame, xLabel, yLabel):
    dictionaryObject = FindCount(dataFrame,xLabel)
    plt.bar(dictionaryObject.keys(), dictionaryObject.values())
    plt.xlabel(xLabel)
    plt.ylabel(yLabel)
    plt.show()


# function to find the most and least frequently visited ip
def FindMostLeastFrequent(elementDictionary):
    ipMax = max(elementDictionary, key=elementDictionary.get)
    ipMin = min(elementDictionary, key=elementDictionary.get)

    return (ipMax, ipMin)


# returns the number of occurrence of a particular element from a column
def CountRequest(dataFrame, columnName, requestType):
    tagDictionary = FindCount(dataFrame, columnName)
    return tagDictionary[requestType]

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

