import pandas
from collections import Counter
import matplotlib.pyplot as plt

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

    df = pandas.read_csv(filepath, names=cols, delim_whitespace=True, header=None)

    df[["Log Tag", "HTTP Code"]] = df[cols[3]].str.split("/", expand=True)
    df[["Hierarchy", "Hostname"]] = df[cols[8]].str.split("/", expand=True)

    df = df.drop([cols[3], cols[8]], axis=1)

    return df


# returns the number of occurrence of each unique element in a dataframe column
def FindCount(dataFrame, columnName):
    columnList = df[columnName].values
    counterObj = Counter(columnList)
    elementDictionary = {}
    for key, value in counterObj.items():
        elementDictionary[key] = value

    return elementDictionary


# plots the histogram given a dictionary of key-value pairs
def PlotHistogram(dictionaryObject, xLabel, yLabel):
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
