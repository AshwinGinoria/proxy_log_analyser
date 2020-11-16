import pandas as pd
import numpy as np
import dask.dataframe as dd
from collections import Counter
import matplotlib.pyplot as plt
from datetime import datetime

class helpers():
    # Returns log file data as a Dataframe
    def ReadLog(self, filepath):
        cols = [
            "Timestamp",
            "Elapsed Time",
            "Client",
            "Log_TagHTTP_Code",
            "Size",
            "Method",
            "URI",
            "UserID",
            "HierarchyHostname",
            "Content_type",
        ]

        # Reading File
        df = dd.read_csv(filepath, names=cols, delim_whitespace=True, header=None)
        print("DataFrame Loaded")

        # Separating Log_Tag and HTTP_Code
        Log_Tag = df.Log_TagHTTP_Code.apply(lambda x: str(x).split('/')[0], meta=object)
        HTTP_Code = df.Log_TagHTTP_Code.apply(lambda x: x.split('/')[1], meta=object)
        df = df.assign(Log_Tag=Log_Tag)
        df = df.assign(HTTP_Code=HTTP_Code)

        # Separating Hostname and Hierarchy
        Hierarchy = df.HierarchyHostname.apply(lambda x: x.split('/')[0], meta=object)
        Hostname = df.HierarchyHostname.apply(lambda x: x.split('/')[1], meta=object)
        df = df.assign(Hierarchy=Hierarchy)
        df = df.assign(Hostname=Hostname)
        print("Columns Splitted")

        # Extracting Domain from URI
        m = df["URI"].str.extract("(?<=http://)(.*?)(?=/)|(?<=https://)(.*?)(?=/)")
        m = m[0].fillna(m[1])
        df["Domain_Name"] = m
        df["Domain_Name"] = df["Domain_Name"].fillna(df["URI"].str.extract("()(.*?)(?=:)")[1])
        print("Domains Extraced")

        # Dropping Useless Data to reduce RAM usage
        df = df.drop([cols[3], cols[7], cols[8]], axis=1)
        print("Columns Dropped")

        # Dropping un-important websites
        domainsToDrop = ["gateway.iitmandi.ac.in", "ak.staticimgfarm.com", "live.login.com"]
        for domain in domainsToDrop:
            df = df[df["Domain_Name"] != domain]
        print("Filtered out Domains")

        self.df = df

    def CountWebsite(self):

        columnList = self.df["Domain_Name"].value_counts().compute()

        elementList = columnList.nlargest(n=20)
            
            
        mostVisited = columnList.max()
        

        leastVisited = columnList.min()
        
        websites = [key for key,val in elementList.items()]
        frequency = [val for key,val in elementList.items()]

        plt.bar(websites, frequency)
        plt.title("Most Frequently Visited Websites")
        plt.xlabel("Domain_Name")
        plt.ylabel("Frequency")
        plt.xticks(rotation=90)
        plt.subplots_adjust(bottom=0.3)
        plt.show()

        return (mostVisited, mostVisited)


    def PlotAcceptedDeniedCount(self):
        countAll = [0] * 24
        countDenied = [0] * 24
        time = self.df["Timestamp"].values
        logTag = self.df["Log Tag"].values
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
        
    def GetTopTenClients(self):
        print("In GetTopTenClients")
        clientsRequestCounts = self.df["Client"].value_counts().compute()

        data = {"Clients": "Number of Requests"}
        for i in range(10):
            client = clientsRequestCounts.keys()[i]

            data[client] = clientsRequestCounts[client]

        return data

    def GetNumberOfWebsitesVisited(self, time1, time2):
        #     sample formats
        #     time1 = "24/12/12 12:33:22"
        #     time2 = "25/12/20 12:12:12"
        d=set()
        start = datetime.strptime(time1, '%d/%m/%y %H:%M:%S')
        end = datetime. strptime(time2, '%d/%m/%y %H:%M:%S')
        times = self.df["Timestamp"].values
        names = self.df["URI"].values
        for i in range(len(time)):
            hr = datetime.fromtimestamp(times[i])
            if hr<=end and hr>=start :
                d.add(hr)
        print("number of websites visited between %s and %s : %s" %(start, end, len(d)) )
