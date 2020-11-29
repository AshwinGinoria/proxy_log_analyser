import pandas as pd
import numpy as np
import dask.dataframe as dd
from collections import Counter
import matplotlib.pyplot as plt
from datetime import datetime
import logging
import dask


logging.basicConfig()
logger = logging.getLogger("Helpers")
logger.setLevel(logging.DEBUG)


class Helpers:
    # Returns log file data as a Dataframe
    def ReadLog(self, listFiles):
        logger.info("Reading Data")

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

        logger.debug("Reading " + listFiles[0])
        df = dd.read_csv(listFiles[0], names=cols, delim_whitespace=True, header=None)

        for filePath in listFiles[1:]:
            logger.debug("Reading " + filePath)
            # Reading File
            df = df.append(
                dd.read_csv(filePath, names=cols, delim_whitespace=True, header=None)
            )

        # Separating Log_Tag and HTTP_Code
        logger.debug("Splitting LogTag and HTTP Code")
        logTag = df.Log_TagHTTP_Code.apply(lambda x: str(x).split("/")[0], meta=object)
        httpCode = df.Log_TagHTTP_Code.apply(lambda x: x.split("/")[1], meta=object)
        df = df.assign(Log_Tag=logTag)
        df = df.assign(HTTP_Code=httpCode)

        # Separating Hostname and Hierarchy
        logger.debug("Splitting Hierarchy and hostname")
        hierarchy = df.HierarchyHostname.apply(lambda x: x.split("/")[0], meta=object)
        hostname = df.HierarchyHostname.apply(lambda x: x.split("/")[1], meta=object)
        df = df.assign(Hierarchy=hierarchy)
        df = df.assign(Hostname=hostname)

        # Extracting Domain from URI
        logger.debug("Extracting Domain Names")
        m = df["URI"].str.extract("(?<=http://)(.*?)(?=/)|(?<=https://)(.*?)(?=/)")
        m = m[0].fillna(m[1])
        df["Domain_Name"] = m
        df["Domain_Name"] = df["Domain_Name"].fillna(
            df["URI"].str.extract("()(.*?)(?=:)")[1]
        )

        # Dropping Useless Data to reduce RAM usage
        logger.debug("Dropping Useless Columns")
        df = df.drop([cols[3], cols[7], cols[8]], axis=1)

        df["Timestamp"] = dd.to_datetime(df["Timestamp"], unit="s")

        # Dropping un-important websites
        logger.debug("Filtering out un-important Domains")
        domainsToDrop = [
            "gateway.iitmandi.ac.in",
            "ak.staticimgfarm.com",
            "login.live.com",
        ]
        for domain in domainsToDrop:
            df = df[df["Domain_Name"] != domain]

        logger.info("Data Read Successfully")
        self.df = df

    def CountWebsite(self, ax):
        logger.info("Counting Most Visited Domains")

        columnList = self.df["Domain_Name"].value_counts().compute()

        elementList = columnList.nlargest(n=20)
        mostVisited = columnList.max()
        leastVisited = columnList.min()

        websites = [key for key, val in elementList.items()]
        frequency = [val for key, val in elementList.items()]

        ax.bar(websites, frequency)
        ax.set_title("Most Frequently Visited Websites")
        ax.set_xlabel("Domain_Name")
        ax.set_ylabel("Frequency")
        ax.tick_params(labelrotation=60)

        return ax

    def PlotAcceptedDeniedCount(self, ax):

        logger.info("Counting Total Requests")
        countAll = [0] * 24
        countDenied = [0] * 24

        time = self.df["Timestamp"]
        logTag = self.df["Log_Tag"]
        allSeries = self.df["Timestamp"].dt.hour.value_counts().compute()
        deniedSeries = self.df[self.df["Log_Tag"]=="TCP_DENIED"]["Timestamp"].dt.hour.value_counts().compute()
        logger.debug("Counting Hourly Denied Requests ")
        for i in range(24):
            try:countDenied[i] = deniedSeries[i+1]
            except:continue
            try:countAll[i] = allSeries[i+1]
            except:continue

        barWidth = 0.25

        ax.set_ylabel("Number of Requests", fontweight="bold")
        ax.bar(
            np.arange(24),
            countAll,
            color="blue",
            width=barWidth,
            edgecolor="white",
            label="All",
        )
        ax.bar(
            np.arange(24) + barWidth,
            countDenied,
            color="red",
            width=barWidth,
            edgecolor="white",
            label="Denied",
        )
        ax.set_xlabel("Time(Hours of day)", fontweight="bold")
        ax.set_xticks([r + barWidth for r in range(len(countAll))])
        ax.set_xticklabels([str(x) for x in range(1, 25)])

        return ax

    def GetTopClients(self):
        logger.info("Calculating Top Clients")

        clientsRequestCounts = self.df["Client"].value_counts()

        topClients = clientsRequestCounts.nlargest(50).compute()
        data = {}
        for client, hits in topClients.items():
            data[client] = hits

        return data

    def PeakHourForEachWebsites(self, ax):
        logger.info("Calculating Peak time for each Domain")

        Websites = self.df["Domain_Name"]
        times = self.df["Timestamp"]
        WebsitesList = {}
        MostActiveHour = {}
        
        for i in Websites:
            WebsitesList[i] = [0] * 24
            MostActiveHour[i] = [0,0]

        for i, j in zip(Websites, times):
            temp = j.hour
            WebsitesList[i][temp] += 1
            if MostActiveHour[i][1]<WebsitesList[i][temp]:
                MostActiveHour[i] = [temp,WebsitesList[i][temp]]

        


        #Hours = []
        #for i in WebsitesList:
        #    Hours.append(sum(WebsitesList[i]))

        #Hours.sort(reverse=True)
        #Hours = Hours[:20]

        #TopTwenty = {}

        #Count = 0
        #for i in WebsitesList:
        #    if sum(WebsitesList[i]) in Hours and Count < 20:
        #        TopTwenty[i] = MostActiveHour[i]
        #        Count += 1
                #plt.plot([x for x in range(0,24)],WebsitesList[i],label = i)
        #plt.bar(TopTwenty.keys(),TopTwenty.values())
        #plt.title("Peak Hours For Top 20 Visited websites : ")
        #plt.xlabel("Domain_Name")
        #plt.ylabel("Hour")
        #plt.xticks(rotation=90)
        #plt.subplots_adjust(bottom=0.3)
        #plt.show()

        return MostActiveHour

    def GetNumberOfUniqueWebsites(self, time1, time2):
        logger.info("Calculating Number of Unique Websites in the given time-frame.")
        #     sample formats
        #     time1 = "24/12/12 12:33:22"
        #     time2 = "25/12/20 12:12:12"
        #     dd/mm/yy hh:mm:ss

        start = datetime.strptime(time1, "%d/%m/%y %H:%M:%S")
        end = datetime.strptime(time2, "%d/%m/%y %H:%M:%S")
        tmp = self.df.loc[
            (self.df["Timestamp"] <= end) & (self.df["Timestamp"] >= start)
        ]
        #       alternate(slower) implementation
        #         times = self.df["Timestamp"].values
        #         names = self.df["Domain_Name"].values
        #         d=set()
        #       for i in range(len(times)):
        #             hr = datetime.fromtimestamp(times[i])
        #             if(i==0 or i==len(times)-1):
        #                 print(hr)
        #             if hr<=end and hr>=start :
        #                 d.add(names[i])
        #         print(tmp.tail())
        denied_requests = len(tmp.loc[tmp["Log_Tag"] == "TCP_DENIED"])
        different_clients = len(tmp.drop_duplicates(subset=["Client"]))
        different_websites = len(tmp.drop_duplicates(subset=["Domain_Name"]))
        mylist = [denied_requests, different_clients, different_websites]
        mylist = dask.compute(*mylist)
        print(
            "between %s and %s :\nnumber of different clients: %s , number of different websites: %s, number of denied requests: %s"
            % (time1, time2, mylist[1], mylist[2], mylist[0])
        )
        d={}
        d["start time"] = time1
        d["end time"] = time2
        d["different clients"] = mylist[1] 
        d["different websites"] = mylist[2]
        d["number of denied requests"] = mylist[0]
        return d
