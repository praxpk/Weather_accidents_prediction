import pandas as pd
import datetime
import time
import threading
from selenium import webdriver
import queue
import traceback

class w_sel:
    def __init__(self,muni,num):
        """
        The queue stores tuples where each tuple contains start and end indices of the rows of the dataframe.
        :param muni: the list of municipalities whose data we need to extract
        """
        self.q = queue.Queue()
        self.muni = muni
        self.num=num



    def run(self):
        """
        Each thread returns its start and end index and that is used to remove the start and end segment tuple on the queue.
        This action denotes the successful extraction of data for corresponding rows in dataframe.
        :return: None
        """
        for i in self.muni:
            df = self.filter_records(i)
            d={}
            for j in range(0,df.shape[0],50):
                d[(j,j+50)] = 1
            while(True):
                if len(d)==0:
                    break
                for j in d.copy():
                    t=threading.Thread(target=self.create_request,args=(df,j[0],j[1],self.q,self.num))
                    a = time.time()
                    t.start()
                    t.join()
                    k = self.q.get()
                    print(k)
                    d.pop(k)
                    b=time.time()
                    print("Time = ",int(b)-int(a))


    def filter_records(self,municipality):
        """
        Creates a dataframe of munipalities mentioned in the parameters.
        :param municipality: municipality whose dataframe is needed.
        :return:
        """
        print(municipality)
        print("filter records")
        df = pd.read_csv("filtered_mvc.csv")
        df = df.loc[df["Municipality"].isin([municipality])]
        print(df.shape[0])
        return df

    def process_string(self,string_list, _time, _date, muni):
        """
        This method takes in the list and associates the time with the particular record. The list contains
        weather information where the first entry in the list is the weather condition at 1AM to the last entry
        in the list which is the weather condition at 12 AM
        :param string_list: weather list.
        :param _time: time from the vehicle accident data
        :param _date: date of the accident
        :param muni: municipality where the accident occured.
        :return: list containing date, time, municipality and weather conditions.
        """
        data = string_list[(int(_time.split(":")[0])) - 1]
        data = data.split()
        result = []
        result.append(_date)
        result.append(_time)
        result.append(muni)
        result.append(data[2] + data[3])
        result.append(data[4] + data[5])
        result.append(data[6] + data[7])
        result.append(data[8])
        result.append(data[9] + data[10])
        result.append(data[11] + data[12])
        result.append(data[13] + data[14])
        result.append(data[15] + data[16])
        result.append(data[17])
        return result

    def create_request(self,df1,start,end,q,num):
        """
        This method is used to access the county urls to extract the weather information.
        :param df: dataframe of the entire accident dataset
        :return: None
        """
        # dictionary containing URL templates for each county, append date after last forward slash
        url_dict = {"KINGS": "https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA/date/",
                    "BRONX": "https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA/date/",
                    "QUEENS": "https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA/date/",
                    "NEW YORK": "https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA/date/",

                    "BROOKHAVEN": "https://www.wunderground.com/history/daily/us/ny/brookhaven/KNYBROOK285/date/",
                    "HEMPSTEAD": "https://www.wunderground.com/history/daily/us/ny/hempstead/KNYHEMPS2/date/",

                    "ISLIP": "https://www.wunderground.com/history/daily/us/ny/ronkonkoma/KISP/date/",
                    "BABYLON": "https://www.wunderground.com/history/daily/us/ny/ronkonkoma/KISP/date/",

                    "BUFFALO": "https://www.wunderground.com/history/daily/us/ny/buffalo/KBUF/date/",
                    "ROCHESTER": "https://www.wunderground.com/history/daily/us/ny/rochester/KROC/date/",
                    "RICHMOND": "https://www.wunderground.com/history/daily/us/ny/new-york-city/KJFK/date/",

                    "HUNTINGTON": "https://www.wunderground.com/history/daily/us/ny/huntington/KNYHUNTI77/",
                    "OYSTER BAY": "https://www.wunderground.com/history/daily/us/ny/harrison/KHPN/date/"
                    }
        try:
            data_list = []
            df = df1.iloc[start:end]
            driver = webdriver.Firefox()
            for index, row in df.iterrows():

                if url_dict.get(row["Municipality"]) is not None:

                    url = url_dict.get(row["Municipality"])
                    date = datetime.datetime.strptime(row["Date"], "%m/%d/%Y").strftime(
                        "%Y-%m-%d")  # convert date to appropriate format
                    url += str(date)



                    result = []
                    elem=""
                    # extract table from website using table's Xpath
                    try:
                        driver.get(url)
                        elem = driver.find_elements_by_xpath(
                        "/html/body/app-root/app-history/one-column-layout/wu-header/sidenav/mat-sidenav-container/mat-sidenav-content/div/section/div[2]/div[1]/div[5]/div[1]/div/lib-city-history-observation/div/div[2]/table/tbody")
                    except:
                        driver = webdriver.Firefox()
                        continue
                    # access element's text and process the string.
                    for i in elem:
                        string = ""
                        for j in i.text:
                            if j is not "\n":  # append to list when break line element is encountered.
                                string += j
                            else:
                                result.append(string)
                                string = ""
                    try:
                        result = self.process_string(result, row["Time"], row["Date"],
                                            row["Municipality"])  # obtain processed data
                    except:
                        continue
                    data_list.append(result)  # append data to a list



            # write the list to a new dataframe object
            df_data = pd.DataFrame(data_list, index=None, columns=["Date", "Time", "Municipality", "Temperature",
                                                                   "Dew Point", "Humidity", "Wind Direction", "Wind Speed",
                                                                   "Wind Gust", "Pressure", "Precipitation", "Condition"])
            # write dataframe object to CSV
            df_data.to_csv("Weather_municipality"+str(num)+".csv", encoding="utf-8", index=False,mode='a')
            driver.close()
            q.put((start,end))
        except:
            traceback.print_exc()

if __name__ == '__main__':
    a = w_sel(["ROCHESTER","BUFFALO","RICHMOND"],1)
    b = w_sel(["KINGS", "BRONX", "QUEENS","NEW YORK"],2)
    c = w_sel(["BROOKHAVEN", "ISLIP", "BABYLON", "HUNTINGTON","OYSTER BAY"],3)
    # the following objects can be run as individual threads based on the processing power of the system.
    a.run()
    b.run()
    c.run()