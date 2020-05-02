import pandas as pd
from collections import defaultdict
from sklearn.model_selection import cross_val_score
from sklearn.svm import SVC



def read_csv():
    """
    This method is used to read the csv that contains accident info from all counties.
    We remove all the counties that less than 10000 recorded accidents.
    :return: a dataframe with municipalities that have > 10000 recorded accidents.
    """
    df = pd.read_csv("motorcrash.csv")
    d = defaultdict(int)  # count the number times each Municipality occurs and store in dictionary
    for i in df["Municipality"]:
        d[i] += 1
    remove = []
    for key, value in d.items():
        if value < 10000:
            remove.append(key)  # remove all the keys that have less than 10K occurences
    for i in remove:
        d.pop(i)
    print(d)
    return df.loc[df["Municipality"].isin(d.keys())]  # return dataframe with Municipalities greater than 10K


def remove_duplicates_and_combine(a, b, c):
    """
    This method is used to combine the files created by different instances.
    :param a: csv file name
    :param b: csv file name
    :param c: csv file name
    :return: combined dataframe
    """
    df1 = pd.read_csv(a)
    df2 = pd.read_csv(b)
    df3 = pd.read_csv(c)
    df1 = df1.drop_duplicates()
    df2 = df2.drop_duplicates()
    df3 = df3.drop_duplicates()
    result = pd.concat([df1, df2, df3])
    result.drop_duplicates()
    result.to_csv("combined_weather", encoding='utf-8', index=False)
    return result


def preprocess_weather(df):
    """
    preprocess and clean data extracted from website.
    :param df: combined dataframe
    :return: cleaned dataframe
    """
    df = df.drop(['Wind Direction'], axis=1)
    df = df.drop(['Pressure'], axis=1)
    # removing value "municipality" from column Municipality
    df = df[~df["Municipality"].str.contains("Municipality")]
    # removing string values in Precipitation
    for i in ["inPartly", "inMostly", "inHeavy"]:
        df = df[~df["Precipitation"].str.contains(i)]
    # remove F in temperature column and rename column
    for i in list(df["Temperature"].unique()):
        df["Temperature"] = df["Temperature"].replace(i, i.replace('F', ""))
    df = df.rename(columns={"Temperature": "Temperature(F)"})
    # remove F in Dew Point and rename column
    for i in list(df["Dew Point"].unique()):
        df["Dew Point"] = df["Dew Point"].replace(i, i.replace('F', ""))
    df = df.rename(columns={"Dew Point": "Dew Point(F)"})
    # remove % in Humidity and rename column
    for i in list(df["Humidity"].unique()):
        df["Humidity"] = df["Humidity"].replace(i, i.replace('%', ""))
    df = df.rename(columns={"Humidity": "Humidity(%)"})
    # remove mph in wind speed and rename column
    for i in list(df["Wind Speed"].unique()):
        df["Wind Speed"] = df["Wind Speed"].replace(i, i.replace('mph', ""))
    df = df.rename(columns={"Wind Speed": "Wind Speed(mph)"})
    # remove mph in wind gust and rename column
    for i in list(df["Wind Gust"].unique()):
        df["Wind Gust"] = df["Wind Gust"].replace(i, i.replace('mph', ""))
    df = df.rename(columns={"Wind Gust": "Wind Gust(mph)"})
    # remove inches in Precipitation and rename column
    for i in list(df["Precipitation"].unique()):
        df["Precipitation"] = df["Precipitation"].replace(i, i.replace('in', ""))
    df = df.rename(columns={"Precipitation": "Precipitation(in)"})
    df["Condition"].replace({"Partly": "Cloudy", "Mostly": "Cloudy", "Fair": "Clear", "Light": "Rain",
                             "Heavy": "Rain", "Wintry": "Snow"}, inplace=True)
    # for i in list(df):
    #     print(i," = ",df[i].unique())
    return df


def preprocess_accident(df):
    df = df.drop(
        ['Year', 'Crash Descriptor', 'Day of Week', 'Police Report', 'Collision Type Descriptor',
         'County Name', 'Road Descriptor', 'Traffic Control Device', 'DOT Reference Marker Location',
         'Pedestrian Bicyclist Action', 'Event Descriptor'], axis=1)
    # for i in list(df):
    #     print(i," = ",df[i].unique())
    return df


def combine_weather_accident(df1, df2):
    result_df = pd.merge(left=df1, right=df2, how='left', left_on=['Date', 'Time', 'Municipality'],
                         right_on=['Date', 'Time',
                                   'Municipality'])
    count = 0
    for i in result_df["Weather Conditions"]:
        if i == "Unknown":
            result_df["Weather Conditions"].iloc[count,] = result_df["Condition"].iloc[count,]
        count += 1
    result_df["Weather Conditions"].replace({"Sleet/Hail/Freezing Rain": "Snow", "Fog/Smog/Smoke": "Fog", "Haze": "Fog",
                                             "Thunder": "Other", "T-Storm": "Other", "Patches": "Other",
                                             "Blowing": "Other", "Mist": "Other", "Small": "Other", "Drizzle": "Other",
                                             "Other*": "Other"}, inplace=True)

    result_df.drop("Condition", axis=1, inplace=True)
    result_df["Road Surface Conditions"].replace({"Muddy": "Slush", "Flooded Water": "Wet", "Other": "Unknown"},
                                                 inplace=True)
    result_df["Number of Vehicles Involved"] = result_df["Number of Vehicles Involved"].apply(
        lambda x: 4 if (x > 4) else x)
    result_df.to_csv("combined.csv", encoding='utf-8', index=False)
    # creating one hot encoding
    result_df = pd.get_dummies(result_df,
                               columns=["Lighting Conditions", "Weather Conditions", "Road Surface Conditions"],
                               prefix=None)
    result_df.to_csv("combined_with_hot_encoding.csv", encoding='utf-8', index=False)

    return result_df


def classifier(df):
    df2 = df["Number of Vehicles Involved"]
    df = df[["Lighting Conditions_Daylight", "Weather Conditions_Cloudy",
             "Lighting Conditions_Unknown", "Road Surface Conditions_Dry", "Road Surface Conditions_Unknown",
             "Temperature(F)",
             "Wind Speed(mph)", "Dew Point(F)", "Weather Conditions_Clear", "Wind Gust(mph)"]]

    cls = SVC(random_state=50)
    score_list = cross_val_score(cls, df, df2, cv=5)
    print(score_list)
    print(score_list.mean())


if __name__ == '__main__':
    df = read_csv()
    df.to_csv("filtered_mvc.csv", encoding='utf-8', index=False)
    combined_weather = remove_duplicates_and_combine("Weather_municipality1.csv", "Weather_municipality2.csv",
                                                     "Weather_municipality3.csv")
    combined_weather = preprocess_weather(combined_weather)
    df = preprocess_accident(df)
    result = combine_weather_accident(combined_weather, df)
    classifier(result)


