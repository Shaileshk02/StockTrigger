import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import nsepy as nspy
from nsepy import get_history
import datetime as date
import os, time
import glob

print("Starting...")
folder_name = 'D:\shailesh\stk\Bavcopy'
os.chdir(folder_name)
bhavcopy_files = glob.glob(folder_name + "/*.csv")
bhavcopy_files.sort(key=os.path.getmtime)
print(type(bhavcopy_files))
from pandas import DataFrame

column_names = ['MONEYFLOW']
money_flow_ratio = pd.DataFrame(columns=column_names)

#=================================================================
# Money Flow Index calculations
#=================================================================
def CalculateMFI(bhavcopy_files):
    column_names = ['CLOSE']
    close_price_df = pd.DataFrame(columns=column_names)
    close_price_df_daybefore_df = pd.DataFrame(columns=column_names)
    column_names = ['TOTTRDQTY']
    trd_Qty_df = pd.DataFrame(columns=column_names)
    column_names = ['MONEYFLOW']
    raw_money_flow_df = pd.DataFrame(columns=column_names)
    raw_money_flow_daybefore_df = pd.DataFrame(columns=column_names)
    column_names = ['MONEYFLOW']
    positive_money_flow_df = pd.DataFrame(columns=column_names)
    negative_money_flow_df = pd.DataFrame(columns=column_names)
    sum_positive_mf = pd.DataFrame(columns=column_names)
    sum_negative_mf = pd.DataFrame(columns=column_names)


    file_index = 0
    sum_diff = 0
    for file in bhavcopy_files[-15:]:
        file_index = file_index + 1
        bhavcopy = pd.read_csv(file, index_col=0)
        ind = bhavcopy.index
        is_duplicate = ind.duplicated(keep="first")
        not_duplicate = ~is_duplicate
        bhavcopy = bhavcopy[not_duplicate]
        close_price_df = bhavcopy[['CLOSE']].copy()
        close_price_df = pd.DataFrame(close_price_df)
        close_price_df = close_price_df.dropna()

        trd_Qty_df = bhavcopy[['TOTTRDQTY']].copy()
        trd_Qty_df = trd_Qty_df.dropna()

        if (file_index == 1):
            close_price_df_daybefore_df = pd.DataFrame(close_price_df)

            for row_ind in close_price_df_daybefore_df.index:
                positive_money_flow_df.at[row_ind, 'MONEYFLOW'] = 0.0
                negative_money_flow_df.at[row_ind, 'MONEYFLOW'] = 0.0

            sum_positive_mf = positive_money_flow_df
            sum_negative_mf = negative_money_flow_df
            continue

        close_price_df_daybefore_df = close_price_df_daybefore_df.dropna()
        close_price_df = close_price_df.dropna()

        close_index = close_price_df.index
        for row_ind in close_price_df_daybefore_df.index:
            if row_ind in close_index:
                close_price_current = close_price_df.at[row_ind, 'CLOSE']
                close_price_daybefore = close_price_df_daybefore_df.at[row_ind, 'CLOSE']
            else:
                continue

            if((close_price_current - close_price_daybefore) > 0):
                positive_money_flow_df.at[row_ind, 'MONEYFLOW'] =  ((trd_Qty_df.at[row_ind, 'TOTTRDQTY']) * close_price_current)
            elif((close_price_current - close_price_daybefore) < 0):
                negative_money_flow_df.at[row_ind, 'MONEYFLOW'] =  ((trd_Qty_df.at[row_ind, 'TOTTRDQTY']) * close_price_current)

        close_price_df_daybefore_df = pd.DataFrame(close_price_df)
        if(file_index > 1):
            sum_positive_mf = sum_positive_mf + positive_money_flow_df
            sum_negative_mf = sum_negative_mf + negative_money_flow_df

        #for row_ind in close_price_df_daybefore_df.index:
        #print(sum_negative_mf)
        money_flow_ratio = (sum_positive_mf/sum_negative_mf.where(sum_negative_mf != 0, np.nan))
        money_flow_ratio = money_flow_ratio.dropna()


    #print(money_flow_ratio)
    #print("money flow done")
    print("money flow done")
    return money_flow_ratio


def CalculateMACD(bhavcopy_files):
    ema_const_9 = 2 / (9 + 1)
    ema_const_12 = 2 / (12 + 1)
    ema_const_26 = 2 / (26 + 1)

    column_names = ['OPEN', 'CLOSE']
    stock_sum = pd.DataFrame(columns=column_names)
    stock_close_sum = pd.DataFrame(columns=column_names)
    close_ema_9 = pd.DataFrame(columns=column_names)
    close_ema_12 = pd.DataFrame(columns=column_names)
    close_ema_26 = pd.DataFrame(columns=column_names)
    histogram_val = pd.DataFrame(columns=column_names)
    histogram_val_old = pd.DataFrame(columns=column_names)

    file_index = 0
    sum_diff = 0
    for file in bhavcopy_files:
        # print('file index: '+ str(file_index))
        bhavcopy = pd.read_csv(file, index_col=0)
        ind = bhavcopy.index
        is_duplicate = ind.duplicated(keep="first")
        not_duplicate = ~is_duplicate
        bhavcopy = bhavcopy[not_duplicate]
        bhavcopy = bhavcopy[['OPEN', 'CLOSE']].copy()
        #   print(bhavcopy.shape)
        bhavcopy = bhavcopy.dropna()

        if (file_index == 0):
            stock_sum = bhavcopy
        else:
            stock_sum = stock_sum + bhavcopy
            # print(stock_sum.shape)
            stock_sum = stock_sum.dropna()

        if (file_index == 11):
            close_ema_12 = stock_sum
            close_ema_12 = close_ema_12 / (file_index + 1)
        elif (file_index == 25):
            close_ema_26 = stock_sum
            close_ema_26 = close_ema_26 / (file_index + 1)

        if (file_index > 11):
            close_ema_12 = ((bhavcopy - close_ema_12) * ema_const_12) + close_ema_12
            close_ema_12 = close_ema_12.dropna()

        if (file_index > 25):
            close_ema_26 = ((bhavcopy - close_ema_26) * ema_const_26) + close_ema_26
            close_ema_26 = close_ema_26.dropna()
            if (file_index < 35):
                sum_diff = sum_diff + (close_ema_12 - close_ema_26)
            elif (file_index == 35):
                close_ema_9 = sum_diff / 9
                close_ema_9 = close_ema_9.dropna()
            elif (file_index > 35):
                diff_ema = close_ema_12 - close_ema_26
                diff_ema = diff_ema.dropna()
                close_ema_9 = ((diff_ema - close_ema_9) * ema_const_9) + close_ema_9
                close_ema_9 = close_ema_9.dropna()
                histogram_val = diff_ema - close_ema_9
                histogram_val = histogram_val.dropna()

            if (file_index == len(bhavcopy_files) - 1):
                histogram_val_diff = histogram_val_old - histogram_val
                break

            if (file_index > 35):
                histogram_val_old = histogram_val

        file_index = file_index + 1

    hist_index = histogram_val.index
    for val in hist_index:
       if (histogram_val_old.at[val, 'CLOSE'] < 0 and histogram_val.at[val, 'CLOSE'] > 0):
           if val in money_flow_ratio.index:
                mf_ratio = money_flow_ratio.at[val, 'MONEYFLOW']
                mfi = (100 - (100/(1+mf_ratio)))
                if(histogram_val.at[val, 'CLOSE'] > 0.50 and mfi < 65.0):
                    print('Trade:' + str(val) + ": " + str(histogram_val.at[val, 'CLOSE']) + " MFI: " + str(mfi))

    print("=================================================================")
    print("Those may start movement in positive direction")
    for val in hist_index:
        if val in money_flow_ratio.index:
            mf_ratio = money_flow_ratio.at[val, 'MONEYFLOW']
            mfi = (100 - (100 / (1 + mf_ratio)))
        if(histogram_val.at[val, 'CLOSE'] - histogram_val_old.at[val, 'CLOSE'] > 0 and(mfi > 25 and mfi < 40)):
            print(str(val) + "  " + str(histogram_val.at[val, 'CLOSE']))


def CalculateTodaysHammersInDayChart(bhavcopy_files):
    print("Hammers formed for: \n")
    column_names = ['OPEN', 'CLOSE', 'HIGH', 'LOW']
    stock_value = ['SYMBOL', 'VALUE']
    opCloseHighLow_df = pd.DataFrame(columns=column_names)
    positive_stocks = pd.DataFrame(columns=column_names)
    day2_old_Hammer = pd.DataFrame(columns=column_names)
    day1_old_Hammer = pd.DataFrame(columns=column_names)
    negative_stocks = pd.DataFrame(columns=column_names)
    close_open_diff_df = pd.DataFrame(columns=stock_value)
    hammer_ratio_df = pd.DataFrame(columns=stock_value)
    wick_ratio_df = pd.DataFrame(columns=stock_value)
    temp_df = pd.DataFrame(columns=stock_value)


    file_index = 0
    sum_diff = 0
    for file in bhavcopy_files[-3:]:
        file_index = file_index + 1
        bhavcopy = pd.read_csv(file, index_col=0)
        ind = bhavcopy.index
        is_duplicate = ind.duplicated(keep="first")
        not_duplicate = ~is_duplicate
        bhavcopy = bhavcopy[not_duplicate]
        bhavcopy = bhavcopy[['OPEN', 'CLOSE', 'HIGH', 'LOW']].copy()

        opCloseHighLow_df = bhavcopy
        #print(opCloseHighLow_df.shape)
        close_open_diff_df = opCloseHighLow_df['CLOSE'] - opCloseHighLow_df['OPEN']

        #print(close_open_diff_df)
        num_bool  = close_open_diff_df > 0.0
        negative_stocks = bhavcopy[num_bool == False]
        positive_stocks = bhavcopy[num_bool]

        #print(positive_stocks['OPEN'])

        hammer_ratio_df = (positive_stocks['OPEN'] - positive_stocks['LOW'])/(positive_stocks['CLOSE'] - positive_stocks['OPEN'])
        wick_ratio_df = (positive_stocks['OPEN'] - positive_stocks['LOW'])/(positive_stocks['HIGH'] - positive_stocks['CLOSE'])

        num_bool = hammer_ratio_df > 2
        num_bool2 = wick_ratio_df > 4

        result_bool = (num_bool) & (num_bool2)

        if (file_index == 1):
            print("Day before yesterday's hammer formed for: ")
            positive_stocks = positive_stocks[result_bool]
            day2_old_Hammer = positive_stocks
            print(day2_old_Hammer)
        elif(file_index == 2):

            print("\n Yesterd's Confirmation for Day Before Yesterday's hammers: ")
            frame = [positive_stocks, day2_old_Hammer]
            result = pd.concat(frame, axis=1, join="inner")
            print(result)

            print("\nYesterday's hammer formed for: ")
            day1_old_Hammer = positive_stocks[result_bool]
            print(day1_old_Hammer)
        elif(file_index == 3):
            print("positive stocks today")

            print("\n Confirmation for yesterays hammers: ")
            frame = [positive_stocks, day1_old_Hammer]
            result = pd.concat(frame, axis=1, join="inner")
            print(result)
            print("\n Today's Confirmation for Day Before Yesterday's hammers: ")
            frame = [positive_stocks, day2_old_Hammer]
            result = pd.concat(frame, axis=1, join="inner")
            #print(result)
            print("=================================================================")
            print("\nToday hammer formed for: ")
            positive_stocks = positive_stocks[result_bool]
            print(positive_stocks)

















def CalculateTodaysShootingStarsInDayChart(bhavcopy_files):
    print("Shooting star in progress")

def CalculateBoolishEngulfingTodayInDayChart(bhavcopy_files):
    print("Engulfing in progrss")


money_flow_ratio = CalculateMFI(bhavcopy_files)
CalculateMACD(bhavcopy_files)
CalculateTodaysHammersInDayChart(bhavcopy_files)
CalculateTodaysShootingStarsInDayChart(bhavcopy_files)
CalculateBoolishEngulfingTodayInDayChart(bhavcopy_files)

print('end program')