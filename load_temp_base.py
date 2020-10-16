import numpy as np
import pandas as pd
import os
import sys
import xlsxwriter
import datetime

# import user specified inputs
path = os.getcwd()
os.chdir(path)
df_input = pd.read_excel("Load Input.xlsx", sheet_name='Input', skiprows=2, index_col=1)
df_census_div_group = pd.read_excel("Load Input.xlsx", sheet_name='Census_Division', skiprows=0, index_col=0)
df_demand_area_group = pd.read_excel("Load Input.xlsx", sheet_name='Demand_Area', skiprows=0, index_col=0)

scenario = df_input.loc['Scenario', 'value']
loss = df_input.loc['Loss', 'value']



# Step 1: Turn the retail sales imported from four_scenarios.csv into generation by accounting for losses
four_scenarios = pd.read_csv(os.path.join(path+'\\calc\\four_scenarios.csv'), index_col=[0, 1, 2], header=0)
four_scenarios = four_scenarios/(1-loss)



# Step 2: Create a base load file for selected scenario
# create a structure to store base load
four_scenarios_baseload = four_scenarios.groupby(["census_division", "Scenario"]).sum().copy()*0
# base load is a sum of "Buildings Other" and "Industry"
for region, sce, cat in four_scenarios.index.values:
    for year in four_scenarios.columns:
        four_scenarios_baseload.loc[pd.IndexSlice[region, sce], year] = \
            four_scenarios.loc[pd.IndexSlice[region, sce, "Buildings Other"], year]+four_scenarios.loc[pd.IndexSlice[region, sce, "Industry"], year]



# Step 3: Group census division into bigger region
region = []
for div in four_scenarios_baseload.index.get_level_values(0):
    region.append(df_census_div_group.loc[str(div), 'Census Division Group'])
four_scenarios_baseload.insert(0, 'Region', region)
# reset index
four_scenarios_baseload = four_scenarios_baseload.reset_index()
four_scenarios_baseload.set_index(['census_division','Scenario','Region'])



# Step 4: Import the relationship between regions into AURORA
aurora_raw = pd.read_csv(os.path.join(path+"\\raw_data\\Aurora_raw.csv"), index_col=[0,1])
# get the avg hourly load and peak load information for 2018 from AURORA
aurora_2018 = aurora_raw.loc[pd.IndexSlice[2018, :], :]
# assign area to regions
area_group = []
for area in aurora_2018.columns:
    area_group.append(df_demand_area_group.loc[str(area), 'Demand Area Group'])
area_series = [aurora_raw.columns, area_group]
tuples = list(zip(*area_series))
aurora_2018.columns=pd.MultiIndex.from_tuples(tuples, names=['area', 'group'])
# sum the hourly avg load for each region
sum_matrix_aurora = aurora_2018.groupby(level=1, axis=1).sum()
# calculate the relationship of load for each area with respect to the total load within the region
aurora_inter_area_relationship = pd.DataFrame().reindex_like(aurora_2018)
for year, month in aurora_inter_area_relationship.index.values:
    for area, group in aurora_inter_area_relationship.columns:
        aurora_inter_area_relationship.loc[pd.IndexSlice[year, month],pd.IndexSlice[area, group]] = \
            aurora_2018.loc[pd.IndexSlice[year, month],pd.IndexSlice[area, group]]/sum_matrix_aurora.loc[pd.IndexSlice[year, month], pd.IndexSlice[group]]
# calculate peak load ratio for each area
aurora_load_peak_relationship = pd.DataFrame().reindex_like(aurora_2018)
for area, group in aurora_inter_area_relationship.columns:
    aurora_load_peak_relationship.loc[pd.IndexSlice[2018, 14], pd.IndexSlice[area, :]] = \
            aurora_2018.loc[pd.IndexSlice[2018, 14], pd.IndexSlice[area, :]]/aurora_2018.loc[pd.IndexSlice[2018, 13], pd.IndexSlice[area, :]]



# Step 5: Extrapolate base load based on scenario
# this table has the total baseload of group
sum_matrix_baseload = four_scenarios_baseload.groupby(['Region', 'Scenario']).sum()
# calculate the hourly avg load by dividing the total load by the number of hours per year
sum_matrix_baseload_avg = pd.DataFrame().reindex_like(sum_matrix_baseload)
for group, sce in sum_matrix_baseload.index.values:
    for year in sum_matrix_baseload.columns:
        if int(year) % 4 == 0:
            sum_matrix_baseload_avg.loc[pd.IndexSlice[group, sce], pd.IndexSlice[year]] = \
                sum_matrix_baseload.loc[pd.IndexSlice[group, sce], pd.IndexSlice[year]]/8784
        else:
            sum_matrix_baseload_avg.loc[pd.IndexSlice[group, sce], pd.IndexSlice[year]] = \
                sum_matrix_baseload.loc[pd.IndexSlice[group, sce], pd.IndexSlice[year]]/8760
# convert load from TWh to MWh
sum_matrix_baseload_avg_mwh = sum_matrix_baseload_avg*(10**6)
# populate avg hourly load into aurora_final.csv
aurora_final = pd.DataFrame().reindex_like(aurora_raw)
for area, group in aurora_inter_area_relationship.columns:
    for year, num in aurora_final.index.values:
        if int(num) == 13 and 2018 <= int(year) <= 2050 and group != 'OTHER':
            aurora_final.loc[pd.IndexSlice[year, num], pd.IndexSlice[area]] = \
                sum_matrix_baseload_avg_mwh.loc[pd.IndexSlice[group, scenario], pd.IndexSlice[str(year)]] * \
                aurora_inter_area_relationship.loc[pd.IndexSlice[2018, 13], pd.IndexSlice[area, group]]
        else:
            0
# populate peak load into aurora_final.csv
for area, group in aurora_load_peak_relationship.columns:
    for year, num in aurora_final.index.values:
        if int(num) == 14 and 2018 <= int(year) <= 2050 and group != 'OTHER':
            aurora_final.loc[pd.IndexSlice[year, num], pd.IndexSlice[area]] = \
                aurora_final.loc[pd.IndexSlice[year, 13], pd.IndexSlice[area]] * \
                aurora_load_peak_relationship.loc[pd.IndexSlice[2018, 14], pd.IndexSlice[area, group]]
        else:
            0
# copy default load from aurora raw if year is not withint the range and group belongs to "OTHER"
for area, group in aurora_load_peak_relationship.columns:
    for year, num in aurora_final.index.values:
        if int(year) < 2018 or int(year) > 2050 or group == 'OTHER':
            aurora_final.loc[pd.IndexSlice[year, num], pd.IndexSlice[area]] = \
                aurora_raw.loc[pd.IndexSlice[year, num], pd.IndexSlice[area]]
        else:
            0
# replace na with zero
aurora_final.fillna(0, inplace=True)



# Step 6: print the base load with specified scenario name and timestamp
rt = datetime.datetime.now()
now = '{}{}{}_{}{}'.format(rt.year, str(rt.month).zfill(2), str(rt.day).zfill(2), str(rt.hour).zfill(2), str(rt.minute).zfill(2))
aurora_final.to_csv(os.path.join(path+'\\to_aurora\\Base Load_'+scenario+"_"+now+'.csv'))

print("Input table for baseline load is generated in the to_aurora folder")