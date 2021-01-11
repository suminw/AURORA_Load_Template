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
df_specified_area = pd.read_excel("Load Input.xlsx", sheet_name='Self_Specified_Area', skiprows=0, index_col=0).dropna()  # drop nan values in the df_specified_area
df_fuel_table = pd.read_excel("Load Input.xlsx", sheet_name='Fuels_Table', skiprows=0, index_col=0)
# df_storage_table = pd.read_excel("Load Input.xlsx", sheet_name='Storage_Table', skiprows=0, index_col=0)

scenario = df_input.loc['Scenario', 'value']
loss = df_input.loc['Loss', 'value']
ldvm_per = df_input.loc['LDVM_per', 'value']
geographic_area = df_input.loc['Geographic_area', 'value']
ev_battery_duration = df_input.loc['EV_battery_duration', 'value']
fuel_id = df_input.loc['Fuel_ID', 'value']
# storage_id = df_input.loc['Storage_ID', 'value']


# Step 1: Turn the retail sales imported from four_scenarios.csv into generation by accounting for losses
four_scenarios = pd.read_csv(os.path.join(path+'\\calc\\four_scenarios.csv'), index_col=[0, 1, 2], header=0)
four_scenarios = four_scenarios/(1-loss)



# Step 2: Create the load files for each category
# - Space Heating (SH)
# - Water Heating (WH)
# - Heavy Duty Vehicle (HDV)
# - Light Duty Vehicle (LDV)
# create a dataframe to store load for each census division and each scenario with group name
four_scenarios_group = four_scenarios.groupby(["census_division", "Scenario"]).sum()
region = []
for div in four_scenarios_group.index.get_level_values(0):
    region.append(df_census_div_group.loc[str(div), 'Census Division Group'])
four_scenarios_group.insert(0, 'Region', region)
# reset index
four_scenarios_group = four_scenarios_group.reset_index()
four_scenarios_group = four_scenarios_group.set_index(['census_division','Scenario','Region'])

# create a dataframe with only space heating load for each census division and each scenario
SH_load = pd.DataFrame().reindex_like(four_scenarios_group)
for region, sce, cat in four_scenarios.index.values:
    for year in four_scenarios.columns:
        SH_load.loc[pd.IndexSlice[region, sce], year] = four_scenarios.loc[pd.IndexSlice[region, sce, "Buildings: Space Heating"], year]

# create a dataframe with only water heating load for each census division and each scenario
WH_load = pd.DataFrame().reindex_like(four_scenarios_group)
for region, sce, cat in four_scenarios.index.values:
    for year in four_scenarios.columns:
        WH_load.loc[pd.IndexSlice[region, sce], year] = four_scenarios.loc[pd.IndexSlice[region, sce, "Buildings: Water Heating"], year]

# create a dataframe with only heavy duty vehicle load for each census division and each scenario
HDV_load = pd.DataFrame().reindex_like(four_scenarios_group)
for region, sce, cat in four_scenarios.index.values:
    for year in four_scenarios.columns:
        HDV_load.loc[pd.IndexSlice[region, sce], year] = \
        four_scenarios.loc[pd.IndexSlice[region,sce, "Transportation: Heavy Duty"], year] + \
        four_scenarios.loc[pd.IndexSlice[region,sce, "Transportation: Other"], year]
        # lump "transportation other" with "transportation HDV"

# create a dataframe with only light duty vehicle load for each census division and each scenario
LDV_load = pd.DataFrame().reindex_like(four_scenarios_group)
for region, sce, cat in four_scenarios.index.values:
    for year in four_scenarios.columns:
        LDV_load.loc[pd.IndexSlice[region, sce], year] = four_scenarios.loc[pd.IndexSlice[region, sce, "Transportation: Light Duty"], year]

# create a dataframe with total load for each census division and each scenario
total_load = pd.DataFrame().reindex_like(four_scenarios_group)
for region, sce, cat in four_scenarios.index.values:
    for year in four_scenarios.columns:
        total_load.loc[pd.IndexSlice[region, sce], year] = four_scenarios_group.loc[pd.IndexSlice[region, sce, :], year]


category_dict = {"SH_load": SH_load, "WH_load": WH_load, "HDV_load": HDV_load, "LDV_load": LDV_load}



# Step 3: Reallocate AURORA default load into corresponding zones for ERCOT
aurora_raw = pd.read_csv(os.path.join(path+"\\raw_data\\Aurora_raw.csv"), index_col=[0,1])
# percentage comes from Aurora_demand_collection. The number represents the zonal load is comprised of how many % of area load
# ex. ERCOT AEN load = 21.83% * ERCOT_WZ_SouthCen load
percentage = pd.read_csv(os.path.join(path+"\\raw_data\\Aurora_demand_collection.csv"), index_col=0).fillna(value=0)
aurora_edit = aurora_raw.copy()
#clear out the values for ERCOT first because we need to reallocate the load based on demand collection
aurora_edit.loc[pd.IndexSlice[:, :], "ERCOT_AEN":"ERCOT_WZ_West"] = 0
# multiply load of each area by percentage and sum them up
for zone in percentage.index.values:
    aurora_edit[zone] = (percentage.loc[zone] / 100 * aurora_raw).sum(axis=1)
# refill the data for year 2012 as default in AURORA
aurora_edit.loc[pd.IndexSlice[2012, 1:12], :] = 1
aurora_edit.to_csv(os.path.join(path+"\\calc\\aurora_edit.csv"))



# Step 4: Generate new inter-area relationship
# get the avg hourly load and peak load information for 2018 from AURORA
aurora_2018 = aurora_edit.loc[pd.IndexSlice[2018, :], :]
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
aurora_inter_area_relationship.to_csv(os.path.join(path+'\\calc\\aurora_inter_area_relationship_elecload.csv'))


# Step 5: Extrapolate categories of load into each area
# create a template to store SH, WH, HDV and LDV load by area
load_by_area = pd.DataFrame(index=aurora_raw.columns, columns=four_scenarios.columns)
# # group SH, WH, HDV, LDV load by region
# category_dict2 = {'SH_load_group': SH_load, 'WH_load_group': WH_load, 'LDV_load_group': LDV_load, 'HDV_load_group': HDV_load}
# for category in category_dict2.keys():
#     category = category_dict2[category].groupby(['Region', 'Scenario']).sum()
SH_load_group = SH_load.groupby(['Region', 'Scenario']).sum()
WH_load_group = WH_load.groupby(['Region', 'Scenario']).sum()
LDV_load_group = LDV_load.groupby(['Region', 'Scenario']).sum()
HDV_load_group = HDV_load.groupby(['Region', 'Scenario']).sum()
total_load_group = total_load.groupby(['Region', 'Scenario']).sum()

# # create files to store SH, WH, HDV, LDV load
# key_list = ['SH_load_by_area', 'WH_load_by_area', 'HDV_load_by_area', 'LDV_load_by_area']
# for key in key_list:
#     key = pd.DataFrame().reindex_like(load_by_area)
SH_load_by_area = pd.DataFrame().reindex_like(load_by_area)
WH_load_by_area = pd.DataFrame().reindex_like(load_by_area)
LDV_load_by_area = pd.DataFrame().reindex_like(load_by_area)
HDV_load_by_area = pd.DataFrame().reindex_like(load_by_area)
total_load_by_area = pd.DataFrame().reindex_like(load_by_area)

# to get the total SH, WH, LDV, HDV load for each area by year. Multiply it by 10**6 to turn it from TWh to MWh
for area, group in aurora_inter_area_relationship.columns:
    for year in SH_load_by_area.columns:
            if group != 'OTHER':
                SH_load_by_area.loc[area, year] = \
                    SH_load_group.loc[pd.IndexSlice[group, scenario], str(year)] * aurora_inter_area_relationship.loc[pd.IndexSlice[2018, 13], pd.IndexSlice[area, group]]*(10**6)
            else:
                0

for area, group in aurora_inter_area_relationship.columns:
    for year in WH_load_by_area.columns:
            if group != 'OTHER':
                WH_load_by_area.loc[area, year] = \
                    WH_load_group.loc[pd.IndexSlice[group, scenario], str(year)] * aurora_inter_area_relationship.loc[pd.IndexSlice[2018, 13], pd.IndexSlice[area, group]]*(10**6)
            else:
                0

for area, group in aurora_inter_area_relationship.columns:
    for year in LDV_load_by_area.columns:
            if group != 'OTHER':
                LDV_load_by_area.loc[area, year] = \
                    LDV_load_group.loc[pd.IndexSlice[group, scenario], str(year)] * aurora_inter_area_relationship.loc[pd.IndexSlice[2018, 13], pd.IndexSlice[area, group]]*(10**6)
            else:
                0

for area, group in aurora_inter_area_relationship.columns:
    for year in HDV_load_by_area.columns:
            if group != 'OTHER':
                HDV_load_by_area.loc[area, year] = \
                    HDV_load_group.loc[pd.IndexSlice[group, scenario], str(year)] * aurora_inter_area_relationship.loc[pd.IndexSlice[2018, 13], pd.IndexSlice[area, group]]*(10**6)
            else:
                0

for area, group in aurora_inter_area_relationship.columns:
    for year in total_load_by_area.columns:
            if group != 'OTHER':
                total_load_by_area.loc[area, year] = \
                    total_load_group.loc[pd.IndexSlice[group, scenario], str(year)] * aurora_inter_area_relationship.loc[pd.IndexSlice[2018, 13], pd.IndexSlice[area, group]]*(10**6)
            else:
                0


# Step 6: Import shape files obtained from the PATHWAYS team
# Normalized shape:
#     for each hour, electricity consumption is normalized to its total annual consumption
# capacity factor shape/tshourly:
#     for each hour, electricity consumption is divivded by system peak consumption
# Import the normalized shape from PATHWAY Team
normalized = pd.read_excel("Load Input.xlsx", sheet_name='Normalized_Shape', skiprows=0)
# Import the capacity factor time series that needs to get imported into AURORA
tshourly = pd.read_excel("Load Input.xlsx", sheet_name='Capacity_Shape', skiprows=0)
# Insert the two columns to match AURORA format
tshourly.insert(3, "Data", "")
tshourly.insert(4, "Primary Key", "")




# Step 7: Create the Time Series Annual file
area_names = []
if geographic_area == "Self specified":
    area_names = df_specified_area.index.values.tolist()  # get rid of nan imported from Excel
else:
    for area in aurora_raw.columns:
        if area.split("_")[0] == geographic_area:
            area_names.append(area)

n_area = len(area_names)
# create the Time Series Annual file that feeds into AURORA
tsannual_col = ["Demand Area", "Electrification Type", "ID", "Use"]
tsannual_col.extend(list(range(2010, 2055, 1)))  # use extend instead of append to add each element of one list to another
# tsannual_row = list(range(0, n_area*6, 1))
tsannual_row = list(range(0, n_area*5, 1))
tsannual = pd.DataFrame(columns=tsannual_col, index=tsannual_row)
tsannual.columns = tsannual.columns.map(str)

for i in range(0, n_area, 1):
        tsannual.iloc[i, 0] = area_names[i]
        tsannual.iloc[i+n_area*1, 0] = area_names[i]
        tsannual.iloc[i+n_area*2, 0] = area_names[i]
        tsannual.iloc[i+n_area*3, 0] = area_names[i]
        tsannual.iloc[i+n_area*4, 0] = area_names[i]
        # tsannual.iloc[i+n_area*5, 0] = area_names[i]
tsannual.iloc[n_area*0:n_area*1, 1] = "SH"
tsannual.iloc[n_area*1:n_area*2, 1] = "WH"
tsannual.iloc[n_area*2:n_area*3, 1] = "HDV"
tsannual.iloc[n_area*3:n_area*4, 1] = "LDVU"
tsannual.iloc[n_area*4:n_area*5, 1] = "LDVM"
# tsannual.iloc[n_area*5:n_area*6, 1] = "LDVM_storage" # need two sets of rows for managed charging

for i in range(0, n_area*5, 1):
    tsannual.iloc[i, 2] = tsannual.iloc[i, 0]+"_capacity_"+tsannual.iloc[i, 1]+str(i % n_area)
# for i in range(n_area*5, n_area*6, 1):
#     tsannual.iloc[i, 2] = tsannual.iloc[i, 0]+"_max_storage_"+tsannual.iloc[i, 1]+str(i % n_area)

# capacity = annual energy in that zone * max of the normalized shape
# other resources should have negative capacity because it's modeled as a generation source
# managed charging should have positive capacity because it's modeled as a battery
tsannual = tsannual.set_index(['Demand Area', 'Electrification Type'])
for area, load_type in tsannual.index.values:
    for year in load_by_area.columns:
            tsannual.loc[pd.IndexSlice[area, 'SH'], year] = -SH_load_by_area.loc[area, year]*normalized[df_specified_area.loc[area, "SH_shape"]].max()
            tsannual.loc[pd.IndexSlice[area, 'WH'], year] = -WH_load_by_area.loc[area, year]*normalized[df_specified_area.loc[area, "WH_shape"]].max()
            tsannual.loc[pd.IndexSlice[area, 'HDV'], year] = -HDV_load_by_area.loc[area, year]*normalized[df_specified_area.loc[area, "HDV_shape"]].max()
            tsannual.loc[pd.IndexSlice[area, 'LDVU'], year] = -LDV_load_by_area.loc[area, year]*(1-ldvm_per)*normalized[df_specified_area.loc[area, "LDVU_shape"]].max()
            # different from previous version. Model managed charging the same as unmanaged, a consumption resource.
            tsannual.loc[pd.IndexSlice[area, 'LDVM'], year] = -LDV_load_by_area.loc[area, year] * ldvm_per * normalized[df_specified_area.loc[area, "LDVM_shape"]].max()

            # use charging shape to calculate the capacity of LDVM resources
            # tsannual.loc[pd.IndexSlice[area, 'LDVM'], year] = LDV_load_by_area.loc[area, year]*ldvm_per*normalized[df_specified_area.loc[area, "LDVM_shape"]].max()
            # tsannual.loc[pd.IndexSlice[area, 'LDVM_storage'], year] = ev_battery_duration * LDV_load_by_area.loc[area, year]*ldvm_per*normalized[df_specified_area.loc[area, "LDVM_shape"]].max()

# create a list with matching shape names to the corresponding rows
shape_list = tsannual.reset_index().loc[:, "Demand Area":"ID"].copy()
shape_list["Shape"] = 0  # insert a column called shape
shape_list = shape_list.set_index(['Demand Area', 'Electrification Type'])
for area, load_type in shape_list.index.values:
    shape_list.loc[pd.IndexSlice[area, load_type], 'Shape'] = df_specified_area.loc[area, str(load_type + "_shape")]

    # if load_type != 'LDVM_storage':
    #     shape_list.loc[pd.IndexSlice[area, load_type], 'Shape'] = df_specified_area.loc[area, str(load_type+"_shape")]
    # else:
    #     shape_list.loc[pd.IndexSlice[area, load_type], 'Shape'] = df_specified_area.loc[area, 'LDVM_discharging_shape']

shape_list = shape_list.reset_index()



# Step 8: Create Resources Disaggregated file
resources_disaggregated_col = pd.read_csv(os.path.join(path+"\\raw_data\\resources disaggregated column names.csv"), header=[0]).columns
resources_disaggregated_row = list(range(0, n_area*5, 1)) # no need to create lines for the max_storage row
resources_disaggregated = pd.DataFrame(columns=resources_disaggregated_col, index=resources_disaggregated_row)
# fill in values

tsannual = tsannual.reset_index()
for i in resources_disaggregated.index.values:
    resources_disaggregated.loc[i, 'Zone Name'] = tsannual.loc[i, 'Demand Area']
    resources_disaggregated.loc[i, 'Electrification Type'] = tsannual.loc[i, 'Electrification Type']
    resources_disaggregated.loc[i, "Reporting"] = "FALSE"
    resources_disaggregated.loc[i, "ID"] = tsannual.loc[i, "ID"].split("_")[-1]
    resources_disaggregated.loc[i, "Name"] = tsannual.loc[i, "Demand Area"] + "_" + tsannual.loc[i, "Electrification Type"]
    resources_disaggregated.loc[i, "Utility"] = "na"
    resources_disaggregated.loc[i, "Heat Rate"] = 0
    resources_disaggregated.loc[i, "Capacity"] = "yr_" + tsannual.loc[i, "ID"]
    resources_disaggregated.loc[i, "Nameplate Capacity"] = "yr_" + tsannual.loc[i, "ID"]

    resources_disaggregated.loc[i, "Area"] = df_specified_area.loc[tsannual.loc[i, "Demand Area"], 'Num']  # from the area_number defined by user

    resources_disaggregated.loc[i, "Variable O&M":"Non Cycling"] = "FUEL"
    resources_disaggregated.loc[i, "Must Run"] = 0
    resources_disaggregated.loc[i, "Start Up Costs"] = "FUEL"
    resources_disaggregated.loc[i, "Minimum Capacity"] = 0
    resources_disaggregated.loc[i, "Resource Begin Date"] = "1/1/2017"
    resources_disaggregated.loc[i, "Resource End Date"] = "12/31/2054"
    resources_disaggregated.loc[i, "Capacity Monthly Shape"] = "FUEL"
    resources_disaggregated.loc[i, "Ramp Rate"] = "FUEL"
    resources_disaggregated.loc[i, "Min Up Time":"Min Down Time"] = 0
    resources_disaggregated.loc[i, "Maintenance Cycle":"Maintenance Length"] = "FUEL"
    resources_disaggregated.loc[i, "Schedule Maintenance"] = "FUEL"
    resources_disaggregated.loc[i, "Resource Fixed"] = "TRUE"
    resources_disaggregated.loc[i, "Can Drop"] = "FUEL"

    resources_disaggregated.loc[i, "Hourly Shaping Factor"] = "hr_ElecLoadShapes|" + shape_list.loc[i, "Shape"] + "|2009"
    resources_disaggregated.loc[i, "Peak Credit"] = 1
    resources_disaggregated.loc[i, "Include Capability In Net Demand"] = "TRUE"
    resources_disaggregated.loc[i, "Fuel"] = "E3_NR"

    # if resources_disaggregated.loc[i, "Electrification Type"] != "LDVM":
    #     resources_disaggregated.loc[i, "Hourly Shaping Factor"] = "hr_ElecLoadShapes|" + shape_list.loc[i, "Shape"] + "|2009"
    #     resources_disaggregated.loc[i, "Peak Credit"] = 1
    #     resources_disaggregated.loc[i, "Include Capability In Net Demand"] = "TRUE"
    #     resources_disaggregated.loc[i, "Fuel"] = "E3_NR"
    # else:
    #     resources_disaggregated.loc[i, "Storage Inflow"] = "hr_ElecLoadShapes|" + shape_list.loc[i+n_area, "Shape"] + "|2009"  # use discharging shape for storage inflow
    #     resources_disaggregated.loc[i, "Storage Control Type"] = "Demand"
    #     resources_disaggregated.loc[i, "Recharge Capacity"] = "yr_" + tsannual.loc[i, "ID"]
    #     resources_disaggregated.loc[i, "Maximum Storage"] = "yr_" + tsannual.loc[i + n_area, "ID"]
    #     resources_disaggregated.loc[i, "Initial Contents"] = 0.5
    #     resources_disaggregated.loc[i, "Peak Credit"] = -1
    #     resources_disaggregated.loc[i, "Storage ID"] = storage_id
    #     resources_disaggregated.loc[i, "Fuel"] = "PS"

    resources_disaggregated.loc[i, "zREM Status"] = "OP"
    resources_disaggregated.loc[i, "zREM Commercial Date"] = 2018



# Step 9: print the electrification load files with specified scenario name and timestamp
rt = datetime.datetime.now()
now = '{}{}{}_{}{}'.format(rt.year, str(rt.month).zfill(2), str(rt.day).zfill(2), str(rt.hour).zfill(2), str(rt.minute).zfill(2))
resources_disaggregated.to_csv(os.path.join(path+'\\to_aurora\\Resources Disaggregated_'+scenario+"_"+now+'.csv'))
tshourly.to_csv(os.path.join(path+'\\to_aurora\\Time Series Hourly_'+scenario+"_"+now+'.csv'))
tsannual.to_csv(os.path.join(path+'\\to_aurora\\Time Series Annual_'+scenario+"_"+now+'.csv'))
df_fuel_table.to_csv(os.path.join(path+'\\to_aurora\\Fuels Table_'+scenario+"_"+now+'.csv'))
# df_storage_table.to_csv(os.path.join(path+'\\to_aurora\\Storage Table_'+scenario+"_"+now+'.csv'))

print("Input tables for electrification load is generated in to_aurora folder") 

# output intermediate calculations
SH_load_by_area.to_csv(os.path.join(path+'\\calc\\SH_load_by_area_'+scenario+"_"+now+'.csv'))
WH_load_by_area.to_csv(os.path.join(path+'\\calc\\WH_load_by_area_'+scenario+"_"+now+'.csv'))
HDV_load_by_area.to_csv(os.path.join(path+'\\calc\\HDV_load_by_area_'+scenario+"_"+now+'.csv'))
LDV_load_by_area.to_csv(os.path.join(path+'\\calc\\LDV_load_by_area_'+scenario+"_"+now+'.csv'))
total_load_by_area.to_csv(os.path.join(path+'\\calc\\total_load_by_area_'+scenario+"_"+now+'.csv'))


# SH_load_group_avg = pd.DataFrame().reindex_like(SH_load_group)
# for group, sce in SH_load_group_avg.index.values():
#     for year in SH_load_group_avg.columns:
#         if int(year) % 4 == 0:
#             SH_load_group_avg.loc[pd.IndexSlice[group, sce], year] = SH_load_group.loc[pd.IndexSlice[group, sce], year]/8784
#         else:
#             SH_load_group_avg.loc[pd.IndexSlice[group, sce], year] = SH_load_group.loc[pd.IndexSlice[group, sce], year]/8760

