import pandas as pd
import os


# Step 1: Import downloaded retail sales of electricity from EIA
cwd = os.getcwd()
# import retail sales (consumption level) data of 2018 from EIA
retail_sales_GWh = pd.read_csv(os.path.join(cwd+"\\raw_data\\Retail_sales_of_electricity_all_sectors_annual.csv"),skiprows=4, index_col=0)
retail_sales_GWh.columns = ["United States", "New England", "Middle Atlantic", "East North Central","West North Central",
                            "South Atlantic", "West South Central", "East South Central","Mountain","Pacific Contiguous","Pacific Noncontiguous"]
# Combine Pacific Contiguous and Pacific Noncontiguous
retail_sales_GWh['Pacific'] = retail_sales_GWh["Pacific Contiguous"]+retail_sales_GWh["Pacific Noncontiguous"]
# Drop Pacific Contiguous and Pacific Noncontiguous
retail_sales_GWh = retail_sales_GWh.drop(['Pacific Contiguous',"Pacific Noncontiguous","United States"], axis=1)
# turn retail sales. Original unit is million kWh (GWh). Divide it by 1000 to turn it into TWh.
retail_sales_TWh = (retail_sales_GWh/1000)
# only take the data from 2018
retail_sales_TWh = retail_sales_TWh.loc[2018]
retail_sales_TWh.transpose()
retail_sales_TWh.index.name = "census_division"
retail_sales_TWh = retail_sales_TWh.to_frame()
retail_sales_TWh



# Step 2: Import forecasted load by scenario from PATHWAY and NREL
four_scenarios_original = pd.read_csv(os.path.join(cwd+"\\raw_data\\Four Scenarios_Original.csv"), index_col=[0,1,2])
# get rid of the data in 2017
del four_scenarios_original['2017']
# replace zero values in 2018 Heavy Duty Reference scenarios with a small value to allow extrapolation
four_scenarios_original.loc[four_scenarios_original['2018'] <= 0, '2018'] = 0.0005



# Step 3: Benchmark 2018 forecast with actual EIA sales
# create an empty DataFrame to store final values using the same structure as four scenarios original
four_scenarios = pd.DataFrame().reindex_like(four_scenarios_original)
# sum up the total load for 2018
total_load_census_scenario = four_scenarios_original.groupby(["census_division", "Scenario"]).sum()
# create an empty Dataframe to store the ratio of load under each catgory over total load of each census divison
ratio = pd.DataFrame().reindex_like(four_scenarios_original)
ratio['2018'] = four_scenarios_original['2018'].div(total_load_census_scenario['2018'])
# replicate the relationship between categories in the Reference scenario to other scenarios
for region, sce, cat in ratio.index.values:
    ratio.loc[pd.IndexSlice[region, sce, cat], '2018'] = ratio.loc[pd.IndexSlice[region, 'Reference', cat], '2018']
# benchmark load in 2018 based on the actual retail sales in 2018
# index: region
for region, row in retail_sales_TWh.iterrows():
    four_scenarios.loc[pd.IndexSlice[region, :, :], '2018'] = ratio.loc[pd.IndexSlice[region, :, :], '2018'] * row[2018]
    # four_scenarios.loc[pd.IndexSlice[region, :, :], '2018'] = ratio.loc[pd.IndexSlice[region, :, :], '2018'] * val.iloc[0]
    # val.iloc[0] prints out the values of the first column in the dataframe



# Step 4: Use benchmarked 2018 load values to forecast load to 2050
# find the relationship of values in future years with respect to 2018
inter_year_relationship = pd.DataFrame().reindex_like(four_scenarios_original)
for region, sce, cat in ratio.index.values:
    inter_year_relationship.loc[pd.IndexSlice[region, sce, cat], :] = \
        four_scenarios_original.loc[pd.IndexSlice[region, sce, cat], :]/four_scenarios_original.loc[pd.IndexSlice[region, sce, cat], '2018']
# extrapolate load forward
for region, sce, cat in four_scenarios.index.values:
    four_scenarios.loc[pd.IndexSlice[region,sce, cat], "2019":"2050"] = \
        inter_year_relationship.loc[pd.IndexSlice[region,sce, cat], "2019":"2050"]*four_scenarios.loc[pd.IndexSlice[region,sce, cat], '2018']


# Step 5: output the benchmarked four_scenarios file
four_scenarios.to_csv(os.path.join(cwd+'\\calc\\four_scenarios.csv'))
