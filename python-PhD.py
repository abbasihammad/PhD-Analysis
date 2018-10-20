# -*- coding: utf-8 -*-
"""
Created on Tue Mar 27 16:09:22 2018
@author: Hammad
This code is part of the author's PhD dissertation
Copyright (C) 2018

Objective: To perform the data preprocessing to prepare the data for the analysis of the author's PhD dissertation
Langauges used: Python, SAS, and SQL
Packages used in Python: Numpy, Pandas, OS 
Procedure & tools used in SAS : Proc means, Proc Freq, Proc Import, Proc Export, Proc logistics, Proc SQL, Macros
"""


"""
Python code
"""
# Set your working directory with os.chdir()
# importing packages required for manipulating the data 

import os 
import pandas as pd
import numpy as np
import matplotlib as mlt
import seaborn as sn

os.chdir("C:\\Users\\Hammad\\Desktop\\Maaz")

# Created a for loop to read all 194 excel files and append them by using append() function in a data frame called "data". There were 22290 rows 
# and 294 columns in the data frame "data". Then the names of column were saved in the list "names" to keep track of an index. There were 294 columns 
# name in the list "names"

data=pd.DataFrame()
for i in range(1,195):
    st= str("a")+str(i)+str(".xlsx")
    temp=pd.read_excel(st)
    data=data.append(temp)
names=list(data.columns.values)
	
# In the next step, Only the relevant columns were save in the list "id" using column index in the data frame named "df1" containing 22290 rows and 
# 31 columns

id=[30,266,26,29,57,58,59,60,61,68,76,77,78,79,80,81,82,267,268,269,270,271,278,286,287,288,289,290,291,292]
nm=[]
for i in range(0,len(id)):
    nm.append(names[id[i]])
df1=data[nm]

# Index was reset. Remember we append 194 files in one data frame. This repeated every index number 194 times. To fix this "reset_index" was used.
test=df1.reset_index()
df1=test.drop('index',axis=1)

# In the next step, we merged all 13 questions and  13 answers column in one respective columns respectively. In the original data frame , the 
# questions were mixed in each question columns so as answers in each answer column. So we first need to merge all questions and their answers in one 
# column so that we can sort the questions and make a separate column for each of them. To do this we need to maintain unique identifiers that can 
# track the records based on responses by unique workers in each factory. So we had "fac_id" as factory identifiers and "user_id" as user identifier 
# along with duration of the call and timestamps. Each page_name column represent a question in the survey and user_input represent the answer. There 
# were 13 questions and their respective answers. So, we merged page_name-1 to page_name-13 into one column "questions" and so as user_input-1 to 
# user_input-13 into one column "answers" by using "concat". The code for page_name is below:
 
dataframe1=df1[nm[0:5]]
cn=[]
cn=nm[0:3]
cn.append(nm[5])
dataframe2=df1[cn]
dataframe2=dataframe2.rename(columns={'page_name-10': 'page_name-1'})
new=pd.concat([dataframe1,dataframe2])

for i in range(6,17):
    cn=[]
    cn=nm[0:3]
    cn.append(nm[i])
    dataframe2=df1[cn]
    dataframe2=dataframe2.rename(columns={nm[i]: 'page_name-1'})
    new=pd.concat([new,dataframe2])

# the code for user_input is:
	
cn=nm[0:4]
cn.append(nm[17])
dataframe1=df1[cn]

for i in range(18,30):
    cn=nm[0:4]
    cn.append(nm[i])
    dataframe2=df1[cn]
    dataframe2=dataframe2.rename(columns={nm[i]: 'user_input-1'})
    dataframe1=pd.concat([dataframe1,dataframe2])


dataframe1['questions']=new['page_name-1']
dataframe1['answers']=new['user_input-1']

# Now the sorting is done based on questions

dataframe1=dataframe1.sort_values(by=['questions'])

# Next we created a new column "Version" which splits the rows based on timestamps into 1,2,& 3. The survey was updated 3 times in the year 2016 and 
# each survey contains different questions. Version 1 is responses from Jan to May, Version 2 is from Jun to Aug, and Version 3 is from Sept to Dec.
# Version column is created by creating test_func:

def test_func(df):
    """ Test Function for generating new value"""
    if int(df['conversation_created_at'][5:7]) <=5:
        return 1
    elif int(df['conversation_created_at'][5:7]) <=8:
        return 2
    else:
        return 3
dataframe1["version"]=dataframe1.apply(test_func,axis=1)

# The file was exported to the desired folder 

from pandas import ExcelWriter
writer = ExcelWriter('Hammadfinal.xlsx')
dataframe1.to_excel(writer,'Sheet5')
writer.save()

# After cleaning the data using python, the files were imported to SAS to extract the mean and the percentage proportion of yes responses for each 
# survey question across all factory workers per factory. Proc import was used to import the file. The dataset “Hammadfinalv1” represent the survey 
# responses from version 1 (version 1 and version 2 from previous python work was combined into one data set) of survey whereas the dataset 
# “Hammadfinalv2” represent the survey responses from version 2*/

libname hammad 'C:/Users/mhabbasi/Desktop/PhD code';
proc import datafile="C:\Users\mhabbasi\Desktop\PhD code\Hammadfinalv1.xlsx" dbms= xlsx out=Hammadfinal;
run;
proc import datafile="C:\Users\mhabbasi\Desktop\PhD code\Hammadfinalv2.xlsx" dbms=xlsx out=Hammadfinal23;
run;

# Proc sort was used to sort the survey responses by factory identifier */
proc sort data=hammadfinal;
by fac_id;
run; 
proc sort data=Hammadfinal23;
by fac_id;
run;

# Proc means was used to calculate the mean of survey responses on sanitation question and the duration of each survey. Worker responses on 
# sanitation questions were on Likert scale. We took the mean of these responses in order to see if the responses are satisfactory on sanitation 
# related issues */

proc means data=hammadfinal n mean;
var duration Sanitation_Canteen Sanitation_toilet;
class fac_id;
output out =v12 mean= mean_duration mean_Sanitation_Canteen mean_Sanitation_toilet; 
run;
proc means data=Hammadfinal23 n mean;
var duration;
class fac_id;
output out =v3 mean= mean_duration; 
run;

# After calculating mean, the datasets were again sorted by factory identifier before exporting the data using proc export */

Proc sort data= v12 out = hammad.v12;
by fac_id;
run;
Proc sort data= v3 out = hammad.v3;
by fac_id;
run;
proc export data=hammad.v12 outfile="C:\Users\mhabbasi\Desktop\PhD code\v12.xlsx"
dbms=xlsx replace;
run;
proc export data=hammad.v3 outfile="C:\Users\mhabbasi\Desktop\PhD code\v3.xlsx"
dbms=xlsx replace;
run;

# Proc freq was used to calculate the frequency and percent frequency of workers responses per survey question per factory. Each survey question 
# responses were saved in a separate temporary dataset using Out option

proc freq data=hammadfinal;
tables fac_id*Fire_Exits / noprint Out= freq1;
run;
proc freq data=hammadfinal;
by fac_id;
tables fac_id*Feedback / noprint Out= freq2;
run;
proc freq data=hammadfinal;
by fac_id;
tables fac_id*Long_Hours / noprint Out= freq3;
run;
proc freq data=hammadfinal;
by fac_id;
tables fac_id*abuse/ noprint Out= freq4;
run;
proc freq data=hammadfinal;
by fac_id;
tables fac_id*Child_Labor / noprint Out= freq5;
run;
proc freq data=hammadfinal;
by fac_id;
tables fac_id*Wages / noprint Out= freq6;
run;
proc freq data=hammadfinal;
by fac_id;
tables fac_id*Sanitation_Canteen / noprint Out= freq7;
run;
proc freq data=hammadfinal;
by fac_id;
tables fac_id*Sanitation_toilet / noprint  Out= freq8;
run;
proc freq data=hammadfinal;
by fac_id;
tables fac_id*Recommendation / noprint Out= freq9; 
run;
proc freq data=hammadfinal23;
by fac_id;
tables fac_id*Forced_Labor / noprint Out= freqa; 
run;
proc freq data=hammadfinal23;
by fac_id;
tables fac_id*FOA / noprint Out= freqb; 
run;
proc freq data=hammadfinal23;
by fac_id;
tables fac_id*Clean_Water / noprint Out= freqc; 
run;

# The dataset was again sorted to prepare the datasets for merging

Proc sort data= freq1 out = freq1;
by fac_id;
run;
Proc sort data= freq2 out = freq2;
by fac_id;
run;
Proc sort data= freq3 out = freq3;
by fac_id;
run;
Proc sort data= freq4 out = freq4;
by fac_id;
run;
Proc sort data= freq5 out = freq5;
by fac_id;
run;
Proc sort data= freq6 out = freq6;
by fac_id;
run;
Proc sort data= freq7 out = freq7;
by fac_id;
run;
Proc sort data= freq8 out = freq8;
by fac_id;
run;
Proc sort data= freq9 out = freq9;
by fac_id;
run;
Proc sort data= freqa out = freqa;
by fac_id;
run;
Proc sort data= freqb out = freqb;
by fac_id;
run;
Proc sort data= freqc out = freqc;
by fac_id;
run;

# all data sets representing each survey question category were merged together to get survey responses including percent proportion of worker 
# responses per question per factory in one data set. Column was renamed by using rename function) 

data freqv12;
    merge freq1 (rename= (COUNT= firecount PERCENT= firepercent))
          freq2 (rename= (COUNT= feedbackcount PERCENT= feedbackpercent))
          freq3 (rename= (COUNT= longhrcount PERCENT= longhrpercent))
          freq4(rename= (COUNT= abusecount PERCENT= abusepercent))
          freq5(rename= (COUNT= childlabrcount PERCENT= childlabrpercent))
          freq6(rename= (COUNT= wagescount PERCENT= wagespercent))
          freq7(rename= (COUNT= sancanteencount PERCENT= sancanteenpercent))
          freq8(rename= (COUNT= santoiletcount PERCENT= santoiletpercent))
          freq9(rename= (COUNT= recommendationcount PERCENT= recommendationpercent));
    by fac_id;
run;
data freqv3;
    merge freqa (rename= (COUNT= forcedcount PERCENT= forcedpercent))
          freqb (rename= (COUNT= foacount PERCENT= foapercent))
          freqc (rename= (COUNT= cleanwatercount PERCENT= cleanwaterpercent));
    by fac_id;
run; 

# duplicates observations were removed using nodupkey

proc sort data= freqv12 nodupkey out= freqv12;
    by fire_exits fac_id;
	run;
Proc sort data= freqv12 out= freqv12;
   by fac_id;
   run;
proc sort data= freqv3 nodupkey out= freqv3;
    by clean_water fac_id;
	run;
Proc sort data= freqv3 out= freqv3;
   by fac_id;
   run; 

# Irrelevant variables were dropped using drop option, Responses that contained ‘1’ was retained as we were only interested in yes responses. 
# Columns were labelled with proper names using Label option 

data freqv12 (drop= firecount feedbackcount longhrcount abusecount 
               childlabrcount wagescount sancanteencount santoiletcount
               recommendationpercent);
set freqv12;
if fire_exits = 1 or feedback = 1 or long_hours = 1 or abuse = 1 or child_labor =1 or wages = 1 or 
   sanitation_canteen = 1 or sanitation_toilet = 1 or recommendation = 1;
label firepercent = 'Fire Safety' feedbackpercent = 'Feedback' longhrpercent = 'Long Hours' abusepercent = 'Abuse'
          childlabrpercent = 'Child Labor' wagespercent = 'Wages' sancanteenpercent = 'Sanitation Canteen'
          santoiletpercent = 'Sanitation Toilet' recommendationcount = 'Recommendation';
run;
data freqv3 (drop= forcedcount foacount cleanwatercount);
set freqv3;
if forced_labor = 1 or foa = 1 or clean_water = 1;
label forcedpercent = 'Forced Overtime' foapercent = 'FOA' cleanwaterpercent = 'Clean Water';
run;

# Datasets were again sorted before exporting the excel files using proc export

Proc sort data= freqv12 out = hammad.freqv12;
by fac_id;
run;
Proc sort data= freqv3 out = hammad.freqv3;
by fac_id;
run;
proc export data=hammad.freqv12 outfile="C:\Users\mhabbasi\Desktop\hammad\freqv12.xlsx"
dbms=xlsx replace;
run;
proc export data=hammad.freqv3 outfile="C:\Users\mhabbasi\Desktop\hammad\freqcv3.xlsx"
dbms=xlsx replace;
run;

# To address the problem of imbalanced class, three datasets representing each survey version (i.e., mod12 for version 1,mod12lg for version2, & 
# v3 for version3) were oversampled by under-sampling the majority class. 16 negative factories were manually combined with roughly 32 non-negative 
# factories to maintain the minority ratio to majority class as 1:3. For mod12, the oversampled datasets are from mod121 to mod125. Same applies
# to mod12lg and v3.

libname hammad 'C:\Users\Hammad\Desktop\new oversampl';

# Importing all datasets into sas

proc import datafile="C:\Users\Hammad\Desktop\new oversampl\mod12.xlsx"  dbms = xlsx out=mod12; 
run; # same procedure applied to datasets from mod121 to mod125

proc import datafile="C:\Users\Hammad\Desktop\new oversampl\mod12lg.xlsx"  dbms = xlsx out=mod12lg;
run; # same procedure applied to datasets from mod12lg1 to mod12lg5

proc import datafile="C:\Users\Hammad\Desktop\new oversampl\v3.xlsx"  dbms = xlsx out=v3;
run; # same procedure applied to datasets from v31 to v35

# stepwise regression for variable selection and then logistic regression with stepwise variable selection and then logistic regression 
# on selected variables from each over sampled data

# stepwise regression for variable selection for factory characteristics from workers survey

proc reg data= mod12; # same procedure applied to datasets from mod121 to mod125 
model headline= sanitation_toilet -- fire/
selection = stepwise vif tol collinoint;
run;

proc reg data= mod12lg; # same procedure applied to datasets from mod12lg1 to mod12lg5
model headline= sanitation_canteen -- recommendation/
selection = stepwise vif tol collinoint;
run;

proc reg data= v3; # same procedure applied to datasets from v31 to v35 
model headline= forced_labor -- clean_water/
selection = stepwise vif tol collinoint;
run;

# stepwise regression for variable selection for factory characteristics from publicly available information 

proc reg data= mod12; # same procedure applied to datasets from mod121 to mod125 
model headline= buyer_supplier_relationship -- cooperative_approach/
selection = stepwise vif tol collinoint;
run;

# Stepwise logistic regression with input from stepwise regression for factory characteristics from workers survey

proc logistic data = mod12; # same procedure applied to datasets from mod121 to mod125 
model headline(event= "1") = sanitation_toilet/ 
selection = stepwise clodds=pl;
run;

proc logistic data = mod12lg; # same procedure applied to datasets from mod12lg1 to mod12lg5 
model headline(event= "1") = sanitation_canteen/ 
selection = stepwise clodds=pl;
run;

proc logistic data = v3; # same procedure applied to datasets from v31 to v35 
model headline(event= "1") = foa forced_labor clean_water/ 
selection = stepwise clodds=pl;
run;

# Stepwise logistic regression with input from stepwise regression for factory characteristics from publicly available information 

proc logistic data = mod12; # same procedure applied to datasets from mod121 to mod125 
model headline(event= "1") = certified cooperative_approach/ 
selection = stepwise clodds=pl;
run;

# Stepwise logistic regression with exact conditional test for factory characteristics from workers survey

proc logistic data= mod12; # same procedure applied to datasets from mod121 to mod125 
model headline(event= "1") = sanitation_toilet;
exact sanitation_toilet/estimate;
run;

proc logistic data= mod12lg; # same procedure applied to datasets from mod12lg1 to mod12lg5 
model headline(event= "1") = sanitation_canteen;
exact sanitation_canteen/estimate;
run;

proc logistic data= v3; # same procedure applied to datasets from v31 to v35 
model headline(event= "1") = forced_labor;
exact forced_labor/estimate;
run;

# Stepwise logistic regression with exact conditional test for factory characteristics from publicly available information

proc logistic data= mod12; # same procedure applied to datasets from mod121 to mod125 
model headline(event= "1") = certified cooperative_approach;
exact certified cooperative_approach/estimate;
run;

# In the next step of data analysis, bootstrap sampling was done to run stepwise logistic regression on bootstrap sample to get the parameter
# estimate distribution of all predictors 

libname hammad 'C:/Users/mhabbasi/Desktop/head16';

# Importing both negative and non-negative factories datasets to sas

proc import datafile="C:\Users\mhabbasi\Desktop\head16\bootsnonneg.xlsx"  dbms = xlsx out=bootsnonneg;
run;
proc import datafile="C:\Users\mhabbasi\Desktop\head16\bootsneg.xlsx"    dbms = xlsx out=bootsneg;
run;

# bootstrapping to get 1000 samples, each sample contains almost 32 random non-negative factories observation */ 

%let NumSamples = 1000;       /* number of bootstrap resamples */

# to generate many bootstrap samples

proc surveyselect data=bootsnonneg NOPRINT seed=1
     out=BootSSFreq(rename=(Replicate=SampleID))
	 seed=2
     method=urs              # re-sample with replacement 
    samprate=0.1893492       # each bootstrap sample has N observations
     /* OUTHITS              # option to suppress the frequency var 
	/*n = 32*/
     reps=&NumSamples;       # generate NumSamples bootstrap re-samples
run;

# Adding negative headlines to each bootstrap sample that contains 32 random non-negative factories 
%macro AddingNegativeHeadlines(NumSamples=);


proc sql;                     # Count before adding negative headlines 
	select count(*)
	from Bootssfreq;
run;

%do i=1 %to &NumSamples;

data negative;
	set bootsneg;
	SampleID = &i;
run;

data Bootssfreq;
	set Bootssfreq negative;
run;

%end;

proc sort data=Bootssfreq;
 by sampleId fac_id;
run;


proc sql;                                # Count after adding negative headlines
	select count(*)
	from Bootssfreq;
run;

%mend;

%AddingNegativeHeadlines(NumSamples=&NumSamples);

# logistic regression to get the estimates of 1000 bootstrap samples containing almost 32 non negative and 16 negative factories for version 1 and 2 
# survey categories

options nonotes;
proc logistic data=Bootssfreq noprint outest=PE;
   by SampleId;
   model headline(Event='1') = fire_exits -- recommendation;
run;
options notes;

data sample;
set pe;
run;

# compute value of the means and median of version 1 and 2 categories on 1000 bootstrap samples*/

proc means data=sample;  var fire_exits -- recommendation;
output out= med_var median(fire_exits)= fire_exits median(feedback)= feedback median(long_hours)= long_hours median(abuse)= abuse 
median(child_labor)= child_labor median(wages)= wages median(sanitation_canteen)= sanitation_canteen median(sanitation_toilet)= sanitation_toilet 
median(recommendation)= recommendation; run;  

# Frequency distribution of estimates on each version 1 and version 2 category 

title "Bootstrap Distribution For Estimates";
%let fire_exits = 7.966;
proc sgplot data=sample;
histogram fire_exits/dataskin= matte;
refline &fire_exits/ axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let feedback = -42.578;
proc sgplot data=sample;
   histogram feedback/dataskin= matte;
 refline &feedback/ axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let long_hours = 0.265;
proc sgplot data=sample;
   histogram long_hours/dataskin= matte;
 refline &long_hours / axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let abuse = 7.977;
proc sgplot data=sample;
   histogram abuse/dataskin= matte;
 refline &abuse / axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let child_labor = -16.816;
proc sgplot data=sample;
   histogram child_labor/dataskin= matte;
 refline &child_labor / axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let wages = -34.773;
proc sgplot data=sample;
   histogram wages/dataskin= matte;
 refline &wages / axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let sanitation_canteen = -6.746;
proc sgplot data=sample;
   histogram sanitation_canteen/dataskin= matte;
 refline &sanitation_canteen / axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let sanitation_toilet = 1.698;
proc sgplot data=sample;
   histogram sanitation_toilet/dataskin= matte;
 refline &sanitation_toilet / axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let recommendation = -4.375;
proc sgplot data=sample;
   histogram recommendation/dataskin= matte;
 refline &recommendation / axis=x lineattrs=(color=red);
run;

# logistic regression to get the estimates of 1000 bootstrap samples containing almost 32 non negative and 16 negative factories on publicly 
# available factory predictors

options nonotes;
proc logistic data=Bootssfreq noprint outest=PE2;
   by SampleId;
   model headline(Event='1') = buyer_supplier_relationship -- cooperative_approach;
run;
options notes;

data sample2;
set pe2;
run;

# compute value of the means and median of publicly available factory predictors on 1000 bootstrap samples

proc means data=sample2;  var buyer_supplier_relationship -- cooperative_approach;
output out= med_var2 median(buyer_supplier_relationship)= median(buyer_supplier_relationship)= buyer_supplier_relationship 
median(certified)= certified median(Cooperative_approach)= Cooperative_approach; run; 

# Frequency distribution of estimates on publicly available factory predictors 

title "Bootstrap Distribution For Estimates";
%let buyer_supplier_relationship = 1.914;
proc sgplot data=sample2;
   histogram buyer_supplier_relationship/dataskin= matte;
 refline &buyer_supplier_relationship/ axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let certified = 12.30;
proc sgplot data=sample2;
   histogram certified/dataskin= matte;
 refline &certified/ axis=x lineattrs=(color=red);
run;

title "Bootstrap Distribution For Estimates";
%let cooperative_approach = -0.273;
proc sgplot data=sample2;
   histogram cooperative_approach/dataskin= matte;
 refline &cooperative_approach / axis=x lineattrs=(color=red);
run;

Proc sort data= sample2  nodup out= hammad.bootlogi2;
by sampleid;
run;

proc export data=hammad.bootlogi2 outfile="C:\Users\mhabbasi\Desktop\head16\bootlogi2.xls"
dbms=xls replace;
run;

      



        

        


