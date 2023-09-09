# Bootcamp_Hackathon
Hackathon Project Team Achievers

Steps1:
Making datasources:
-I have build 4 code, 2 code [click_conversion_code,click_event_code] for real-time stream which will be integrated in kafka producer and another 2 [campaigns_code,user_demographic_code]
for dimension table

Step2:
-Integrating the random real stream code[click_conversion_code,click_event_code] in kafka producer to act like a real scenario.
-Made two prouducers and two consumers with two topics[i.e. ad_click, ad_conversion] 
-One producer and one consumer which has subscribed to ad_click will run
-One producer and one consumer which has subscribed to ad_conversion will run
-In consumer code only I have integrated the dimesnion table so that on the fly the we have all the event details on based of ad_id and user_id.
-Paralley they will run and dump data in Nosql database i.e. cassandra from which we can do CQL analysis and can connect Apache Spark to do complex queries

Step3:
To dump the data first make a connection to database and create the table, so that realtime data could be pushed into the table. DO check the data types initially when creating the tables.

Step4:
Start analysising the data.

![architecture](https://github.com/rohit98batra/Bootcamp_Hackathon/assets/66216743/f03d2358-300c-4671-9415-554f2e8cf25b)
