import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col, month, concat_ws,sum,row_number,avg,round,lit,when,concat
from graphframes import *
from pyspark.sql.types import IntegerType, Row
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
# import matplotlib.plot 

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
#--------------------------------------------------------------------Task 1 --------------------------------------------------

    #Read the CSV files 
    rideshare_data_df = spark.read.option("delimiter", ",").option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv")
    taxi_zone_lookup_df = spark.read.option("delimiter",",").option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv")

    #Show both the dataframes
    rideshare_data_df.show()
    taxi_zone_lookup_df.show()
    

    #Perform Join for rideshare and taxi zone on the basis where pickup location ID is equal to taxizone location ID
    merged_df = rideshare_data_df.join(taxi_zone_lookup_df, [rideshare_data_df["pickup_location"] == taxi_zone_lookup_df["LocationID"]]) \
                                        .drop(taxi_zone_lookup_df["LocationID"]) \
                                        .withColumnRenamed("Borough", "Pickup_Borough") \
                                        .withColumnRenamed("Zone", "Pickup_Zone") \
                                        .withColumnRenamed("service_zone", "Pickup_service_zone")

    #Perform Join for rideshare and taxi zone on the basis where dropoff location ID is equal to taxizone location ID
    
    merged_df = merged_df.join(taxi_zone_lookup_df, [merged_df["dropoff_location"] == taxi_zone_lookup_df["LocationID"]])\
                                     .drop(taxi_zone_lookup_df["LocationID"]) \
                                     .withColumnRenamed("Borough", "Dropoff_Borough") \
                                     .withColumnRenamed("Zone", "Dropoff_Zone") \
                                     .withColumnRenamed("service_zone", "Dropoff_service_zone")
 
    #Convert the unitime to datetime format using from_unixtime API
    merged_df = merged_df.withColumn('date', from_unixtime(merged_df['date'], "yyyy-MM-dd"))

    #Print the schema of the new dataframe after performing the join operation
    merged_df.printSchema()

    #Get the total row count using count API
    row_count = merged_df.count()
    print('------------------------row count-----------------------',row_count)

    #-----------------------------------------------------Task 2 Solution--------------------------------------------------
    #                                                     Solution 1
    #Create a new df with month column to perform groupby
    new_df = merged_df.withColumn('month', month(merged_df['date']))

    # new_df.printSchema()

    #Cast the columns to float datatype on which we will be performing mathematical operations
    new_df = new_df.withColumn("driver_total_pay",col("driver_total_pay").cast('float'))

    new_df = new_df.withColumn("rideshare_profit",col("rideshare_profit").cast('float'))

    new_df = new_df.withColumn("trip_length",col("trip_length").cast('float'))

    new_df = new_df.withColumn("request_to_pickup",col("request_to_pickup").cast('float'))

    # Take count for the business according to months
    count_df = new_df.groupBy('business','month').count()

    #concat the values of business column with month values to get uber-1 etc
    count_df=count_df.select(concat_ws('-',count_df.business,count_df.month).alias('business'),'count')

    #Display the counts
    count_df.orderBy('business').show()

    #Write the data to csv file
    count_df.coalesce(1).write.option("header",True).option("delimiter",",").csv("s3a://" + s3_bucket + "/trip_counts1.csv")

    #                                                     #Solution 2

    #Perform groupby and sum to get the monthvise profit for each business
    profit_df = new_df.groupBy('business', 'month').sum('rideshare_profit')

    #concat the values of business column with month values to get uber-1 etc
    profit_df = profit_df.select(concat_ws('-',profit_df['business'],profit_df['month']).alias('business'),profit_df['sum(rideshare_profit)'].alias('profit_sum'))

    #Display Month vise profits
    profit_df.orderBy('business').show()

    #Write the data to csv file
    profit_df.coalesce(1).write.option("header",True).option("delimiter",",").csv("s3a://" + s3_bucket + "/profits.csv")

    #                                                     #Solution 3

    #Perform groupby and sum to get the monthvise driver_pay for each business
    driver_df = new_df.groupBy('business', 'month').sum('driver_total_pay')

    #concat the values of business column with month values to get uber-1 etc
    driver_df = driver_df.select(concat_ws('-',driver_df['business'],driver_df['month']).alias('business'),driver_df['sum(driver_total_pay)'].alias('driver_pay_sum'))

    #Wrtie the data to csv file
    driver_df.coalesce(1).write.option("header",True).option("delimiter",",").csv("s3a://" + s3_bucket + "/driver_pay.csv")

    #Display the monthvise driver pay
    driver_df.orderBy('business').show()

    # ------------------------------------------------------TASK 3 Solution------------------------------------------------

                                                            # Solution 1

    # Group by 'Pickup_Borough' and 'month' and count the number of trips for each combination of borough and month
    # This gives us the count of trips originating from each borough for each month
    top_borough_df = new_df.groupBy('Pickup_Borough','month').count()

    #Group the resulting DataFrame by 'Pickup_Borough' and 'month' again and sum up the counts obtained in the previous step
    top_borough_df = top_borough_df.groupBy('Pickup_Borough','month').sum('count')

    #Sort the DataFrame by the sum of counts in descending order
    top_borough_df = top_borough_df.sort('sum(count)', ascending = False)
    
    # Define a window specification for partitioning the data by 'month' and ordering by the sum of counts in descending order
    windowDept = Window.partitionBy('month').orderBy(col("sum(count)").desc())

    # Add a new column 'row' to the DataFrame that represents the row number of each record within its partition (month), ordered by the sum of counts
    # And apply filter to get the top 5 records of each partition this will give us top 5 records of each month
    top_borough_df = top_borough_df.withColumn("row",row_number().over(windowDept)).filter(col('row')<=5)

    # Remove the 'row' column from the DataFrame
    # Rename the 'sum(count)' column to 'trip_count'
    top_borough_df = top_borough_df.drop('row')\
                                   .withColumnRenamed('sum(count)','trip_count')

    # Display the DataFrame ordered by 'month' and 'trip_count' in descending order
    top_borough_df.orderBy('month','trip_count',ascending=False).show()

                                                                # Solution 2
    # Similar to Solution 1, but this time for 'Dropoff_Borough'
    # Instead of counting trips originating from each borough, 
    # this counts trips ending in each borough
    # The rest of the steps are the same as Solution 1
    top_drop_borough_df = new_df.groupBy('Dropoff_Borough','month').count()

    top_drop_borough_df = top_drop_borough_df.groupBy('Dropoff_Borough','month').sum('count')

    top_drop_borough_df = top_drop_borough_df.sort('sum(count)', ascending = False)
    
    windowDept = Window.partitionBy('month').orderBy(col("sum(count)").desc())

    top_drop_borough_df = top_drop_borough_df.withColumn("row",row_number().over(windowDept)).filter(col('row')<=5)

    top_drop_borough_df = top_drop_borough_df.drop('row')\
                                             .withColumnRenamed('sum(count)','trip_count')

    top_drop_borough_df.orderBy('month','trip_count',ascending=False).show()

                                                                # Solution 3
    # Convert the column 'driver_total_pay' to float type
    new_df = new_df.withColumn("driver_total_pay",col("driver_total_pay").cast('float'))

    # Group by 'Pickup_Borough' and 'Dropoff_Borough' and sum up the 'driver_total_pay' for each route
    top_routes_df = new_df.groupBy('Pickup_Borough','Dropoff_Borough').sum('driver_total_pay')

    # Sort the DataFrame by the sum of 'driver_total_pay' in descending order
    top_routes_df = top_routes_df.sort('sum(driver_total_pay)',ascending = False)

    # Rename the column 'sum(driver_total_pay)' to 'total_profit'
    top_routes_df = top_routes_df.withColumnRenamed('sum(driver_total_pay)','total_profit')

     # Concatenate 'Pickup_Borough' and 'Dropoff_Borough' to form the 'Route' column
    top_routes_df = top_routes_df.select(concat_ws(' to ',top_routes_df['Pickup_Borough'],top_routes_df['Dropoff_Borough']).alias('Route'),top_routes_df['total_profit'])

    # Display the top 30 routes along with their total profit
    top_routes_df.show(30,truncate=False)

    #----------------------------------------------------Task 4 Solution-----------------------------------------------------

                                                         #Solution 1
    #Group by 'time_of_day' and calculate the average 'driver_total_pay' for each time of the day
    average_pay_df = new_df.groupBy('time_of_day').avg('driver_total_pay')

    # Select the columns 'time_of_day' and the calculated average 'driver_total_pay'
    # Rename the column containing the average to 'average_driver_total_pay'
    average_pay_df = average_pay_df.select('time_of_day',average_pay_df['avg(driver_total_pay)'].alias('average_driver_total_pay'))

    # Display the DataFrame showing the average driver total pay for each time of the day
    average_pay_df.show()

                                                        #Solution 2

    # Group by 'time_of_day' and calculate the average 'trip_length' for each time of the day
    average_trip_length = new_df.groupBy('time_of_day').avg('trip_length')

    # Select the columns 'time_of_day' and the calculated average 'trip_length'
    # Rename the column containing the average to 'average_trip_length'
    average_trip_length = average_trip_length.select('time_of_day',average_trip_length['avg(trip_length)'].alias('average_trip_length'))

    # Display the DataFrame showing the average trip length for each time of the day
    average_trip_length.show()

                                                        #Solution 3

    # Rename the 'time_of_day' column of average_trip_length to 'trip_time_of_day'
    average_trip_length = average_trip_length.withColumnRenamed('time_of_day', 'trip_time_of_day')

    # Join average_pay_df with average_trip_length on the 'time_of_day' column
    average_earning_df = average_pay_df.join(average_trip_length,average_pay_df['time_of_day'] == average_trip_length['trip_time_of_day'])

    # Convert the columns 'average_driver_total_pay' and 'average_trip_length' to float type
    average_earning_df = average_earning_df.withColumn("average_driver_total_pay",col("average_driver_total_pay").cast('float'))\
                                           .withColumn("average_trip_length",col("average_trip_length").cast('float'))

    # Calculate the 'average_earning_per_mile' by dividing 'average_driver_total_pay' by 'average_trip_length'
    average_earning_df = average_earning_df.withColumn('average_earning_per_mile', ( average_earning_df['average_driver_total_pay']) / average_earning_df['average_trip_length'] )

    # Select the columns 'time_of_day' and 'average_earning_per_mile'
    # Display the DataFrame showing the average earning per mile for each time of the day
    average_earning_df = average_earning_df.select(average_earning_df['time_of_day'], average_earning_df['average_earning_per_mile'])

    average_earning_df.show()

    # --------------------------------------------------Task 5 solution---------------------------------------------------
    # Filter the DataFrame to select only the data for the month of January (month == 1)
    wait_time_df = new_df.filter(new_df.month==1)

    # Select the columns 'month', 'request_to_pickup', and 'date' from the filtered DataFrame
    wait_time_df = wait_time_df.select('month','request_to_pickup','date')

    # Group the DataFrame by 'date' and calculate the average 'request_to_pickup' for each date
    wait_time_df = wait_time_df.groupBy('date').avg('request_to_pickup')

    # Rename the column containing the average to 'request_to_pickup'
    wait_time_df = wait_time_df.withColumnRenamed('avg(request_to_pickup)','request_to_pickup')

    # Write the DataFrame to a CSV file with a single partition, enabling header and specifying the delimiter
    # wait_time_df.coalesce(1).write.option("header",True).option("delimiter",",").csv("s3a://" + s3_bucket + "/wait_time.csv")

    # Display the DataFrame showing the average request-to-pickup time for each date, ordered by 'request_to_pickup' in descending order
    wait_time_df.orderBy('request_to_pickup', ascending = False).show()

    # ---------------------------------------------------Task 6 Solution---------------------------------------------------
                                                        # Solution 1

    # Group the DataFrame by 'Pickup_Borough' and 'time_of_day', and count the number of trips for each group  
    pickup_trip_count = new_df.groupBy('Pickup_Borough','time_of_day').count()

    # Filter the DataFrame to select only those groups where the trip count is less than or equal to 1000
    pickup_trip_count = pickup_trip_count.filter(col('count')<=1000)

    # Rename the count column to 'trip_count'
    pickup_trip_count = pickup_trip_count.withColumnRenamed('count','trip_count')

    # Display the DataFrame showing the trip counts for each pickup borough and time of day
    pickup_trip_count.show()

                                                        # Solution 2

    # Group the DataFrame by 'Pickup_Borough' and 'time_of_day', and count the number of trips for each group
    evening_trip_count = new_df.groupBy('Pickup_Borough','time_of_day').count()

    # Filter the DataFrame to select only those trips that occurred in the evening (time_of_day == 'evening')
    evening_trip_count = evening_trip_count.filter(col('time_of_day')=='evening')

    # Rename the count column to 'trip_count'
    evening_trip_count = evening_trip_count.withColumnRenamed('count','trip_count')

    # Display the DataFrame showing the trip counts for each pickup borough in the evening
    evening_trip_count.show()

                                                        # Solution 3

    # Select the columns 'Pickup_Borough', 'Dropoff_Borough', and 'Pickup_Zone' from the DataFrame
    pickup_zone_df = new_df.select('Pickup_Borough','Dropoff_Borough','Pickup_Zone')

    # Filter the DataFrame to select only those rows where the pickup borough is 'Brooklyn' and the dropoff borough is 'Staten Island'
    pickup_zone_df = pickup_zone_df.filter((col('Pickup_Borough')=='Brooklyn') & (col('Dropoff_Borough')=='Staten Island'))

    #Count the number of rows in the filtered DataFrame
    zone_row_count = pickup_zone_df.count()

    # Display the first 10 rows of the filtered DataFrame without truncating the output
    pickup_zone_df.show(10, truncate=False)

    # Print the number of trips for the given pickup and dropoff boroughs
    print('Number of trip for the given pickup and dropoff bourough =',zone_row_count)

    # ---------------------------------------------------Task 7 Solution---------------------------------------------------                                                    

    # Create the 'Route' column by concatenating 'Pickup_Zone' and 'Dropoff_Zone'
    temp_df = new_df.withColumn('Route', concat(col('Pickup_Zone'), lit(' to '), col('Dropoff_Zone')))

    # Group by 'Route' and 'business', then aggregate the counts
    grouped_df = temp_df.groupBy('Route', 'business').count()

    # Pivot the DataFrame to get separate columns for Uber and Lyft counts
    pivoted_df = grouped_df.groupBy('Route').pivot('business', ['Uber', 'Lyft']).sum('count')

    # Fill null values with 0
    pivoted_df = pivoted_df.fillna(0)
    
    # Rename the uber and lyft to uber_count and lyft_count
    pivoted_df = pivoted_df.withColumnRenamed('Uber','uber_count').withColumnRenamed('Lyft','lyft_count')

    # Calculate the total count for each route
    pivoted_df = pivoted_df.withColumn('total_count', pivoted_df['uber_count'] + pivoted_df['lyft_count'])

    # Sort the DataFrame by total count in descending order and select the top 10 routes
    top_routes_df = pivoted_df.orderBy(col('total_count').desc()).limit(10)

    # Show the top 10 popular routes
    top_routes_df.show(truncate = False)
    
    spark.stop()