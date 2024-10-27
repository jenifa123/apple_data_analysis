# Apple Data Analysis

## Overview 

This project focuses on the analyzing the Iphone data in DataBricks. It includes extract , transform and load (ETL) workflows to analyze various different queries from the dataset.The main highlight of the project is the use of Factory Design Pattern which makes the code well organized and structured. 

## Architecture Diagram

![apple_data_analysis](https://github.com/user-attachments/assets/9113fd89-5245-444b-8ebd-11254867db66)

## Tools and Technologies

- DataBricks
- Pyspark

## Project Structure 

### Input Files

```
- Customer_Updated.csv
- Products_Updated.csv
- Transaction_Updated.csv
```

### reader_factory :

- `DataSource` : Abstract Class that defines the structure to fetch data from various sources 
- `CSVDataSource` : Class to fetch data from CSV files
- `ParquetDataSource` : Class to fetch data from Parquet files
- `ORCDataSource` : Class to fetch data from ORC files
- `AvroDataSource` : Class to fetch data from Avro files
- `DeltaDataSource` : Class to fetch data from Delta tables


### extractor:

- `Extractor` : Abstract Class that defines the structure to extract data
- `AirpodsAfterIphoneExtractor` : Class to extract customer, transaction and products data 
- `ParquetDataSource` : Class to fetch data from Parquet files
- `ORCDataSource` : Class to fetch data from ORC files


### transform:

- `Transformer` : Abstract Class that defines the structure to transform data
- `AirpodsAfterIphone` : Class to find out the customers who bought Airpods after Iphone
- `onlyAirPodsAndIphone` : Class to find out the customers who bought only IPhone and Airpods only
- `AverageTimeDelay` : Class to find out average delay between IPhone and Airpods purchases
- `TopSellingProducts` : Class to find out the top 3 selling products in each category


### loader_factory :

- `DataSink` : Abstract Class that defines the structure to load data to various destinations 
- `LoadToDBFS` : Class to load data to DBFS (DataBricks File System)
- `LoadToDBFSWithPartition` : Class to load data to DBFS (DataBricks File System) with partitioning
- `LoadToDeltaTable` : Class to load data to Delta Table in Databricks

## Workflows:

- `FirstWorkflow`: ETL Pipeline to generate the data for all customers who have bought Airpods just after buying Iphone

  **Steps**:
  ```
  +     Extracts the data from required source using AirpodsAfterIPhoneExtractor
  +     Transform the data using AirpodsAfterIphone
  +     Load the data into required target using AirpodsLoader
  ```

- `SecondWorkflow`: ETL Pipeline to generate the data for all customers who have bought only Airpods and Iphone

  **Steps**:
  ```
  +     Extracts the data from required source using AirpodsAfterIPhoneExtractor
  +     Transform the data using onlyAirPodsAndIphone
  +     Load the data into required target using AirpodsAndIphoneLoader
  ```

- `ThirdWorkflow`: ETL Pipeline to find the avg time delay buying an iphone and buying airpods for each customer
  
   **Steps**:
  ```
  +     Extracts the data from required source using AirpodsAfterIPhoneExtractor
  +     Transform the data using AverageTimeDelay
  +     Load the data into required target using AverageTimeDelayLoader
  ```

- `FourthWorkflow`: ETL Pipeline to find the top 3 selling products in each category by total revenue
  
  **Steps**:
  ```
  +    Extracts the data from required source using AirpodsAfterIPhoneExtractor
  +    Transform the data using TopSellingProducts
  +    Load the data into required target using TopSellingProductsLoader
  ```

- `WorkFlowRunner`: Runner class to run the specified workflow

### loader :

- `Loader` : Abstract Class that defines the structure to load transformed data 
- `AirpodsLoader` : Class to sink the transformed data into specified DBFS path for FirstWorkflow
- `AirpodsAndIphoneLoader` : Class to sink the transformed data into specified Delta Table for SecondWorkflow
- `AverageTimeDelayLoader` : Class to sink the transformed data into specified Delta Table for ThirdWorkflow
- `TopSellingProductsLoader` : Class to sink the transformed data into specified DBFS path for FourthWorkflow

## Steps to Run:

 + Run the required workflow by specifying the workflow name in apple_analysis file
 + After the run, check if the output has been populated in the specified delta table or dbfs path

## Conclusion

The project emphasises on understanding the spark concepts like bucketing, partitioning, joins and building a ETL pipeline using the concepts.
