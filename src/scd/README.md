# Implementing ETL into a Slowly Changing Dimension Using Spark

## Purpose
The purpose of this code is to prototype the implemenation of using PySpark to insert new data into a star schema that utilizes a slowly changing dimension. From this code, we hope to learn and address the obstacles of this ETL pattern.

## Setup
The initial setup of this case contains a very small sample slowly changing dimension data set in its initial state:

```
+-------+--------------------+----------+----------+--------------------------+--------------------------+
|user_id|address             |is_current|is_deleted|active_date               |inactive_date             |
+-------+--------------------+----------+----------+--------------------------+--------------------------+
|1      |123 Anywhere Street |false     |false     |2020-10-05 08:15:27.24386 |2020-10-05 10:22:31.938404|
|1      |999 Someother Street|true      |false     |2020-10-05 10:22:31.938404|3001-01-01 00:00:00       |
|2      |1000 Spark Street   |true      |false     |2020-10-05 08:15:27.314486|3001-01-01 00:00:00       |
|3      |1060 W Addison      |true      |false     |2020-10-05 08:15:27.440021|3001-01-01 00:00:00       |
+-------+--------------------+----------+----------+--------------------------+--------------------------+

```

This consists of three user ids: 1,2, and 3...each with an address. The address is treated as a slowly changing dimension - it can change over time, and we want to preserve history. For example, user id 1 lived at 123 Anywhere Street from 2020-10-05 08:15:27.24386 to 2020-10-05 10:22:31.938404. At that time, the user moved to 999 Someother Street, where he currently resides. 

For this exercise, the slowly changing dimension is represented as two columns: an active date that represents when the address was active, and the inactive date when the address was no longer valid. If the address is currently valid, the inactive date is represented by '3001-01-01 00:00:00' rather than a null. '3001-01-01 00:00:00' will be refered to as the 'high time'.

This implementation also consists of an 'is current' boolean, redundantly representing whether or not this address is the current address for the user, and a 'is deleted' boolean, representing if this address has been deleted.
 
 
## Address Updates
The following address changes arrive and need to be batch processed into the initial data set:

```
+-----------------+---------------------+
|user_id          |address              |
+-----------------+---------------------+
|1                |999 Someother Street |
|2                |2000 Snowflake Street|
|4                |233 S Wacker         |
+-----------------+---------------------+
```

User 1 has new address '999 Someother Street' - which his address already is, user 2's address changes to '2000 Snowflake Street', and new user id 4 arrives with address '233 S Wacker'. There is no record of user 3's address in this batch state change, for this prototype it is assumed that means the address should transition to 'deleted'.

## Expected Output
The expected output upon batch processing the address changes on the initial slowly changing dimension:

```
+-------+---------------------+----------+----------+--------------------------+--------------------------+
|user_id|address              |is_current|is_deleted|active_date               |inactive_date             |
+-------+---------------------+----------+----------+--------------------------+--------------------------+
|1      |123 Anywhere Street  |false     |false     |2020-10-05 08:15:27.24386 |2020-10-05 10:22:31.938404|
|1      |999 Someother Street |true      |false     |2020-10-05 10:22:31.938404|3001-01-01 00:00:00       |
|2      |1000 Spark Street    |false     |false     |2020-10-05 08:15:27.314486|2020-10-08 16:00:16.150026|
|2      |2000 Snowflake Street|true      |false     |2020-10-08 16:00:16.150026|3001-01-01 00:00:00       |
|3      |1060 W Addison       |false     |true      |2020-10-05 08:15:27.440021|2020-10-08 16:00:16.150026|
|4      |233 S Wacker         |true      |false     |2020-10-08 16:00:16.150026|3001-01-01 00:00:00       |
+-------+---------------------+----------+----------+--------------------------+--------------------------+
```

No change to user 1, despite the input which matches its current address. User 2's address changes to '2000 Snowflake Street' at batch processing time, The address for user id 3 changes to a deleted state, and new user 4 gets added with an initial address of '233 S Wacker' with an active date of batch processing time.

## Implementation

The first step is cross joining the current time and the high time to the incoming address change data. Of course, in reality, the state change time may already be part of the incoming state change data. If that is the case, the cross join is not needed, and instead we use the change time provided.

```
+------------+-------------------+--------------------------+-----------------------+
|new_user_id |new_address        |new_active_date           |new_inactive_date|
+------------+-------------------+--------------------------+-----------------------+
|1           |999 Someother Stree|2020-10-08 16:00:16.150026|3001-01-01 00:00:00    |
|2           |2000 Snowflake Stre|2020-10-08 16:00:16.150026|3001-01-01 00:00:00    |
|4           |233 S Wacker       |2020-10-08 16:00:16.150026|3001-01-01 00:00:00    |
+------------+-------------------+--------------------------+-----------------------+
```

Next, we perform a full outer join on the current (initial) state and the address changes, with the join ids being the user id and inactive date:

```
+-------+--------------------+----------+----------+--------------------------+--------------------------+-----------------+---------------------+--------------------------+-----------------------+
|user_id|address             |is_current|is_deleted|active_date               |inactive_date             |new_user_id      |new_user_address     |new_user_active_date      |new_user_inactive_date |
+-------+--------------------+----------+----------+--------------------------+--------------------------+-----------------+---------------------+--------------------------+-----------------------+
|1      |123 Anywhere Street |false     |false     |2020-10-05 08:15:27.24386 |2020-10-05 10:22:31.938404|null             |null                 |null                      |null                   |
|1      |999 Someother Street|true      |false     |2020-10-05 10:22:31.938404|3001-01-01 00:00:00       |1                |999 Someother Street |2020-10-08 16:00:16.150026|3001-01-01 00:00:00    |
|3      |1060 W Addison      |true      |false     |2020-10-05 08:15:27.440021|3001-01-01 00:00:00       |null             |null                 |null                      |null                   |
|null   |null                |null      |null      |null                      |null                      |4                |233 S Wacker         |2020-10-08 16:00:16.150026|3001-01-01 00:00:00    |
|2      |1000 Spark Street   |true      |false     |2020-10-05 08:15:27.314486|3001-01-01 00:00:00       |2                |2000 Snowflake Street|2020-10-08 16:00:16.150026|3001-01-01 00:00:00    |
+-------+--------------------+----------+----------+--------------------------+--------------------------+-----------------+---------------------+--------------------------+-----------------------|

```

Logic applied to both sides of the join results indicates how each row in the joined data set needs to be processed. This is represented by the 'action' column:

```
+-------+--------------------+----------+----------+--------------------------+--------------------------+-----------------+---------------------+--------------------------+-----------------------+--------+
|user_id|address             |is_current|is_deleted|active_date               |inactive_date             |new_user_id      |new_user_address     |new_user_active_date      |new_user_inactive_date |action  |
+-------+--------------------+----------+----------+--------------------------+--------------------------+-----------------+---------------------+--------------------------+-----------------------+--------+
|1      |123 Anywhere Street |false     |false     |2020-10-05 08:15:27.24386 |2020-10-05 10:22:31.938404|null             |null                 |null                      |null                   |NOACTION|
|1      |999 Someother Street|true      |false     |2020-10-05 10:22:31.938404|3001-01-01 00:00:00       |1                |999 Someother Street |2020-10-08 16:00:16.150026|3001-01-01 00:00:00    |NOACTION|
|3      |1060 W Addison      |true      |false     |2020-10-05 08:15:27.440021|3001-01-01 00:00:00       |null             |null                 |null                      |null                   |DELETE  |
|null   |null                |null      |null      |null                      |null                      |4                |233 S Wacker         |2020-10-08 16:00:16.150026|3001-01-01 00:00:00    |INSERT  |
|2      |1000 Spark Street   |true      |false     |2020-10-05 08:15:27.314486|3001-01-01 00:00:00       |2                |2000 Snowflake Street|2020-10-08 16:00:16.150026|3001-01-01 00:00:00    |UPSERT  |
+-------+--------------------+----------+----------+--------------------------+--------------------------+-----------------+---------------------+--------------------------+-----------------------+--------+
```

Filtering on that column, we have four seperate cases that need to be processed:

### NOACTION
No updates needed to occur on these rows, they need to simply get passed on to the target data set

### INSERT
These are new rows that need to be added to the target data set, with the data to be added originating from the 'new' side of the full outer join.

### UPSERT
These rows need to spawn two rows in the target data set: 
    1. from the existing side of the full outer join, include all columns, with the inactive_date set to the batch processing time
    1. from the new side of the full outer join, include all columns, with the state inactive time set to the high time
    
### DELETE
These rows need to be added to the target data set from the existing side of the full outer join, with the is_deleted flag set to 'True' and the state inactive date set to the current time.


Finally, the four above resulting data sets get unioned together into a single data frame, which can be written to a table using the [Spark Dataframe write jdbc method](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.jdbc). Examples of Azure JDBC connectivity can be found [here](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/sql-databases). The delta lake documentation can be found [here](https://docs.delta.io/0.4.0/api/python/index.html)

## Conclusions
 

