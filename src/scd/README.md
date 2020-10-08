# Implementing ETL into a Slowly Changing Dimension Using Spark

## Purpose
The purpose of this code is to prototype the implemenation of using PySpark to insert new data into a star schema that utilizes a slowly changing dimension. From this code, we hope to learn and address the obstacles of this ETL pattern.

## Setup
The initial setup of this case contains a very small sample slowly changing dimension data set in its initial state:

```
+---+---------+----------+----------+--------------------------+--------------------------+
|id |attribute|is_current|is_deleted|active_date               |inactive_date             |
+---+---------+----------+----------+--------------------------+--------------------------+
|1  |blue     |false     |false     |2020-10-05 08:15:27.24386 |2020-10-05 10:22:31.938404|
|1  |green    |true      |false     |2020-10-05 10:22:31.938404|3001-01-01 00:00:00       |
|2  |red      |true      |false     |2020-10-05 08:15:27.314486|3001-01-01 00:00:00       |
|3  |orange   |true      |false     |2020-10-05 08:15:27.440021|3001-01-01 00:00:00       |
+---+---------+----------+----------+--------------------------+--------------------------+
```

This consits of three items: 1,2, and 3 and an attribute. An item can be 'current' or 'deleted'. The state of an attribute is denoted by a color. The attribute for item 1 has previously changed states from 'blue' to 'green' at 2020-10-05 10:22:31.938404. 

For this exercise, the slowly changing dimension is represented as two columns: an active date that represents when the atrribute transitioned to the state, and the inactive date when the attribute transitioned out of the state. If the attribute is still in that state, the state's inactive date is represented by '3001-01-01 00:00:00' rather than a null. This will be refered to as the 'high time'.

This dimension also consists of an 'is current' boolean, redundantly representing whether or not this state is the current state for the item, and a 'is deleted' boolean, representing a deleted state for the item.
 
 
## State Change
The following state changes arrive and need to be batch processed into the initial data set:

```
+------------+-------------------+
|new_state_id|new_state_attribute|
+------------+-------------------+
|1           |green              |
|2           |black              |
|4           |yellow             |
+------------+-------------------+
```

item 1 new state is 'green' - which it is already in, item 2 transitions to 'black', and new item 4 arrives in state 'yellow'. There is no record of item 3 in this batch state change, for this prototype it is assumed that means the item transitions to 'deleted'.

## Expected Output
The expected output upon batch processing the state changes on the initial slowly changing dimension:

```
+---+---------+----------+----------+--------------------------+--------------------------+
|id |attribute|is_current|is_deleted|active_date               |inactive_date             |
+---+---------+----------+----------+--------------------------+--------------------------+
|1  |blue     |false     |false     |2020-10-05 08:15:27.24386 |2020-10-05 10:22:31.938404|
|1  |green    |true      |false     |2020-10-05 10:22:31.938404|3001-01-01 00:00:00       |
|2  |red      |false     |false     |2020-10-05 08:15:27.314486|2020-10-08 11:02:16.491847|
|2  |black    |true      |false     |2020-10-08 11:02:16.491847|3001-01-01 00:00:00       |
|3  |orange   |false     |true      |2020-10-05 08:15:27.440021|2020-10-08 11:02:16.491847|
|4  |yellow   |true      |false     |2020-10-08 11:02:16.491847|3001-01-01 00:00:00       |
+---+---------+----------+----------+--------------------------+--------------------------+
```

No change to item 1, despite the input which matches its current state. Item 2 transtions to 'black' at batch processing time, item 3 transitions to a deleted state, and new item 4 gets added with an initial state of 'yellow' with an active date of batch processing time.

## Implementation

The first step is cross joining the current time and the high time to the incoming state change data. Of course, in reality, the state change time may already be part of the incoming state change data. If that is the case, the cross join is not needed, and instead we use the change time provided.

```
+------------+-------------------+--------------------------+-----------------------+
|new_state_id|new_state_attribute|new_state_active_date     |new_state_inactive_date|
+------------+-------------------+--------------------------+-----------------------+
|1           |green              |2020-10-08 12:39:00.911139|3001-01-01 00:00:00    |
|2           |black              |2020-10-08 12:39:00.911139|3001-01-01 00:00:00    |
|4           |yellow             |2020-10-08 12:39:00.911139|3001-01-01 00:00:00    |
+------------+-------------------+--------------------------+-----------------------+
```

Next, we perform a full outer join on the current (initial) state and the state changes, with the join ids being the item id and inactive date:

```
+----+---------+----------+----------+--------------------------+--------------------------+------------+-------------------+--------------------------+-----------------------+
|id  |attribute|is_current|is_deleted|active_date               |inactive_date             |new_state_id|new_state_attribute|new_state_active_date     |new_state_inactive_date|
+----+---------+----------+----------+--------------------------+--------------------------+------------+-------------------+--------------------------+-----------------------+
|1   |blue     |false     |false     |2020-10-05 08:15:27.24386 |2020-10-05 10:22:31.938404|null        |null               |null                      |null                   |
|1   |green    |true      |false     |2020-10-05 10:22:31.938404|3001-01-01 00:00:00       |1           |green              |2020-10-08 12:39:00.911139|3001-01-01 00:00:00    |
|3   |orange   |true      |false     |2020-10-05 08:15:27.440021|3001-01-01 00:00:00       |null        |null               |null                      |null                   |
|null|null     |null      |null      |null                      |null                      |4           |yellow             |2020-10-08 12:39:00.911139|3001-01-01 00:00:00    |
|2   |red      |true      |false     |2020-10-05 08:15:27.314486|3001-01-01 00:00:00       |2           |black              |2020-10-08 12:39:00.911139|3001-01-01 00:00:00    |
+----+---------+----------+----------+--------------------------+--------------------------+------------+-------------------+--------------------------+-----------------------+
```

Logic applied to both sides of the join results indicates how each row in the joined data set needs to be processed. This is represented by the 'action' column:

```
+----+---------+----------+----------+--------------------------+--------------------------+------------+-------------------+--------------------------+-----------------------+--------+
|id  |attribute|is_current|is_deleted|active_date               |inactive_date             |new_state_id|new_state_attribute|new_state_active_date     |new_state_inactive_date|action  |
+----+---------+----------+----------+--------------------------+--------------------------+------------+-------------------+--------------------------+-----------------------+--------+
|1   |blue     |false     |false     |2020-10-05 08:15:27.24386 |2020-10-05 10:22:31.938404|null        |null               |null                      |null                   |NOACTION|
|1   |green    |true      |false     |2020-10-05 10:22:31.938404|3001-01-01 00:00:00       |1           |green              |2020-10-08 12:39:00.911139|3001-01-01 00:00:00    |NOACTION|
|3   |orange   |true      |false     |2020-10-05 08:15:27.440021|3001-01-01 00:00:00       |null        |null               |null                      |null                   |DELETE  |
|null|null     |null      |null      |null                      |null                      |4           |yellow             |2020-10-08 12:39:00.911139|3001-01-01 00:00:00    |INSERT  |
|2   |red      |true      |false     |2020-10-05 08:15:27.314486|3001-01-01 00:00:00       |2           |black              |2020-10-08 12:39:00.911139|3001-01-01 00:00:00    |UPSERT  |
+----+---------+----------+----------+--------------------------+--------------------------+------------+-------------------+--------------------------+-----------------------+--------+ 
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


## Conclusions
 

