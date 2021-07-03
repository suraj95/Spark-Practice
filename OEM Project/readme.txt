
This is one of my first real world projects I worked on recently. Basically, I get some live data coming from a Kafka topic with details like table and the changes to be carried out (Insert, Update and Delete). My job is to write a streaming function to actually carry out the Inserts, Updates and Deletes in the respective places to ensure that the data tables in source and destination are in sync. Also, in the destination table, there are two extra columns (insert_data and update_date). For Inserts, simply insert_date is updated. For deletes, the record itself is deleted so it doesn't matter. For Updates, the insert_date is preserved but the update_date is to be modified with the current timestamp.


