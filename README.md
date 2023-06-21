## STEDI human balance project 

This project is based on the Udacity AWS/Spark data engineering project. 

We create landing, trusted, and curated tables for three types of data: customer, accelerometer, and step_trainer.

Customer and accelerometer landing table creation were created from S3 data lakes using Athena. The scripts are in `accelerometer_landing.sql` and `customer_landing.sql`, respectively. 

Trusted table creation for each data type was done in glue. The Spark scripts for each glue job are `customer_trusted.py`, `step_trainer_trusted.py`, and `accelerometer_trusted.py`. 

Curated table creation was also done in glue. The Spark scripts for each glue job are `customer_curated.py` and `machine_learning_curated.py`.

### Addressing incorrect serial numbers

Because the serial numbers in the incoming customer data are incorrect, we can't join the customer and step_trainer data directly. Instead, we need to use the step_trainer timestamps to join it with the accelerometer data, which can then be joined with customer data on email. Note that this approach does introduce a potential issue if two devices submit data at the identical timestamp. This is unlikely, but possible. A way around this would be to check that there is exactly one accelerometer data point per joined step_trainer data point, and discard any timestamps where this is not the case. 

To create the step_trainer_trusted data, we join the step_trainer_landing table on the accelerometer_trusted table, which we know only has customers who have accelerometer data and have agreed to share their data with research. We keep the `user` field from the `accelerometer` table, since it could be useful in the future to map to `customers` (since the serialNumber is incorrect).

To create the machine_learning_curated table, we again join the step_trainer_trusted table on the accelerometer_trusted table using timestamps, but now we keep all the columns that will be used for research and drop `user`, since it won't be used in the machine learning pipeline. 
