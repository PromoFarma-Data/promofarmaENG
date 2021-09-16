To use the Snowflake and S3ToSnowflake operators in Airflow, their files and pf_utils.db must be placed in a folder
that is added to the python PATH. For example, we have placed these operators in the plugins folder.   

Also, please note that these operators contain default values which are useful just for us 
(we haven't taken them out so that you have an example), and there are some features from the official operators 
which we have removed for simplicity's sake (we don't need them). 
