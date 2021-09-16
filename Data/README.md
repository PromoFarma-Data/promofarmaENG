For being able to use the snowflake operator and s3 to snowflake operators in Airflow, their files, as well 
as pf_utils.db must be placed in a folder that is added to the python PATH. For example, we have the operators in 
the folder plugins. Please note that the operators contain default values that are useful just for us (we don't take
them out for having them as examples) and there are some features into the official operators that we have removed for
simplicity's shake because we don't need them. 