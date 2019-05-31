### **Spark application for batch processing**

**1.** To run application locally use **CustomerBalanceLocalRunner** or **NetProfitByCountryLocalRunner**
classes located in src/test/scala/com/betsson/job

**2.** To run on server use **_run.sh_** script and provide next required parameters:
 - INPUT_PATH
 - OUTPUT_PATH
 - JOB_TYPE (**_"BALANCE"_** for CustomerBalance OR **_"PROFIT"_** for NetProfitByCountry)
 - SPARK_EXECUTOR_INSTANCES
 - SPARK_USER
 - SPARK_HOST
 - SUBMIT_FOLDER
 
**3.** Already processed files can be find in **_processing_result_** folder