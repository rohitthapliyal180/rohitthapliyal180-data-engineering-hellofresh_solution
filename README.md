# HelloFresh Data Engineering Test Solution:

Thanks HelloFresh for giving me the Data Engineering Test. This Test Solution is able to answer the Task1 and Task2, which basically reads the JSON file from the directory to a dataframe
then filter out the ingredient (beef) to calculate the average time taken by the recipe w.r.t there level of difficulty.

# Installation and setup:

Please follow the solution documentation shared to understand the details steps of installation. We need below softwares installed on our machine to run the solution.
The minumum configuration of machine is 2 Core with 8 GB RAM is required.
1. Spark 3.1.2
2. winutils.exe
3. java 1.8 above
4. python 3.8
5. PyCharm IDE
6. Git bash

# Project Structure and folders:

1. src/data_engineering - Contains the required code

   1.1 main_hellofresh.py - main function and the start point of the solution.
   
   1.2 read_json.py - function to read the json file.
   
   1.3 preprocessing.py - finctions to filter records and time conversions
   
   1.4 calculation.py - functions to filter and calculate the avg cooking time.
2. src/config - Contains the config file
3. Input - Contains input json files
4. output - Contains final output file.
5. logs - log file generated at runtime.
6. unit_test/unit_test_files - exported datafames to csv files from different functions to do unit testing.

# Deliverables:
1. Output file in CSV format.
2. Project upload over Git with pull request.
3. Documentaion of the project.
4. README.md and Config file.

# Limitations and Improvements:
1. Unable to create Hive external tables over the recipe partition files due to system limitation. Working machine was not personal and denied permission to write over HDFS (tmp/hive).
2. Unable to share the executable file of the solution as the installed FireEye antivirus quarantine the generated .exe file and blocked to access. However one can easily generate the .exe file by running "pyinstaller main_hellofresh.py --onefile" on the pycharm terminal and the .exe file can be found inside the dist/ folder. Make sure to run "pip install pyinstaller" before "pyinstaller main_hellofresh.py --onefile" to get the pyinstaller library installed.
3. Didn't get enough time to write unit test function and CI/CD pipeline with ongoing projects. However function output (unit_test/unit_test_files) were generated and can be used to do unit testing by comparing with actual outputs from running functions.

