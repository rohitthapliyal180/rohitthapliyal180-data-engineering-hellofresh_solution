# HelloFresh Data Engineering Test Solution

Thanks HelloFresh for giving me the Data Engineering Test. This Test Solution is able to answer the Task1 and Task2, which basically reads the JSON file from the directory to a dataframe
then filter out the ingredient (beef) to calculate the average time taken by the recipe w.r.t there level of difficulty.

# Installation and setup

Please follow the solution documentation shared to understand the details steps of installation. We need below softwares installed on our machine to run the solution.
The minumum configuration of machine is 2 Core with 8 GB RAM is required.
1. Spark 3.1.2
2. winutils.exe
3. java 1.8 above
4. python 3.8
5. PyCharm IDE
6. Git bash

# Project Structure

1. src/data_engineering - Contains the required code

   1.1 main_hellofresh.py - main function and the start point of the solution.
   
   1.2 read_json.py - function to read the json file.
   
   1.3 preprocessing.py - 
   
   1.4 calulation.py - functions to filter and calulate the avg cooking time.
