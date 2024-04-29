'''
                                                            Pipeline

    This is the pipeline script, the one that will get the data from the CSV dataset, transform it and load it in to the MySQL database.

    Author: Zourethe
    Date: January, 29, 2024
'''

# Libraries imports.
import pandas as pd
import os
import time

# Data extractor function definition.
def dataExtractor(datasetPath):
    # Read the dataset.
    dataframe = pd.read_csv(datasetPath)

    # Return the readed data.
    return dataframe

# Data transformer function definition.
def dataTransformer(dataframe):
    # Array of unnecessary columns.
    unnecessary_columns = ['job_category', 'company_size', 'employee_residence', 'salary_currency', 'salary']

    # Removing the unnecessary columns.
    transformedDataframe = dataframe.drop(columns = unnecessary_columns, axis = 1, inplace = True)

    # Setting the dataframe in a descending form.
    transformedDataframe = dataframe.sort_values(by='salary_in_usd', ascending=False)
    return transformedDataframe

# Data loader function definition.
def dataLoader(transformedDataframe):
    for year in transformedDataframe['work_year'].unique():
        # Separating the different years in the dataframe.
        year_transformedDataframe = transformedDataframe[transformedDataframe['work_year'] == year]

        # Defining the files names.
        filename = f'../avg-salary-for-data-science-jobs/env/src/database/year_{year}.csv'

        # Removing the already existing files.
        try:
            os.remove(filename)
            print('Deleted {}'.format(filename))
        except:
            pass

        # Removing the unnecessary 'work_year' column and compiling the dataframe in to a .csv file.
        year_transformedDataframe.drop(columns = 'work_year', axis = 1, inplace = True)
        year_transformedDataframe.to_csv(filename, index=False)

# Testing.
if True:
    dataLoader(dataTransformer(dataExtractor('../avg-salary-for-data-science-jobs/env/src/dataset/jobs_list.csv')))
    