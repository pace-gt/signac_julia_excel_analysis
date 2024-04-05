"""Initialize signac statepoints."""

import os
import numpy as np
import signac

# *******************************************
# ENTER THE MAIN USER STATEPOINTS (START)
# *******************************************
# Initialize the signac project
signac.init_project()

# Enter the variable 'excel_filename_wo_ext_list': list of strings
# Excel file's relative directory 'src/data'
# Note: There are 40 Excel files, with the '.xlsx' extention
# excel_filename_wo_ext_list = ['excel_file_0', 'excel_file_1', ..., 'excel_file_39']
# NOTE this can be done manually or automatically look up all files in a directory

# you can list manually 
excel_filename_wo_ext_list = [
    'excel_file_0',
    'excel_file_1',
    'excel_file_2',
]
'''
excel_filename_wo_ext_list = [
    'excel_file_0',
    'excel_file_1',
    'excel_file_2',
    'excel_file_3',
    'excel_file_4',
    'excel_file_5',
    'excel_file_6',
    'excel_file_7',
    'excel_file_8',
    'excel_file_9',
    'excel_file_10',
    'excel_file_11',
    'excel_file_12',
    'excel_file_13',
    'excel_file_14',
    'excel_file_15',
    'excel_file_16',
    'excel_file_17',
    'excel_file_18',
    'excel_file_19',
    'excel_file_20',
    ]
'''
'''
# automatically lookup .xlsx files in a directory
excel_directory = 'src/data'
all_files_directories_list = sorted(os.listdir(excel_directory), key=str)
excel_filename_wo_ext_list = []
for f_i in all_files_directories_list:
    filen, fix_ext = os.path.splitext(f_i)
    if fix_ext == '.xlsx':
        excel_filename_wo_ext_list.append(filen)
print(f'all_files_directories_list = {all_files_directories_list}')

print(f'excel_filename_wo_ext_list = {excel_filename_wo_ext_list}')
'''

# Enter the number of replicates desired (replicate_number). 
# This adds scalar noise to the data for each replicate with 
# random values between 0.1 to 1.
# replicate_number = [0, 1, 2, 3, 4]
replicate_number = [0, 1]


# *******************************************
# ENTER THE MAIN USER STATEPOINTS (END)
# *******************************************

# Setup the directories in the current directory
print("os.getcwd() = " +str(os.getcwd()))
pr_root = os.getcwd()
pr = signac.get_project(pr_root)


# Set all the statepoints, which will be used to create separate folders 
# for each combination of state points.
all_statepoints = list()

for excel_filename_wo_ext_i in excel_filename_wo_ext_list:
    for replicate_i in replicate_number:
        statepoint = {
            "excel_filename_wo_ext": excel_filename_wo_ext_i,
            "replicate_number_int": replicate_i,
        }

        all_statepoints.append(statepoint)

# Initiate all statepoint createing the jobs/folders.
for sp in all_statepoints:
    pr.open_job(
        statepoint=sp,
    ).init()
