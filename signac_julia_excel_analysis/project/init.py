"""Initialize signac statepoints."""

from pathlib import Path

import os
import signac
import shutil
import subprocess

# ┌───────────────────────────────────────────────┐
# │ SET THE PROJECTS DEFAULT DIRECTORY AND PATHS  │
# └───────────────────────────────────────────────┘

# Initialize the signac project
signac.init_project()

# Setup the directories in the current directory
print("os.getcwd() = " +str(os.getcwd()))
pr_root = os.getcwd()
pr = signac.get_project(pr_root)


# ┌───────────────────────────────────┐
# │ Define plmnist statepoints to run │
# └───────────────────────────────────┘

# Enter the variable 'excel_filename_wo_ext_list': list of strings
# Excel file's relative directory 'src/data'
# Note: There are 5 Excel files, with the '.xlsx' extention
# excel_filename_wo_ext_list = ['excel_file_0', ..., 'excel_file_5']
# NOTE this can be done manually or automatically look up all files 
# in a directory

'''
# Creating the Excel file list manually.
excel_filename_wo_ext_list = [
    'excel_file_0',
    'excel_file_1',
    'excel_file_2',
    'excel_file_3',
    'excel_file_4',
]
'''

# You can create the Excel file list automatically by looking up
# all the .xlsx files in a directory. This is done by changing 
# the 'excel_directory' variable's path, where the path is 
# relative to the project directory or the full path.
excel_directory = 'src/data'

all_files_directories_list = sorted(os.listdir(excel_directory), key=str)
excel_filename_wo_ext_list = []
for f_i in all_files_directories_list:
    filen, fix_ext = os.path.splitext(f_i)
    if fix_ext == '.xlsx':
        excel_filename_wo_ext_list.append(filen)
print(f'all_files_directories_list = {all_files_directories_list}')

print(f'excel_filename_wo_ext_list = {excel_filename_wo_ext_list}')


# Enter the number of replicates desired (replicate_number). 
# This adds scalar noise to the data for each replicate with 
# random values between 0.1 to 1.
# replicate_number = [0, 1, 2, 3, 4]
replicate_number = [0, 1, 2]


# ┌────────────────────────────────────────────────────────────────────┐
# │ Create and initiate signac_julia_excel_analysis statepoints        │
# └────────────────────────────────────────────────────────────────────┘

# Set all the statepoints, which will be used to create separate 
#folders for each combination of state points.
all_statepoints = list()

for excel_filename_wo_ext_i in excel_filename_wo_ext_list:
    for replicate_i in replicate_number:
        statepoint = {
            "statepoint_type": "julia_excel_analysis",
            "excel_filename_wo_ext": excel_filename_wo_ext_i,
            "replicate_number_int": replicate_i,
        }

        all_statepoints.append(statepoint)

# Initiate all statepoint createing the jobs/folders.
for sp in all_statepoints:
    pr.open_job(
        statepoint=sp,
    ).init()


# ┌──────────────────────────────────────────────────────────────┐
# │ Delete prior analaysis between multiple job to avoid errors  │
# └──────────────────────────────────────────────────────────────┘

# Delete any analysis files that require analysis outside a single 
# workspace file and reset row, as row does not dynamically recheck 
# for completion status after the task is completed.  
# If any previous replicate averages and std_devs exist delete them, 
 # because they will need recalculated as more state points were added.

main_analysis_dir_path_and_name = "analysis"
try:
    if os.path.isfile(f'{main_analysis_dir_path_and_name}/output_avg_std_of_replicates_txt_filename.txt'):
        os.remove(f'{main_analysis_dir_path_and_name}/output_avg_std_of_replicates_txt_filename.txt')
except:
    print(
        f"No directory named "
        f"'{main_analysis_dir_path_and_name}' exists."
        )

# The 'avg_std_dev_calculated.txt' file are auto-deleted when 
# the 'init.py' file is run.  So if there are errors with this, 
# you can run 'python init.py' and it will reset it, so you can rerun it. 
# This also resets and recalculated the completion status.
try:
    # Delete the 'avg_std_dev_calculated.txt' file
    exec_delete_avg_std_dev_file = subprocess.Popen(
        "rm workspace/*/avg_std_dev_calculated.txt", 
        shell=True, 
        stderr=subprocess.STDOUT
    )
    os.wait4(exec_delete_avg_std_dev_file.pid, os.WSTOPPED)

    # Clean and reset row's completion status
    exec_reset_row_status = subprocess.Popen(
        "row clean --completed && row scan", 
        shell=True, 
        stderr=subprocess.STDOUT
    )
    os.wait4(exec_reset_row_status.pid, os.WSTOPPED)

except:
    print(f"ERROR: Unable to delete the 'avg_std_dev_calculated.txt' file or clean and scan workspace progress.") 