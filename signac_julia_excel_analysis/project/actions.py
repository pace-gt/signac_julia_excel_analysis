"""Basic example of a signac project reading Excel files and calc dot product"""
# project.py

import argparse
import os
import warnings
import signac
import shutil
import subprocess

import json, datetime
from pathlib import Path
import numpy as np
import pandas as pd

# ┌──────────┐
# │ NOTES    │
# └──────────┘

# Set the walltime, memory, and number of CPUs and GPUs needed
# for each individual job, based on the part/section.
# *******************************************************
# *******************   WARNING   ***********************
# It is recommended to check all HPC submisstions with the
# '--dry-run' (i.e., 'row submit --dry-run') command so you 
# do not make an errors requesting the CPUs, GPUs, and 
# other parameters by its value that many cause more 
# resources to be used than expected, which may result in 
# higher HPC or cloud computing costs! 
# *******************   WARNING   ***********************
# *******************************************************

# ┌───────────────────────────────────────────────┐
# │ SET THE PROJECTS DEFAULT DIRECTORY AND PATHS  │
# └───────────────────────────────────────────────┘

# NOTE: DO NOT CHANGE NAMES UNLESS YOU CHANGE THEM IN THE 'workflow.toml' file also.

# SET THE PROJECTS DEFAULT DIRECTORY
project_directory = f"{os.getcwd()}"
print(f"project_directory = {project_directory}")

# Enter the relative path from the project directory to the 
# directory where the Excel files are
# Excel file's relative directory 'src/data'
directory_path_to_excel_files_str = f"/src/data"


# ******************************************************
# SIGNAC MAIN CODE SECTION (START)
# ******************************************************

# ******************************************************
# CREATE THE INITIAL VARIABLES, WHICH WILL BE STORED IN 
# EACH JOB (START)
# ******************************************************

# ┌────────────────────────────────────────────┐
# │ Part 1 - write the job document parameters │
# └────────────────────────────────────────────┘

def part_1_initial_parameters_command(*jobs):
    """Set the system's job parameters in the json file."""

    for job in jobs:

        # Note: the sp=setpoint variables (from init.py file), doc=user documented variables

        # Print the 'excel_filename_wo_ext' on the job.doc file also
        # Import the excel files and extract the data 
        excel_filename = f'{project_directory}/'\
                        f'{directory_path_to_excel_files_str}/'\
                        f'{job.sp.excel_filename_wo_ext}.xlsx'
        
        print('*********************************************')
        print(f'excel_filename = {excel_filename}\n')
        excel_df = pd.read_excel(excel_filename, "Sheet1")
        print(f'excel_df =\n {excel_df}')
        print('*********************************************')

        # Creating a new json file with user built variables (doc)
        job.doc.value_0_int = list(excel_df.loc[:, 'value_0'])[0]
        job.doc.value_1_int = list(excel_df.loc[:, 'value_1'])[0]
        job.doc.value_2_int = list(excel_df.loc[:, 'value_2'])[0]
        job.doc.value_3_int = list(excel_df.loc[:, 'value_3'])[0]

        job.doc.excel_filename_wo_ext = job.sp.excel_filename_wo_ext
        
        # Print the 'replicate number' on the .doc file also
        job.doc.replicate_number_int = job.sp.replicate_number_int


# ┌──────────────────────────────────────┐
# │ Part 2 - Dot product calculations    │
# └──────────────────────────────────────┘

def part_2_julia_dot_product_calcs_command(*jobs):
    """Run the julia dot product calculations via a bash command."""

    for job in jobs:

        julia_file = f'{project_directory}/{"/src/julia/matrix.jl"}'
        excel_filename_julia = f'{job.doc.excel_filename_wo_ext}.xlsx'
        excel_sheetname_julia = f'Sheet1'

        dot_p = f'calc_dot_product("{project_directory}/{directory_path_to_excel_files_str}/{excel_filename_julia}", ' \
        f'"{excel_sheetname_julia}", "{job.fn("dot_product_output_file.txt")}", "{job.doc.replicate_number_int}") '

        # Run the julia command to get the dot product
        print(f"Running dot product for {job}")
        exec_dot_product = subprocess.Popen(
                f"julia --load '{julia_file}' -e '{dot_p}'",
                shell=True, 
                stderr=subprocess.STDOUT
            )
        os.wait4(exec_dot_product.pid, os.WSTOPPED)

    for job in jobs:
        # Check if file exists and written properly
        if job.isfile(f"{"dot_product_output_file.txt"}"):
        # Check if the dot_product calcs completed properly.
            if job.isfile("dot_product_output_file.txt"):
                with open(job.fn("dot_product_output_file.txt"), "r") as fp:
                    output_line = fp.readlines()
                    for i, line in enumerate(output_line):
                        split_move_line = line.split()
                        if "Dot_Product" in line and len(split_move_line) == 3:
                            if (
                                split_move_line[0] == "Dot_Product"
                                and split_move_line[1] == "Calculations"
                                and split_move_line[2] == "Completed"
                            ):
                                # Print completion file if written correctly
                                exec_make_completion_file = subprocess.Popen(
                                    f"touch {job.fn('dot_product_completed.txt')}",
                                    shell=True, 
                                    stderr=subprocess.STDOUT
                                )
                                os.wait4(exec_make_completion_file.pid, os.WSTOPPED)


# ┌──────────────────────────────────────────┐
# │ Part 3 - Compute avg/std over replicates │
# └──────────────────────────────────────────┘

def part_3_calc_avg_std_dev_command(*jobs):

    # Get the individial averages of the values from each state point,
    # and print the values in each separate folder.    


    # List the output column headers
    output_column_dot_product_input_title = 'excel_filename_wo_ext' 
    output_column_dot_product_avg_title = 'dot_product_avg'
    output_column_dot_product_std_dev_title = 'dot_product_std_dev'  

    # create the lists for avg and std dev calcs
    excel_filename_wo_ext_repilcate_list = []
    dot_product_replicate_list = []
    

    # write the output file before the for loop, so it gets all the 
    # values in the loops
    output_txt_file_header = \
        f"{output_column_dot_product_input_title: <40} " \
        f"{output_column_dot_product_avg_title: <20} " \
        f"{output_column_dot_product_std_dev_title: <20} " \
        f" \n"

    write_file_name_and_path = f'analysis/{"output_avg_std_of_replicates_txt_filename.txt"}' 
    if os.path.isfile(write_file_name_and_path):
        replicate_calc_txt_file = open(write_file_name_and_path, "a")
    else:
        replicate_calc_txt_file = open(write_file_name_and_path, "w")
        replicate_calc_txt_file.write(output_txt_file_header)

    # Loop over all the jobs that have the same "dot_product" (in sort_by="dot_product"). 
    for job in jobs:
        # get the individual values
        output_file = f"{"dot_product_output_file.txt"}"
        with open(job.fn(output_file), "r") as fp:
            output_line = fp.readlines()
            split_output_line = output_line  
            for i, line in enumerate(output_line):
                split_line = line.split() 
                if len(split_line) == 1:
                   excel_filename_wo_ext_repilcate_list.append(job.doc.excel_filename_wo_ext) 
                   dot_product_replicate_list.append(float(split_line[0])) 
                

                elif not (
                    len(split_line) == 3 
                      and split_line[0] == 'Dot_Product' 
                      and split_line[1] == 'Calculations'
                      and split_line[2] == 'Completed'
                    ):
                    raise ValueError("ERROR: The format of the dot_product output files are wrong.")

    # Check that the dot_product are all the same and the aggregate function worked, 
    # grouping all the replicates of dot_product
    for j in range(0, len(excel_filename_wo_ext_repilcate_list)):
        if excel_filename_wo_ext_repilcate_list[0] != excel_filename_wo_ext_repilcate_list[j]:
            raise ValueError(
                "ERROR: The dot_product values are not grouping properly in the aggregate function."
                )
        excel_filename_wo_ext_aggregate = excel_filename_wo_ext_repilcate_list[0] 

    # Calculate the means and standard devs
    print(f'********************')
    print(f'dot_product_aggregate = {excel_filename_wo_ext_aggregate}')
    print(f'********************')
    print(f'********************')
    print(f'dot_product_replicate_list = {dot_product_replicate_list}')

    dot_product_avg = np.mean(dot_product_replicate_list)
    dot_product_avg_std = np.std(dot_product_replicate_list, ddof=1)

   
    replicate_calc_txt_file.write(
        f"{excel_filename_wo_ext_aggregate: <40} "
        f"{dot_product_avg: <20} "
        f"{dot_product_avg_std: <20} "
        f" \n"
    )

    replicate_calc_txt_file.close()


    # Check that the replicate dot_product averages files are written
    for job in jobs:
        output_file = f'../../analysis/{"output_avg_std_of_replicates_txt_filename.txt"}'
        if os.path.isfile(job.fn(output_file)):
            # get the individual values
            with open(job.fn(output_file), "r") as fp:
                output_line = fp.readlines()
                for i, line in enumerate(output_line):
                    split_line = line.split() 
                    if len(split_line) == 3 and str(split_line[0]) == str(job.statepoint.excel_filename_wo_ext):
                    
                        # Print completion file if written correctly
                        exec_make_completion_file = subprocess.Popen(
                            f"touch {job.fn('avg_std_of_replicates_completed.txt')}",
                            shell=True, 
                            stderr=subprocess.STDOUT
                        )
                        os.wait4(exec_make_completion_file.pid, os.WSTOPPED)


# ┌───────────────────────────┐
# │ ROW'S ENDING CODE SECTION │
# └───────────────────────────┘
if __name__ == '__main__':
    # Parse the command line arguments: python action.py --action <ACTION> [DIRECTORIES]
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', required=True)
    parser.add_argument('directories', nargs='+')
    args = parser.parse_args()

    # Open the signac jobs
    project = signac.get_project()
    jobs = [project.open_job(id=directory) for directory in args.directories]

    # Call the action
    globals()[args.action](*jobs)
