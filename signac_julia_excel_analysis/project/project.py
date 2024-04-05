"""Basic example of a signac project reading Excel files and calc dot product"""
# project.py

import os
import numpy as np
import pandas as pd

import flow
from flow import FlowProject, aggregator
from flow.environment import DefaultSlurmEnvironment
import hpc_setup

# ******************************************************
# SIGNAC'S STARTING CODE SECTION (START)
# ******************************************************

class Project(FlowProject):
    """Subclass of FlowProject which provides the attributes and custom methods."""

    def __init__(self):
        super().__init__()

# ******************************************************
# SIGNAC'S STARTING CODE SECTION (END)
# ******************************************************


# ******************************************************
# TYPICAL USER VARIBLES THAT CHANGE (START)
# ******************************************************

# Enter the relative path from the project directory to the 
# directory where the Excel files are
# Excel file's relative directory 'src/data'
directory_path_to_excel_files_str = "src/data"

# file names extensions are added later 
# NOTE: DO NOT CHANGE NAMES AFTER STARTING PROJECT, ONLY AT THE BEGINNING
dot_product_output_filename_str = "dot_product_output_file"
output_avg_std_of_replicates_txt_filename = "output_avg_std_of_replicates_txt_filename"

# Set the walltime, memory, and number of CPUs and GPUs needed
# for each individual job, based on the part/section.
# *******************************************************
# *******************  WARNING  ************************* 
# The "part_X_np_or_ntasks_int" should be 1 for most cases.
# Setting to a higher value will multiply
# the CPUs, GPUs, and other parameters by its value 
# that many cause more resources to be used than expected,
# which may result in higher HPC or cloud computing costs!
# *******************************************************
part_1_np_or_nprocesses = 2
part_1_cpus_per_task = 1
part_1_mem_per_cpu_gb = 4
part_1_gpus_per_task = 0
part_1_walltime_hr = 0.25

part_2_np_or_nprocesses = 1
part_2_cpus_per_task = 2
part_2_mem_per_cpu_gb = 3
part_2_gpus_per_task = 0
part_2_walltime_hr = 0.5

part_3_np_or_nprocesses = 1
part_3_cpus_per_task = 3
part_3_mem_per_cpu_gb = 4
part_3_gpus_per_task = 0
part_3_walltime_hr = 0.75

# ******************************************************
# TYPICAL USER VARIBLES THAT CHANGE (END)
# ******************************************************


# ******************************************************
# SIGNAC MAIN CODE SECTION (START)
# ******************************************************

# ******************************************************
# CREATE THE INITIAL VARIABLES, WHICH WILL BE STORED IN 
# EACH JOB (START)
# ******************************************************

# SET THE PROJECTS DEFAULT DIRECTORY
project_directory = f"{os.getcwd()}"
print(f"project_directory = {project_directory}")


@Project.label
def part_1_initial_parameters_completed(job):
    """Check that the data is generated and in the json files."""
    data_written_bool = False
    if job.isfile(f"{'signac_job_document.json'}"):
        data_written_bool = True

    return data_written_bool


@Project.post(part_1_initial_parameters_completed)
@Project.operation(directives=
    {
        "np": part_1_np_or_nprocesses,
        "cpus-per-task": part_1_cpus_per_task,
        "gpus-per-task": part_1_gpus_per_task,
        "mem-per-cpu": part_1_mem_per_cpu_gb,
        "walltime": part_1_walltime_hr,
    }, with_job=True
)
def part_1_initial_parameters_command(job):
    """Set the system's job parameters in the json file."""
    
    # If any previous replicate averages and std_devs exist delete them, 
    # because they will need recalculated as more state points were added.
    if os.path.isfile(f'../../analysis/{output_avg_std_of_replicates_txt_filename}.txt'):
        os.remove(f'../../analysis/{output_avg_std_of_replicates_txt_filename}.txt')


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

# ******************************************************
# CREATE THE INITIAL VARIABLES, WHICH WILL BE STORED IN 
# EACH JOB (END)
# ******************************************************

# ******************************************************
# FUNCTIONS ARE FOR GETTTING AND AGGREGATING DATA (START)
# ******************************************************

# Replaces runs can be looped together to average as needed
def statepoint_without_replicate(job):
    keys = sorted(tuple(j for j in job.sp.keys() if j not in {"replicate_number_int"}))
    return [(key, job.sp[key]) for key in keys]

# ******************************************************
# FUNCTIONS ARE FOR GETTTING AND AGGREGATING DATA (END)
# ******************************************************


# ******************************************************
# PERFORM THE dot_product CALCULATIONS (START)
# ******************************************************

# check to see if the dot_product calculations started
@Project.label 
def part_2a_dot_product_calcs_started(job):
    """Check to see if the dot_product calculations started."""
    output_started_bool = False
    if job.isfile(f"{dot_product_output_filename_str}.txt"):
        output_started_bool = True

    return output_started_bool


# check to see if the dot_product calculations completed correctly
@Project.label
def part_2b_dot_product_calcs_completed_properly(job):
    """Check if the dot_product calcs completed properly."""
    job_run_properly_bool = False
    output_log_file = f"{dot_product_output_filename_str}.txt"
    if job.isfile(output_log_file):
        with open(job.fn(output_log_file), "r") as fp:
            output_line = fp.readlines()
            for i, line in enumerate(output_line):
                split_move_line = line.split()
                if "Dot_Product" in line and len(split_move_line) == 3:
                    if (
                        split_move_line[0] == "Dot_Product"
                        and split_move_line[1] == "Calculations"
                        and split_move_line[2] == "Completed"
                    ):
                        job_run_properly_bool = True
    else:
        job_run_properly_bool = False

    return job_run_properly_bool



@Project.pre(part_1_initial_parameters_completed)
@Project.post(part_2a_dot_product_calcs_started)
@Project.post(part_2b_dot_product_calcs_completed_properly)
@Project.operation(directives=
    {
        "np": part_2_np_or_nprocesses,
        "cpus-per-task": part_2_cpus_per_task,
        "gpus-per-task": part_2_gpus_per_task,
        "mem-per-cpu": part_2_mem_per_cpu_gb,
        "walltime": part_2_walltime_hr,
    }, with_job=True, cmd=True
)
def part_2_julia_dot_product_calcs_command(job):
    """Run the julia dot product calculations via a bash command."""

    julia_file = "../../src/julia/matrix.jl"
    excel_filename_julia = f'{job.doc.excel_filename_wo_ext}.xlsx'
    excel_sheetname_julia = f'Sheet1'
    dot_product_output_filename_julia = "dot_product_output_file.txt"
    dot_p = f'calc_dot_product("../../{directory_path_to_excel_files_str}/{excel_filename_julia}", ' \
        f'"{excel_sheetname_julia}", "{dot_product_output_filename_julia}", "{job.doc.replicate_number_int}") '
    

    # Run the julia command to do dot product
    print('*********************************************')
    print(f"Running job id {job}")
    run_command = f"julia --load '{julia_file}' -e '{dot_p}'"\
   
    print(f'run command = {run_command}')
    print('*********************************************')

    return run_command


# ******************************************************
# PERFORM THE dot_product CALCULATIONS (END)
# ******************************************************


# ******************************************************
# DATA ANALSYIS: GET THE REPLICATE DATA AVG AND STD. DEV (START)
# ******************************************************

# Check if the average and std. dev. of all the replicates is completed
@Project.label
def part_3_analysis_replica_averages_completed(*jobs):
    """Check that the replicate dot_product averages files are written ."""

    # If any previous replicate averages and std_devs exist delete them, 
    # because they will need recalculated as more state points were added.
    if os.path.isfile(f'../../analysis/{output_avg_std_of_replicates_txt_filename}.txt'):
        os.remove(f'../../analysis/{output_avg_std_of_replicates_txt_filename}.txt')

    file_written_bool_list = []
    all_file_written_bool_pass = False
    for job in jobs:
        file_written_bool = False

        if (
            job.isfile(
                f"../../analysis/{output_avg_std_of_replicates_txt_filename}.txt"
            )
        ):
            file_written_bool = True

        file_written_bool_list.append(file_written_bool)

    if False not in file_written_bool_list:
        all_file_written_bool_pass = True

    return all_file_written_bool_pass


@Project.pre(lambda *jobs: all(part_2b_dot_product_calcs_completed_properly(j)
                               for j in jobs[0]._project))
@Project.post(part_3_analysis_replica_averages_completed)
@Project.operation(directives=
     {
        "np": part_3_np_or_nprocesses,
        "cpus-per-task": part_3_cpus_per_task,
        "gpus-per-task": part_3_gpus_per_task,
        "mem-per-cpu": part_3_mem_per_cpu_gb,
        "walltime": part_3_walltime_hr,
     }, aggregator=aggregator.groupby(key=statepoint_without_replicate, sort_by="excel_filename_wo_ext", sort_ascending=False)
)
def part_3_analysis_replicate_averages_command(*jobs):
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

    write_file_name_and_path = f'analysis/{output_avg_std_of_replicates_txt_filename}.txt' 
    if os.path.isfile(write_file_name_and_path):
        replicate_calc_txt_file = open(write_file_name_and_path, "a")
    else:
        replicate_calc_txt_file = open(write_file_name_and_path, "w")
        replicate_calc_txt_file.write(output_txt_file_header)

    # Loop over all the jobs that have the same "dot_product" (in sort_by="dot_product"). 
    for job in jobs:
        # get the individual values
        output_file = f"{dot_product_output_filename_str}.txt"
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


# ******************************************************
# # DATA ANALSYIS: GET THE REPLICATE DATA AVG AND STD. DEV (END)
# ******************************************************

# ******************************************************
# SIGNAC MAIN CODE SECTION (END)
# ******************************************************


# ******************************************************
# SIGNACS'S ENDING CODE SECTION (START)
# ******************************************************
if __name__ == "__main__":
    pr = Project()
    pr.main()
# ******************************************************
# SIGNACS'S ENDING CODE SECTION (END)
# ******************************************************
