using Pkg
Pkg.add("XLSX")

import XLSX
import Random

function calc_dot_product(excel_filename, excel_sheetname, output_txt_filename, replicate_no)
    try 
        XLSX.openxlsx(excel_filename, enable_cache=false) do f 
            sheet = f[excel_sheetname] 
            for row_i in XLSX.eachrow(sheet) 
                
                # add a dot multiplication vector which is multiplied by Excel input
                mult_vector = [9, 8, 7, 6]
                
                row_number_j = XLSX.row_number(row_i) # `SheetRow` row number 
                if row_number_j == 2
                    # add scalar noise to the data for each replicate with random values
                    random_value = rand(1:10)/10
                    println("random_value=$random_value")

                    v_mult_1 = float(row_i[1]) * mult_vector[1]
                    v_mult_2 = float(row_i[2]) * mult_vector[2]
                    v_mult_3 = float(row_i[3]) * mult_vector[3]
                    v_mult_4 = float(row_i[4]) * mult_vector[4]
                    dot_product = (v_mult_1 + v_mult_2 + v_mult_3 + v_mult_4) * random_value 
                    println("dot_product=$dot_product")

                    output_fname = output_txt_filename

                    file = open(output_fname, "w")
                    write(file, string(dot_product))
                    write(file, "\nDot_Product                Calculations         Completed ")
                    close(file)
                    
                end
            end 
        end


    catch
        # Provide user input that file does not exist
        error(
            "The excel file does not exis or there is an error 
             in the julia 'matrix.jl' file in the 'calc_dot_product' fuction."
        )
    end

end