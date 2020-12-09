version 1.0

workflow preprocessing_workflow
{
    input {
        Array[Object] inputs
        String output_file
    }
    # File out = "gs://fc-c6818520-7b26-46f8-aff1-57c4db31da5a/output00/simple_output.nrrd"
    call preprocessing_task
    { 
        input: dicom_ct_list=inputs[0].INPUT_CT,
        dicom_rt_list=inputs[0].INPUT_RT,
        output_dir='./xxx',
        pat_id=inputs[0].PATIENTID
    }
}
task preprocessing_task
{
    input 
    { 
        Array[File] dicom_ct_list
        Array[File] dicom_rt_list
        String output_dir
        String pat_id
    }
    String ct_interpolation = 'linear'
    String output_dtype = "int"
    command
    <<<
        python3 <<CODE
        import os
        import subprocess
        import json
        def Find(address, max_depth = 0, cond_function = os.path.isfile,
                sort_key = None, reverse_sort = False,
                find_parent_folder=False) -> list:
            # rood depth is max_depth = 1
            address = os.path.abspath(address)
            approved_list = []
            RecursiveFind(address, approved_list, 1, max_depth,
                    cond_function, find_parent_folder)
            if sort_key is not None:
                approved_list.sort(key = sort_key, reverse=reverse_sort)
            return approved_list


        def RecursiveFind(address, approvedlist, current_depth:int, max_depth = 0,
                        cond_function = os.path.isfile, find_parent_folder=False):
            filelist = os.listdir(address)
            for i in range(0, len(filelist)):
                filelist[i] = os.path.join(address, filelist[i])
            for filename in filelist:
                if os.path.isdir(filename) and(
                    max_depth <= 0 or current_depth < max_depth):
                    RecursiveFind(filename, approvedlist, current_depth + 1, max_depth,
                    cond_function, find_parent_folder)
            for filename in filelist:
                if cond_function(filename):
                    if find_parent_folder:
                        approvedlist.append(address)
                        break
                    else:
                        approvedlist.append(filename)
        def export_res_nrrd_from_dicom(dicom_ct_path, dicom_rt_path, output_dir, pat_id,
                               ct_interpolation = 'linear', output_dtype = "int"):
  
            """
            Convert DICOM CT and RTSTRUCT sequences to NRRD files and resample to 1-mm isotropic
            exploiting plastimatch (direct call, bash-like).
            
            @params:
                dicom_ct_path - required :
                dicom_rt_path - required :
                output_dir    - required : 
                pat_id        - required :
                output_dtype  - optional : 
                
            @returns:
                out_log : 
                
            """
            
            out_log = dict()
            
            # temporary nrrd files path (DICOM to NRRD, no resampling)
            ct_nrrd_path = os.path.join(output_dir, 'tmp_ct_orig.nrrd')
            rt_folder = os.path.join(output_dir, pat_id  + '_whole_ct_rt')
            
            # log the labels of the exported segmasks
            rt_struct_list_path = os.path.join(output_dir, pat_id + '_rt_list.txt')
            
            # convert DICOM CT to NRRD file - no resampling
            bash_command = list()
            bash_command += ["plastimatch", "convert"]
            bash_command += ["--input", dicom_ct_path]
            bash_command += ["--output-img", ct_nrrd_path]
                            
            # print progress info
            print("Converting DICOM CT to NRRD using plastimatch... ", end = '')
            out_log['dcm_ct_to_nrrd'] = subprocess.call(bash_command)
            print("Done.")
            
            
            # convert DICOM RTSTRUCT to NRRD file - no resampling
            bash_command = list()
            bash_command += ["plastimatch", "convert"]
            bash_command += ["--input", dicom_rt_path]
            bash_command += ["--referenced-ct", dicom_ct_path]
            bash_command += ["--output-prefix", rt_folder]
            bash_command += ["--prefix-format", 'nrrd']
            bash_command += ["--output-ss-list", rt_struct_list_path]
            
            # print progress info
            print("Converting DICOM RTSTRUCT to NRRD using plastimatch... ", end = '')
            out_log['dcm_rt_to_nrrd'] = subprocess.call(bash_command)
            print("Done.")
            
            # look for the labelmap for GTV
            gtv_rt_file = [f for f in os.listdir(rt_folder) if 'gtv-1' in f.lower()][0]
            rt_nrrd_path = os.path.join(rt_folder, gtv_rt_file)
            
            ## ----------------------------------------
            
            # actual nrrd files path 
            res_ct_nrrd_path = os.path.join(output_dir, pat_id + '_ct_resampled.nrrd')
            res_rt_nrrd_path = os.path.join(output_dir, pat_id + '_rt_resampled.nrrd')
            
            # resample the NRRD CT file to 1mm isotropic
            bash_command = list()
            bash_command += ["plastimatch", "resample"]
            bash_command += ["--input", ct_nrrd_path]
            bash_command += ["--output", res_ct_nrrd_path]
            bash_command += ["--spacing", "1 1 1"]
            bash_command += ["--interpolation", ct_interpolation]
            bash_command += ["--output-type", output_dtype]
            
            # print progress info
            print("\nResampling NRRD CT to 1mm isotropic using plastimatch... ", end = '')
            out_log['dcm_nrrd_ct_resampling'] = subprocess.call(bash_command)
            print("Done.")
            
            # FIXME: log informations about the native volume
            #out_log["shape_original"] = list(tmp.)
            
            
            # resample the NRRD RTSTRUCT file to 1mm isotropic
            bash_command = list()
            bash_command += ["plastimatch", "resample"]
            bash_command += ["--input", rt_nrrd_path]
            bash_command += ["--output", res_rt_nrrd_path]
            bash_command += ["--spacing", "1 1 1"]
            bash_command += ["--interpolation", "nn"]
                
            # print progress info
            print("Resampling NRRD RTSTRUCT to 1mm isotropic using plastimatch... ", end = '')
            out_log['dcm_nrrd_rt_resampling'] = subprocess.call(bash_command)
            print("Done.")

            
            # clean up
            print("\nRemoving temporary files (DICOM to NRRD, non-resampled)... ", end = '')
            os.remove(ct_nrrd_path)
            # FIXME: keep the RTSTRUCTs (latest LUNG1 has multiple structures --> additional checks afterwards)?
            #os.remove(rt_nrrd_path)
            print("Done.")
            return out_log
        dicom_ct_path = os.path.dirname('~{dicom_ct_list[0]}')
        print('dicom_ct_path = {}'.format(dicom_ct_path))
        dicom_rt_path = '~{dicom_rt_list[0]}'
        print('dicom_rt_path = {}'.format(dicom_rt_path))
        export_res_nrrd_from_dicom(
            dicom_ct_path,
            dicom_rt_path,
            '~{output_dir}', '~{pat_id}',
            '~{ct_interpolation}', '~{output_dtype}'
        )
        output_file_list = Find('~{output_dir}')
        with open('outputfiles.json', 'w') as fp:
            json.dump({'data':output_file_list}, fp, indent=4)
        print('this is all {} files\n {}'.format(
            len(output_file_list), json.dumps(output_file_list, indent=4)))
        CODE
    >>>
    runtime
    {
        # docker: "biocontainers/plastimatch:v1.7.4dfsg.1-2-deb_cv1"
        docker: "afshinmha/plastimatch_terra_00:latest"
        memory: "4GB"

    }
    output 
    {
        Object outtt = read_json('outputfiles.json')
        Array[String] outputfiles = outtt.data
    }
    meta {
        author: "Afshin"
        email: "akbarzadehm@gmail.com"
        description: "This is a test on terra"
    }

}