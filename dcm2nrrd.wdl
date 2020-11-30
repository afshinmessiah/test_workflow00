version 1.0

workflow conversion_workflow
{
    Array[File] input_file
    # File out = "gs://fc-c6818520-7b26-46f8-aff1-57c4db31da5a/output00/simple_output.nrrd"
    call dcm2nrrd_plastimatch{ input: in_file=input_file }
}
task dcm2nrrd_plastimatch 
{
    input 
    { 
        Array[File] in_file
        # File out_file 
    }
    command
    {
        mkdir "data"
        mv $[sep=' ' in_file] "data"
        plastimatch convert --input data --output-img test.nrrd --output-type float
    }
    runtime
    {
        docker: "biocontainers/plastimatch:v1.7.4dfsg.1-2-deb_cv1"
        memory: "4GB"

    }
    output 
    {
        File out = "test.nrrd"
    }

}