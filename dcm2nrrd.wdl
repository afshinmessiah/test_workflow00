version 1.0

workflow conversion_workflow
{
    input {
        Array[File] input_file
        File output_file
    }
    # File out = "gs://fc-c6818520-7b26-46f8-aff1-57c4db31da5a/output00/simple_output.nrrd"
    call dcm2nrrd_plastimatch{ input: in_file=input_file,  out_file=output_file}
}
task dcm2nrrd_plastimatch 
{
    input 
    { 
        Array[File] in_file
        File out_file 
    }
    command
    {
        mkdir -p "./data"
        echo 'made folder data'
        cp ${sep=' ' in_file} "./data"
        echo 'moved data to folder'
        plastimatch convert --input ./data --output-img ${out_file} --output-type float
        echo 'ran plastimatch'
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
    meta {
        author: "Afshin"
        email: "akbarzadehm@gmail.com"
        description: "This is a test on terra"
    }

}