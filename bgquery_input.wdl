version 1.0

workflow bgquery_workflow
{
    input {
        String json_file
        String preproc_input_var_name
        Int patien_count_to_query
    }


    call bgquery{input:
        json_file=json_file,
        preproc_input_var_name=preproc_input_var_name,
        patien_count_to_query=patien_count_to_query
        }

    output
    {
        Array[File] jsonfile = bgquery.jsonfile
    }
}
task bgquery
{
    input 
    { 
        String json_file
        String preproc_input_var_name
        Int patien_count_to_query
    }
    String ct_interpolation = 'linear'
    String output_dtype = "int"
    command
    <<<
        pip3 install google-cloud-bigquery
        python3 <<CODE
        from google.cloud import bigquery
        import os
        import json
        def query_and_write(json_file_name: str,
                            input_var_name: str,
                            pat_number: int = -1):
            query = """
            WITH
                CT_SERIES AS 
                (
                    SELECT
                        PATIENTID,
                        STUDYINSTANCEUID AS CTSTUDYINSTANCEUID,
                        SERIESINSTANCEUID AS CTSERIESINSTANCEUID,
                        ARRAY_AGG(FORMAT('%s', LEFT(GCS_URL, INSTR(GCS_URL, '#', -1, 1) - 1))) AS INPUT_CT,
                    FROM
                        `{0}`
                    WHERE
                        SOURCE_DOI = "10.7937/K9/TCIA.2015.PF0M9REI"
                        AND MODALITY = "CT"
                    GROUP BY PATIENTID, STUDYINSTANCEUID, SERIESINSTANCEUID
                ),
                RTSTRUCT_SERIES AS 
                (
                    SELECT
                        (PATIENTID),
                        STUDYINSTANCEUID AS RTSTRUCTSTUDYINSTANCEUID,
                        SERIESINSTANCEUID AS RTSTRUCTSERIESINSTANCEUID,
                        ARRAY_AGG(FORMAT('%s', LEFT(GCS_URL, INSTR(GCS_URL, '#', -1, 1) - 1))) AS INPUT_RT,
                    FROM
                        `{0}`
                    WHERE
                        SOURCE_DOI = "10.7937/K9/TCIA.2015.PF0M9REI"
                        AND MODALITY = "RTSTRUCT"
                    GROUP BY PATIENTID, STUDYINSTANCEUID, SERIESINSTANCEUID
                ),
                SEG_SERIES AS 
                (
                    SELECT
                        (PATIENTID),
                        STUDYINSTANCEUID AS SEGSTUDYINSTANCEUID,
                        SERIESINSTANCEUID AS SEGSERIESINSTANCEUID,
                        ARRAY_AGG(FORMAT('%s', LEFT(GCS_URL, INSTR(GCS_URL, '#', -1, 1) - 1))) AS INPUT_SG,
                    FROM
                        `{0}`
                    WHERE
                        SOURCE_DOI = "10.7937/K9/TCIA.2015.PF0M9REI"
                        AND MODALITY = "SEG"
                    GROUP BY PATIENTID, STUDYINSTANCEUID, SERIESINSTANCEUID
                )
            SELECT
                PATIENTID,
                CTSTUDYINSTANCEUID,
                CTSERIESINSTANCEUID,
                INPUT_CT,
                RTSTRUCTSTUDYINSTANCEUID,
                RTSTRUCTSERIESINSTANCEUID,
                INPUT_RT,
                SEGSTUDYINSTANCEUID,
                SEGSERIESINSTANCEUID,
                INPUT_SG
            FROM CT_SERIES JOIN RTSTRUCT_SERIES USING (PATIENTID)
            JOIN SEG_SERIES USING (PATIENTID)
            ORDER BY PATIENTID
            {1}
            """.format(
                'canceridc-data.idc_views.dicom_all',
                '' if pat_number < 1 else 'LIMIT {}'.format(pat_number))
            # print(query)
            client = bigquery.Client()
            query_job = client.query(query)
            q_results = query_job.result()
            content = ''
            if q_results is not None:
                content += (
                    'workspace:PATIENTID' + '\t' +
                    'CTSTUDYINSTANCEUID' + '\t' +
                    'CTSERIESINSTANCEUID' + '\t' +
                    'INPUT_CT' + '\t' +
                    'RTSTRUCTSTUDYINSTANCEUID' + '\t' +
                    'RTSTRUCTSERIESINSTANCEUID' + '\t' +
                    'INPUT_RT' + '\t' +
                    'SEGSTUDYINSTANCEUID' + '\t' +
                    'SEGSERIESINSTANCEUID' + '\t' +
                    'INPUT_SG'
                )
                content_form = (
                    '{}\t' +
                    '{}\t' +
                    '{}\t' +
                    '{}\t' +
                    '{}\t' +
                    '{}\t' +
                    '{}\t' +
                    '{}\t' +
                    '{}\t' +
                    '{}\t'
                )
                data = {}
                vec_data = []
                for row in q_results:
                    content += content_form.format(
                        row.PATIENTID,
                        row.CTSTUDYINSTANCEUID,
                        row.CTSERIESINSTANCEUID,
                        row.INPUT_CT,
                        row.RTSTRUCTSTUDYINSTANCEUID,
                        row.RTSTRUCTSERIESINSTANCEUID,
                        row.INPUT_RT,
                        row.SEGSTUDYINSTANCEUID,
                        row.SEGSERIESINSTANCEUID,
                        row.INPUT_SG
                    )
                    data1 = {}
                    data1["PATIENTID"] = row.PATIENTID
                    data1["CTSTUDYINSTANCEUID"] = row.CTSTUDYINSTANCEUID
                    data1["CTSERIESINSTANCEUID"] = row.CTSERIESINSTANCEUID
                    data1["INPUT_CT"] = row.INPUT_CT
                    data1["RTSTRUCTSTUDYINSTANCEUID"] = row.RTSTRUCTSTUDYINSTANCEUID
                    data1["RTSTRUCTSERIESINSTANCEUID"] = row.RTSTRUCTSERIESINSTANCEUID
                    data1["INPUT_RT"] = row.INPUT_RT
                    data1["SEGSTUDYINSTANCEUID"] = row.SEGSTUDYINSTANCEUID
                    data1["SEGSERIESINSTANCEUID"] = row.SEGSERIESINSTANCEUID
                    data1["INPUT_SG"] = row.INPUT_SG
                    vec_data.append(data1)
                    with open(json_file_name, 'w') as fp:
                        json.dump(
                            {input_var_name: vec_data}, fp, indent=4)
        j_file_name = ~{json_file}
        var_name = ~{preproc_input_var_name}
        lim = ~{patien_count_to_query}
        query_and_write(j_file_name, var_name, lim)
        CODE
    >>>
    
    runtime
    {
        docker: "afshinmha/plastimatch_terra_00:latest"
        memory: "1GB"
    }
    output
    {
        Array[File] jsonfile = glob(json_file)
    }
    meta 
    {
        author: "Afshin"
        email: "akbarzadehm@gmail.com"
        description: "This task queries big_query table to get the data for Prognostics "
    }

}
