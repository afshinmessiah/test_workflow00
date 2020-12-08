from google.cloud import bigquery
import os
import json
def WriteStringToFile(file_name, content, append=False):
    folder = os.path.dirname(file_name)
    if not os.path.exists(folder):
        os.makedirs(folder)
    if append:
        text_file = open(file_name, "a")
    else:
        text_file = open(file_name, "w")
    n = text_file.write(content)
    text_file.close()

def add_or_append_to_dict(d: dict, key: str, value):
    if key in d:
        d[key].append(value)
    else:
        d[key] = [value]

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
""".format('canceridc-data.idc_views.dicom_all')
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
        add_or_append_to_dict(
            data, "PATIENTID", row.PATIENTID)
        add_or_append_to_dict(
            data, "CTSTUDYINSTANCEUID", row.CTSTUDYINSTANCEUID)
        add_or_append_to_dict(
            data, "CTSERIESINSTANCEUID", row.CTSERIESINSTANCEUID)
        add_or_append_to_dict(
            data, "INPUT_CT", row.INPUT_CT)
        add_or_append_to_dict(
            data, "RTSTRUCTSTUDYINSTANCEUID", row.RTSTRUCTSTUDYINSTANCEUID)
        add_or_append_to_dict(
            data, "RTSTRUCTSERIESINSTANCEUID", row.RTSTRUCTSERIESINSTANCEUID)
        add_or_append_to_dict(
            data, "INPUT_RT", row.INPUT_RT)
        add_or_append_to_dict(
            data, "SEGSTUDYINSTANCEUID", row.SEGSTUDYINSTANCEUID)
        add_or_append_to_dict(
            data, "SEGSERIESINSTANCEUID", row.SEGSERIESINSTANCEUID)
        add_or_append_to_dict(
            data, "INPUT_SG", row.INPUT_SG)
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
        if len(vec_data) < 2:
            vec_data.append(data1)
        
    
with open('preprocessing.json', 'w') as fp:
    json.dump({"preprocessing_workflow.inputs": vec_data}, fp, indent=4)
WriteStringToFile('./input.tsv', content)
    
        
