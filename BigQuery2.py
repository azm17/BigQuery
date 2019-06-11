# -*- coding: utf-8 -*-
from google.cloud import bigquery


def main():
    client = bigquery.Client.from_service_account_json('glassy-life-203401-306be48182fc.json')

    repoid_list=[58172287    ,
45936895    ,
36040894    ,
48109239    ,
93444615    ,
147595186    ,
107153495    ,
121444325    ,
145207303    ,
]
    
    for repo_id in repoid_list:
        query = gen_query(repo_id)
    
        config = bigquery.QueryJobConfig()
        config.use_legacy_sql = True
        string=''
        
        rows = client.query(query, job_config=config).result()
        for row in rows:
            print(row[0])
            string = string +'\n'+str(row[0])
        
        file = open(str(repo_id)+'_time.csv', 'w')
     
        file.write(string)
        file.close()

def gen_query(repo_id):
    query = '''
    SELECT
(STRFTIME_UTC_USEC(TIMESTAMP_TO_USEC(created_at) , "%Y/%m/%d %H:%M:%S")) AS time,
FROM
TABLE_DATE_RANGE([githubarchive.day.],
                   TIMESTAMP('2018-09-01'),
                   TIMESTAMP('2018-09-30'))
                   #where repo.id = 106803605
where repo.id ='''+str(repo_id)+ '''and type ='PullRequestEvent'
                   '''
    return query

if __name__ == "__main__":
    main()