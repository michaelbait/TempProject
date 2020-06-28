### Data Uploader for AS Rank GraphQL API v2

Initialize env for project
```bash
source env/bin/activate
```
Install all required packages from requirements.txt
```bash
pip install -r requirements.txt
```

The data uploading process consists of 3 phases:

1) db upload/update 
2) test (api vs json)  
3) switch between dbs


1. Db upload/update with the 'Uploader' 

Input data folder is set up in /config/config.py under OPTIONS as 'input_folder'
Input data folder contains mutiple datasets folders as 20131101, 20131201,..., 20191201
The db to which data is uploaded is set up in /config/config.py under OPTIONS as 'postgresql'

 just run as:

```
python3 main.py
```

2. Test (api vs json) - project asrank_api3_test


#### At last run test.py file
**parameters:**...
--source  - Path to file. By default use last(by date) folder in root directory specified in config file...
--print   - Print first n elements from jsonl file to console. By default (100) specified in config file..
--compare - Run compare test for ojects (asns, links, orgs etc..). Default equal to "all".
--u       - Graphql end point (URL).

##### Usage without parameters....
```bash
 python3 asrank-api-tester.py.
```

##### Usage with parameters
```bash
 python3 asrank-api-tester.py --compare 'asns' --print 'true'.
```


3. Now we have 2 production (old/new) dbs to switch API between 
(since now the upload/update might take hours): asrank1 and asrank2

db is set up .env.local under DATABASE_URL (just change the name asrank1<->asrank2)
