### TESTING ASRANK API 3.0

#### Usage for test asrank_api
Initialize env for project
```bash
source env/bin/activate
```
Install all required packages from requirements.txt
```bash
pip install -r requirements.txt
``` 

#### At last run test.py file
**parameters:**   
--source  - Path to file. By default use last(by date) folder in root directory specified in config file.  
--print   - Print first n elements from jsonl file to console. By default (100) specified in config file.  
--compare - Run compare test for ojects (asns, links, orgs etc..). Default equal to "all".  
--u       - Graphql end point (URL).  

##### Usage without parameters    
```bash
 python3 asrank-api-tester.py 
```

##### Usage with parameters
```bash
 python3 asrank-api-tester.py --compare 'asns' --print 'true' 
```
