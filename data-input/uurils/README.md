These are instructions for updating database(s). Currently we have a test database (called asrankt and asrankwt(web part that we are going to remove in v2.1)
and live database (called asrank and asrankw (web part)) up and running at the same time.

0. Make respective changes in asrank_utils/config/  and uurils/aii_v2/config/
    - @micheal it isn't clear which files you are talking about or what this step is for?  
      Is it part of setting up test/prod/etc? 

        - I can't find a configs/packet/prod.*
        - They are in folder asrank_utils/config/  and uurils/aii_v2/config/
	
	- I can't find ../services.yaml
	-You dont need .yaml for data upload
        
	- I can't find a .env* files in https://github.com/CAIDA/asrank-tools/tree/master/data-input/uurils
        - You dont need .env for data upload. .env are needed for Symfony
	
#### Upload data for restv2-server
1. Create influx database if not exist using Linux terminal:  
    ```influx```  
    ```create database asrank (OR your username)```    
    ```create database asrankt (OR your usernamet"```
    ```create database asrankw (OR your usernamew)```
    ```create database asrankwt (OR your usernamewt)```
    ```quit```
2. Change directory to uurils using linux terminal:  
    ```cd path_to_uurils (/www/data-import/uurils)```
3. Configure file on path **uurils/aii_v2/config/config.py** for followin options:  
   path to root directory that contain json data directories  
		**'input_folder': '/www/data-import/data/'**  
   set ENV mode for prod or dev changing **DEBUG** var. For prod set to **False**    
		```DEBUG = False```
4. Run script:  
   Before running this script set DEBUG= True (for test) or DEBUG=False (for production)

   ```pyhton3 main.py```  
    
#### Upload data for web-server

1. run ./main.py to load the data data into memory
~~~
# Load the nessary python libraries 
source /www/data-import/uurils/env/bin/activate
# Create the logs directory if it doesn't exist, should only
# be needed the first time you run the program
mkdir --parents logs
# import the data to 
python3 main.py -s /www/data-import/data/20190101

**what is the next step to push this live????**
~~~

#####In case of problems, this might help:
~~~
sudo chwon -R baitaluk:www-data var
sudo chmod -R 0775 var
sh run.sh -e dev

sudo serivce nginx stop
sudo service php2.7-fpm stop
!!sudo fuser -n tcp 80
sudo serivce nginx start
sudo service php2.7-fpm start
mkdir cache/prod
~~~


2. Check that the data looks good at as-rank-test.caida.org
- If there is a problem, update json files and repeat step 1.

3. Assuming it looks @micheal what is the next step?
Follow the intrusctions here:
- https://github.com/CAIDA/asrank-api-v2/blob/master/README.md#mht4


### Load data in memcached
0.Make respective changes in config.py
- @micheal. That isn't enough information. What is the respective changes that need to be done? 

#### Load SVG images for radar
1. Change directory to **asrank_utils/mc_util**
2. Run the script mcr.py:  
    ```python3 mcr.py```
#### Load tables in memcached
3. Run the script mcr.py:  
    ```python3 mct.py```

To switch as-rank-2 data to as-rank-test restart both web and rest server:
sh run.sh -e dev -in test mode (asrankt and asrankwt tables)

To switch back:
sh run.sh -e prod - in prod mod

Actully, to be clear we dont have dev and prod modes we have mode1 (with asrank and asrankw tables)
and mode2 (with asrankt and asrankwt tables). And keep switching between them, when new data comes in.
