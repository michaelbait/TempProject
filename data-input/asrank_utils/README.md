
#### Start memcached server at different ports:
1. In Linux terminal:
```
sudo memcached -u memcache  -A -m 32000 -I 64m -d 127.0.0.1 -p 11211
sudo memcached -u memcache -A -m 32000 -I 64m -d 127.0.0.1 -p 11212
```
memcached at port 11211 for asrank  
memcached at port 11212 for asrankw
2. Test memcached:
```
telnet 127.0.0.1 11211
stats
```
3. For shutdown memcached server use telnet command:
```
shutdown
```
To see memcached pids use linux terminal command:  
```
pidof memcached
```

------

Script run with params:


arankw_influxdb_import.py -s /path_to_json_data_files -dp all


params:


-s - path to data sources


-dp - datapoints to be imported ex. (locations, asns, datasource or all)
