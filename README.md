# Overview
A simple bloom filter, run as an http service.

# Quick start
1) Building
 * Environment dependence:   
   Centos 7.2  
 * Software dependence (install it to '$(HOME)/opt/'):  
   shs-1.0.0
 * Source compile:
```
   make
   make package
```
2) Deployment (run it on a server like 192.168.1.11, under '/home/test' dir)
 * scp packages/sbf-1.0.0.tgz 192.168.1.11:/tmp
 * run these cmd as bellow to deploy:
```
   cd /home/test
   tar xzvf /tmp/sbf-1.0.0.tgz -C .
   ln -s sbf-1.0.0 sbf
   cd sbf/tools/
   ./install.sh demo
   ./serverctl start
```

# Test
1) Get request, if you run a http request via chrome like this (http://192.168.1.11:10018/sbf/filter?uid=Jeremy&sid=888888&action=0&vids=0,1,2,3,4|5,6,7,8,9), Then you'll get these results as bellow, all of them will not be filtered:

![image](https://github.com/liaosanity/sbf/raw/master/images/get.png)

2) Add request, if you run a http request via chrome like this (http://192.168.1.11:10018/sbf/filter?uid=Jeremy&sid=888888&action=1&vids=0,1|5,6), Then get'll these results as bellow:

![image](https://github.com/liaosanity/sbf/raw/master/images/add.png)

Finally, run a get request again, 0,1,5,6 will be filtered, like this:

![image](https://github.com/liaosanity/sbf/raw/master/images/get2.png)

