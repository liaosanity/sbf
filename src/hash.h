#ifndef HASH_H
#define HASH_H 

#include <tr1/functional>
#include <string>
#include "common.h"

using namespace std;

NAME_SPACE_BS

class Hash 
{
public:
    static int64_t Simple_hash(const string &str);  
    static int64_t RS_hash(const string &str);  
    static int64_t JS_hash(const string &str);  
    static int64_t PJW_hash(const string &str);  
    static int64_t ELF_hash(const string &str);  
    static int64_t BKDR_hash(const string &str);  
    static int64_t SDBM_hash(const string &str);  
    static int64_t DJB_hash(const string &str);  
    static int64_t AP_hash(const string &str);  

private:
    static int64_t max_long_;
}; 

typedef tr1::function<int64_t(const string &str)> HashHandle;

NAME_SPACE_ES

#endif

