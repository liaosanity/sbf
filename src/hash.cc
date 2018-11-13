#include "hash.h"

NAME_SPACE_BS

int64_t Hash::max_long_ = 0x7FFFFFFFFFFFFFFF;

int64_t Hash::Simple_hash(const string &str)
{
    int64_t hash = 0;  
    unsigned char *p = NULL;  

    const char *pstr = str.c_str(); 

    for (hash = 0, p = (unsigned char *)pstr; *p; p++) 
    {
        hash = 31 * hash + *p;  
    } 

    return (hash & max_long_);
}

int64_t Hash::RS_hash(const string &str)
{
    int64_t b = 378551;  
    int64_t a = 63689;  
    int64_t hash = 0;  

    const char *pstr = str.c_str();

    while (*pstr) 
    {  
        hash = hash * a + (*pstr++);  
        a *= b;  
    }  

    return (hash & max_long_);  
}

int64_t Hash::JS_hash(const string &str)
{
    int64_t hash = 1315423911;  

    const char *pstr = str.c_str();

    while (*pstr) 
    {  
        hash ^= ((hash << 5) + (*pstr++) + (hash >> 2));  
    }  

    return (hash & max_long_); 
}

int64_t Hash::PJW_hash(const string &str)
{
    int64_t bits = (int64_t)(sizeof(int64_t) * 8);  
    int64_t quarters = (int64_t)((bits * 3) / 4);  
    int64_t one_eighth = (int64_t)(bits / 8);  
    int64_t high_bits = (int64_t)(0xFFFFFFFF) << (bits - one_eighth);  
    int64_t hash = 0;  
    int64_t test = 0;  

    const char *pstr = str.c_str();

    while (*pstr) 
    {  
        hash = (hash << one_eighth) + (*pstr++);  
        if ((test = hash & high_bits) != 0) 
        {  
            hash = ((hash ^ (test >> quarters)) & (~high_bits));  
        }  
    }  

    return (hash & max_long_);  
}

int64_t Hash::ELF_hash(const string &str)
{
    int64_t hash = 0;  
    int64_t x = 0;  

    const char *pstr = str.c_str();

    while (*pstr) 
    {  
        hash = (hash << 4) + (*pstr++);  
        if ((x = hash & 0xF0000000L) != 0) 
        {  
            hash ^= (x >> 24);  
            hash &= ~x;  
        }  
    }  

    return (hash & max_long_); 
}

int64_t Hash::BKDR_hash(const string &str)
{
    int64_t seed = 131313;
    int64_t hash = 0;  

    const char *pstr = str.c_str();

    while (*pstr) 
    {  
        hash = hash * seed + (*pstr++);  
    }  

    return (hash & max_long_);  
}

int64_t Hash::SDBM_hash(const string &str)
{
    int64_t hash = 0;  

    const char *pstr = str.c_str();

    while (*pstr) 
    {  
        hash = (*pstr++) + (hash << 6) + (hash << 16) - hash;  
    }  

    return (hash & max_long_);  
}

int64_t Hash::DJB_hash(const string &str)
{
    int64_t hash = 5381;  

    const char *pstr = str.c_str();

    while (*pstr) 
    {  
        hash += (hash << 5) + (*pstr++);  
    }  

    return (hash & max_long_);  
}

int64_t Hash::AP_hash(const string &str)
{
    int64_t hash = 0;  

    const char *pstr = str.c_str();

    for (int i = 0; *pstr; i++) 
    {  
        if ((i & 1) == 0) 
        {  
            hash ^= ((hash << 7) ^ (*pstr++) ^ (hash >> 3));  
        } 
        else 
        {  
            hash ^= (~((hash << 11) ^ (*pstr++) ^ (hash >> 5)));  
        }  
    }  

    return (hash & max_long_); 
}

NAME_SPACE_ES
