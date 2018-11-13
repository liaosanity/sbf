#ifndef MAP_BLOOM_H
#define MAP_BLOOM_H

#include <boost/shared_ptr.hpp>
#include <string>
#include "common.h"

using namespace std;
using namespace boost;

NAME_SPACE_BS

class MapBloom
{
public:
    MapBloom();
    virtual ~MapBloom();

    bool Init(int64_t bloom_num, int64_t capacity, double fail_rate, 
        string fname, int64_t bit_num = 0, bool rw = true);

    void Add(int64_t offset, vector<int64_t> &hash_vals);
    bool Lookup(int64_t offset, vector<int64_t> &hash_vals);

    void Sync2File();
    void StartFlush();
    void StopFlush();
    void SetDelete(bool del);
    int64_t GetBitNum();
    string GetFileName();
    char *GetMapPtr();

private:
    bool NewBloom(int64_t bloom_num, int64_t capacity, 
        double fail_rate, string fname);
    bool ResetBloom(int64_t bloom_num, int64_t capacity, double fail_rate, 
        string fname, int64_t bit_num, bool rw);
    void Unlink();
    bool Get(int64_t offset, int64_t val);

private:
    int64_t bit_num_; 
    int64_t capacity_;
    int64_t byte_size_;
    double fail_rate_;
    int fd_;
    bool need_flush_;
    bool need_delete_;
    char *mptr_;
    string path_name_;
    string fname_;
};

typedef boost::shared_ptr<MapBloom> MapBloomPtr;

NAME_SPACE_ES

#endif

