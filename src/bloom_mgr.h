#ifndef BLOOM_MGR_H
#define BLOOM_MGR_H

#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>
#include <list>
#include <map>
#include "map_bloom.h"
#include "hash.h"
#include "common.h"
#include "context.h"

#define UID_LEN 64 
#define TYPE_SHOW 1

using namespace std;

NAME_SPACE_BS

typedef struct bloom_index_s 
{
    bool need_sync;
    bool need_del;
    int fd;
    int64_t fsize;
    char *mptr;
    string fname;

    bloom_index_s()
    {
        need_sync = true;
        need_del = false;
        fd = -1;
        fsize = 0;
        mptr = NULL;
    }

    ~bloom_index_s()
    {
        if (mptr) 
        {
            munmap(mptr, fsize);
            mptr = NULL;
        }
        
        if (fd > 0) 
        {
            close(fd);
            fd = -1;
        }

        if (need_sync) 
        {
            sync2file();
        }

        if (need_del) 
        {
            unlink(fname.c_str());
        }
    }

    void sync2file()
    {
        msync(mptr, fsize, MS_SYNC);
    }
} bloom_index_t;

typedef boost::shared_ptr<bloom_index_t> BloomIdxPtr;

typedef struct bloom_offset_s 
{
    char uid[UID_LEN];
    int64_t offset;
    int64_t len;
    int64_t max_adds;
    int64_t adds; 

    bloom_offset_s()
    {
        memset(uid, 0x00, UID_LEN);
        offset = 0;
        len = 0;
        max_adds = 0;
        adds = 0; 
    }
} bloom_offset_t;

typedef boost::shared_ptr<bloom_offset_t> BloomOffsetPtr;

class BloomMgr
{
public:
    explicit BloomMgr(string &prefix, int64_t bloom_num, int64_t capacity, 
        double fail_rate, int days, int create_bloom_at, int32_t type); 
    virtual ~BloomMgr();

    // call it in InitInMaster
    bool InitBlooms();
    // call it in InitInWorker
    void StartReloadMeta();

    bool Add(ContextPtr ctx);
    void Get(ContextPtr ctx);
    void GetBloom(ContextPtr ctx);

    void Sync2File();

private:
    bool ReadMeta();
    bool ResetBlooms();
    bool AddNewBloom();
    bool CreateIndex(BloomIdxPtr bloom_idx);
    bool LoadIndex(BloomIdxPtr bloom_idx, string &bloom_name, bool rw = true);
    void WriteMeta();
    void CreateBloomHandle();
    void ReloadMetaHandle();
    void ReloadMeta();
    void CalcHash(string &vid, vector<int64_t> &hashs);
    bool Lookup(string &uid, string &vid, int days);
    void SyncBloomIndex();
    void StartDeleteBloomIdx(string &bloom_name);
    void DeleteBloomIdxHandle(string &bloom_name);

private:
    string last_hour_;
    string prefix_;
    int64_t bloom_num_;
    int64_t capacity_;
    double fail_rate_;
    int days_;
    int64_t max_adds_;
    int create_bloom_at_;
    int32_t type_;
    int64_t last_mtime_;
    volatile int64_t last_idxs_;

    vector<HashHandle> hash_handles_;
    list<string> bloom_finfos_;
    list<MapBloomPtr> blooms_;
    list<BloomIdxPtr> bloom_idxs_;
    map<string, list<BloomOffsetPtr>> uid2offset_;
    map<string, list<string>> bloom2uid_;
    boost::shared_ptr<boost::thread> create_bloom_thread_;
    boost::shared_ptr<boost::thread> reload_meta_thread_;
    mutable boost::shared_mutex mutex_;
    mutable boost::interprocess::interprocess_mutex proc_mutex_;
};

typedef boost::shared_ptr<BloomMgr> BloomMgrPtr;

NAME_SPACE_ES

#endif
