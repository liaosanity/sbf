#include "map_bloom.h"
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

NAME_SPACE_BS

MapBloom::MapBloom()
{
    bit_num_ = 0;
    capacity_ = 0;
    fail_rate_ = 0.0;
    byte_size_ = 0;
    fd_ = -1;
    mptr_ = NULL;
    need_flush_ = true;
    need_delete_ = false;
}

MapBloom::~MapBloom()
{
    Sync2File();
    
    if (mptr_) 
    {
        munmap(mptr_, byte_size_);
        mptr_ = NULL;
    }
    
    if (fd_ > 0) 
    {
        close(fd_);
        fd_ = -1;
    }

    Unlink();
}

bool MapBloom::Init(int64_t bloom_num, int64_t capacity, double fail_rate, 
    string fname, int64_t bit_num, bool rw)
{
    if (capacity < 0 || fail_rate < 0 || fail_rate > 1) 
    {
        return false;
    }
    
    int ret = access(fname.c_str(), F_OK);
    if (0 == ret && 0 != bit_num) 
    {
        return ResetBloom(bloom_num, capacity, fail_rate, fname, bit_num, rw);
    } 
    else 
    {
	return NewBloom(bloom_num, capacity, fail_rate, fname);
    }
}

bool MapBloom::NewBloom(int64_t bloom_num, int64_t capacity, 
    double fail_rate, string fname)
{
    capacity_ = capacity;
    fail_rate_ = fail_rate;
    path_name_ = fname;

    size_t found = fname.rfind("/");
    fname_ = fname.substr(found + 1);

    double m_g = ((capacity_ * log(fail_rate_)) / (log(2) * log(2))) * -1;
    bit_num_ = ceil(m_g);
    byte_size_ = (bit_num_ / 8) * bloom_num;

    fd_ = open(path_name_.c_str(), O_CREAT | O_RDWR, 0744);
    if (fd_ < 0) 
    {
        return false;
    }

    if (-1 == ftruncate(fd_, byte_size_)) 
    {
        return false;
    }

    void *mptr = mmap(NULL, byte_size_, PROT_READ | PROT_WRITE, 
        MAP_SHARED | MAP_POPULATE, fd_, 0);
    if (MAP_FAILED == mptr) 
    {
        return false;
    }

    mptr_ = (char *)mptr;

    return true;
}

bool MapBloom::ResetBloom(int64_t bloom_num, int64_t capacity, 
    double fail_rate, string fname, int64_t bit_num, bool rw)
{
    capacity_ = capacity;
    fail_rate_ = fail_rate;
    path_name_ = fname;
    bit_num_ = bit_num;
    byte_size_ = (bit_num_ / 8) * bloom_num;

    size_t found = fname.rfind("/");
    fname_ = fname.substr(found + 1);

    fd_ = open(path_name_.c_str(), (rw ? O_RDWR : O_RDONLY), 0744);
    if (fd_ < 0) 
    {
        return false;
    }

    void *mptr = mmap(NULL, byte_size_, 
        (rw ? (PROT_READ | PROT_WRITE) : PROT_READ), 
        MAP_SHARED | MAP_POPULATE, fd_, 0);
    if (MAP_FAILED == mptr) 
    {
        return false;
    }

    mptr_ = (char *)mptr;

    return true;
}

void MapBloom::Add(int64_t offset, vector<int64_t> &hash_vals)
{
    int64_t val = 0;
    int64_t bkt = 0;
    int off = 0;

    for (auto v : hash_vals) 
    {
        val = v % bit_num_;
        bkt = val / 8;
        off = val % 8;
        
        char *ptr = mptr_ + offset + bkt;
        *ptr |= (1 << off);
    }
}

bool MapBloom::Lookup(int64_t offset, vector<int64_t> &hash_vals)
{
    for (auto val : hash_vals) 
    {
    	if (!Get(offset, val % bit_num_)) 
        {
	    return false;
    	}
    }

    return true;
}

bool MapBloom::Get(int64_t offset, int64_t val)
{
    int64_t bkt = val / 8;
    int off = val % 8;
    
    char *ptr = mptr_ + offset + bkt;
    
    return (0 == (*ptr & (1 << off))) ? false : true;
}

void MapBloom::Unlink()
{
    if (need_delete_) 
    {
    	unlink(path_name_.c_str());		
    }
}

void MapBloom::Sync2File()
{
    if (!need_flush_) 
    {
    	return;
    }
    
    msync(mptr_, byte_size_, MS_SYNC);
}

int64_t MapBloom::GetBitNum()
{
    return bit_num_;
}

string MapBloom::GetFileName()
{
    return fname_;
}

char *MapBloom::GetMapPtr()
{
    return mptr_;
}

void MapBloom::StartFlush()
{
    need_flush_ = true;
}

void MapBloom::StopFlush()
{
    need_flush_ = false;
}

void MapBloom::SetDelete(bool del)
{
    need_delete_ = del;
}

NAME_SPACE_ES
