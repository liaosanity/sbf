#include "bloom_mgr.h"

#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>  
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <fstream>
#include <sstream>
#include "comm/logging.h"

#define META_ITEMS 5
#define BLOOM_NAME_SZ 16

LOG_NAME("Filter");

NAME_SPACE_BS

typedef boost::interprocess::scoped_lock<
    boost::interprocess::interprocess_mutex> ScopedLock;

BloomMgr::BloomMgr(string &prefix, int64_t bloom_num, int64_t capacity, 
    double fail_rate, int days, int create_bloom_at, int32_t type) 
    : prefix_(prefix)
    , bloom_num_(bloom_num)
    , capacity_(capacity)
    , fail_rate_(fail_rate)
    , days_(days)
    , create_bloom_at_(create_bloom_at)
    , type_(type)
{
    double m_g = ((capacity * log(fail_rate)) / (log(2) * log(2))) * -1;
    max_adds_ = (ceil(m_g) / 8) * 0.99;

    hash_handles_.push_back(tr1::bind(&Hash::AP_hash, 
        tr1::placeholders::_1));
    hash_handles_.push_back(tr1::bind(&Hash::RS_hash, 
        tr1::placeholders::_1));
    hash_handles_.push_back(tr1::bind(&Hash::JS_hash, 
        tr1::placeholders::_1));
    hash_handles_.push_back(tr1::bind(&Hash::PJW_hash, 
        tr1::placeholders::_1));
    hash_handles_.push_back(tr1::bind(&Hash::ELF_hash, 
        tr1::placeholders::_1));
    hash_handles_.push_back(tr1::bind(&Hash::BKDR_hash, 
        tr1::placeholders::_1));
    hash_handles_.push_back(tr1::bind(&Hash::DJB_hash, 
        tr1::placeholders::_1));
    hash_handles_.push_back(tr1::bind(&Hash::SDBM_hash, 
        tr1::placeholders::_1));
}

BloomMgr::~BloomMgr()
{
    blooms_.clear();
    bloom_idxs_.clear();
}

bool BloomMgr::InitBlooms()
{
    if (-1 == access(prefix_.c_str(), 0)) 
    {
        LOG(ERROR) << "dir: " << prefix_ << " doesn't exist.";

        return false;
    }

    bool bRet = false; 
    bool meta_exist = ReadMeta();

    if (meta_exist) 
    {
        bRet = ResetBlooms();
    } 
    else 
    {
        bRet = AddNewBloom();
    }

    if (!bRet) 
    {
        return false;
    }

    create_bloom_thread_.reset(new boost::thread(tr1::bind(
        &BloomMgr::CreateBloomHandle, this)));
    create_bloom_thread_->detach();

    return true;
}

bool BloomMgr::ReadMeta()
{
    string fname = prefix_ + "/.meta";
	
    fstream fin(fname, ios::binary | ios::in | ios::out);
    if (!fin.is_open() || !fin.good()) 
    {
        return false;
    }

    while (!fin.eof()) 
    {
        string line;
        getline(fin, line);
        if (line.empty()) 
        {
            continue;
        }
    	
        bloom_finfos_.push_back(line);
        if (bloom_finfos_.size() >= days_) 
        {
            break;
        }
    }

    fin.close();

    return !bloom_finfos_.empty();
}

bool BloomMgr::ResetBlooms()
{
    if (bloom_finfos_.empty()) 
    {
        return false;
    }

    bool rw = true;
    bool set_once = true;

    auto ite = bloom_finfos_.begin();
    for (; ite != bloom_finfos_.end(); ++ite) 
    {
        vector<string> bloom_info;
        boost::split(bloom_info, *ite, boost::is_any_of("\t"));
        if (META_ITEMS != bloom_info.size()) 
        {
            continue;
        }

        if (set_once) 
        {
            last_hour_ = bloom_info[0];
            set_once = false;
        } 
        else 
        {
            rw = false;
        }

        string fname = bloom_info[0];
        string bfname = prefix_ + "/" + fname;
        int64_t bloom_num = boost::lexical_cast<int64_t>(bloom_info[1]);    
        int64_t capacity = boost::lexical_cast<int64_t>(bloom_info[2]);    
        double fail_rate = boost::lexical_cast<double>(bloom_info[3]);    
        int64_t bit_num = boost::lexical_cast<int64_t>(bloom_info[4]);    

        MapBloomPtr bloom(new MapBloom);
        if (!bloom) 
        {
            return false;
        }

        if (!bloom->Init(bloom_num, capacity, fail_rate, bfname, 
            bit_num, rw)) 
        {
            return false;
        }

        BloomIdxPtr bloom_idx(new bloom_index_t);
        if (!bloom_idx) 
        {
            return false;
        }
        bloom_idx->fname = prefix_ + "/.idx_" + fname;
        bloom_idx->fsize = sizeof(int64_t) 
            + sizeof(bloom_offset_t) * bloom_num_;

        if (!LoadIndex(bloom_idx, fname, rw)) 
        {
            return false;
        }

        blooms_.push_back(bloom);
        bloom_idxs_.push_back(bloom_idx);

        LOG(INFO) << "ResetBloom\tbloom_name=" << bfname 
            << "\tlast_hour=" << last_hour_ << "\toffset_num=" 
            << uid2offset_.size() << "\tbloom_num=" 
            << bloom2uid_[fname].size();
    }

    auto bite = blooms_.begin();
    for (; bite != blooms_.end(); ++bite) 
    {
        if (bite == blooms_.begin()) 
        {
   	    (*bite)->StartFlush();
        } 
        else 
        {
    	    (*bite)->StopFlush();
    	}
    }

    auto bitx = bloom_idxs_.begin();
    for (; bitx != bloom_idxs_.end(); ++bitx) 
    {
        if (bitx == bloom_idxs_.begin()) 
        {
   	    (*bitx)->need_sync = true;
        } 
        else 
        {
    	    (*bitx)->need_sync = false;
    	}
    }

    return true;
}

bool BloomMgr::AddNewBloom()
{
    time_t curr_time = time(NULL);
    struct tm tmstru;
    localtime_r(&curr_time, &tmstru);
    stringstream fname;    
    fname << tmstru.tm_year + 1900 << tmstru.tm_mon + 1 << tmstru.tm_mday
        << tmstru.tm_hour;
    last_hour_ = fname.str();

    string bfname = prefix_ + "/" + fname.str();
    string biname = prefix_ + "/.idx_" + fname.str();

    MapBloomPtr bloom(new MapBloom);
    if (!bloom) 
    {
        return false;
    }

    if (!bloom->Init(bloom_num_, capacity_, fail_rate_, bfname)) 
    {
        return false;
    }

    BloomIdxPtr bloom_idx(new bloom_index_t);
    if (!bloom_idx) 
    {
        return false;
    }
    bloom_idx->fname = biname;
    bloom_idx->fsize = sizeof(int64_t) 
        + sizeof(bloom_offset_t) * bloom_num_;

    if (!CreateIndex(bloom_idx)) 
    {
        return false;
    }

    if (blooms_.size() > 0) 
    {
        MapBloomPtr newest_bloom = *(blooms_.begin());
        newest_bloom->Sync2File();
        newest_bloom->StopFlush();

        BloomIdxPtr newest_idx = *(bloom_idxs_.begin()); 
        newest_idx->sync2file();
        newest_idx->need_sync = false;
    }

    int64_t bit_num = bloom->GetBitNum();
    string finfo = boost::str(boost::format("%1%\t%2%\t%3%\t%4%\t%5%") 
        %fname.str() %bloom_num_ %capacity_ %fail_rate_ %bit_num);
    {
        blooms_.push_front(bloom);
        bloom_finfos_.push_front(finfo);
        bloom_idxs_.push_front(bloom_idx);
        
        while (bloom_finfos_.size() > days_) 
        {
            MapBloomPtr oldest_bloom = blooms_.back();
            oldest_bloom->SetDelete(true);
            string bloom_name = oldest_bloom->GetFileName();

            bloom_finfos_.pop_back();
            blooms_.pop_back();

            BloomIdxPtr oldest_idx = bloom_idxs_.back();
            oldest_idx->need_del = true;
            bloom_idxs_.pop_back();

            StartDeleteBloomIdx(bloom_name);
        }
    }

    WriteMeta();

    LOG(INFO) << "AddNewBloom\tbloom_name=" << bfname 
        << "\tlast_hour=" << last_hour_;

    return true;
}

bool BloomMgr::CreateIndex(BloomIdxPtr bloom_idx)
{
    bloom_idx->fd = open(bloom_idx->fname.c_str(), O_CREAT | O_RDWR, 0744);
    if (bloom_idx->fd < 0) 
    {
        return false;
    }

    if (-1 == ftruncate(bloom_idx->fd, bloom_idx->fsize)) 
    {
        return false;
    }

    bloom_idx->mptr = (char *)mmap(NULL, bloom_idx->fsize, 
        PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, bloom_idx->fd, 0);
    if (MAP_FAILED == bloom_idx->mptr) 
    {
        return false;
    }

    int64_t valid_idx = 0;
    memcpy(bloom_idx->mptr, &valid_idx, sizeof(int64_t));
    last_idxs_ = 0; 

    return true;
}

bool BloomMgr::LoadIndex(BloomIdxPtr bloom_idx, string &bloom_name, bool rw)
{
    bloom_idx->fd = open(bloom_idx->fname.c_str(), 
        (rw ? O_RDWR : O_RDONLY), 0744);
    if (bloom_idx->fd < 0) 
    {
        return false;
    }

    bloom_idx->mptr = (char *)mmap(NULL, bloom_idx->fsize, 
        (rw ? (PROT_READ | PROT_WRITE) : PROT_READ), 
        MAP_SHARED | MAP_POPULATE, bloom_idx->fd, 0);
    if (MAP_FAILED == bloom_idx->mptr) 
    {
        return false;
    }

    int64_t curr_bloom_num = *(int64_t *)(bloom_idx->mptr);
    char *pIdx = bloom_idx->mptr + sizeof(int64_t);
    for (int i = 0; i < curr_bloom_num; i++) 
    {
        BloomOffsetPtr bloom_offset(new bloom_offset_t);
        strncpy(bloom_offset->uid, pIdx, UID_LEN);
        pIdx += UID_LEN;

        bloom_offset->offset = *(int64_t *)pIdx;
        pIdx += sizeof(int64_t);

        bloom_offset->len = *(int64_t *)pIdx;
        pIdx += sizeof(int64_t);

        bloom_offset->max_adds = *(int64_t *)pIdx;
        pIdx += sizeof(int64_t);

        bloom_offset->adds = *(int64_t *)pIdx; 
        pIdx += sizeof(int64_t);

        if (bloom_offset->len > 0) 
        {
            string uid = string(bloom_offset->uid);
            string key = bloom_name + "_" + uid;
            {
                boost::unique_lock<boost::shared_mutex> lock(mutex_); 
                auto it_off = uid2offset_.find(key);
                if (it_off != uid2offset_.end()) 
                {
                    it_off->second.push_front(bloom_offset);
                } 
                else 
                {
                    list<BloomOffsetPtr> boffset;
                    boffset.push_front(bloom_offset);
                    uid2offset_[key] = boffset;
                }

                auto it_bloom = bloom2uid_.find(bloom_name);
                if (it_bloom != bloom2uid_.end()) 
                {
                    it_bloom->second.push_back(uid); 
                } 
                else 
                {
                    list<string> uids;
                    uids.push_back(uid);
                    bloom2uid_[bloom_name] = uids;
                }
            }
        }
    }

    if (rw) 
    {
        last_idxs_ = curr_bloom_num;
    }

    return true;
}

void BloomMgr::WriteMeta()
{
    string fname = prefix_ + "/.meta";
    fstream fout(fname, ios::binary | ios::out);

    auto ite = bloom_finfos_.begin();
    for (; ite != bloom_finfos_.end(); ++ite) 
    {
        string line = *ite + "\n";
        fout.write(line.c_str(), line.size());  
    }

    fout.close();
}

void BloomMgr::CreateBloomHandle()
{
    while (1) 
    {
    	sleep(2400);

        time_t curr_time = time(NULL);
        struct tm tmstru;
        localtime_r(&curr_time, &tmstru);
        stringstream ss;    
        ss << tmstru.tm_year + 1900 << tmstru.tm_mon + 1 << tmstru.tm_mday 
            << tmstru.tm_hour;
        if (0 == last_hour_.compare(ss.str())) 
        {
            continue;
        }

        if (create_bloom_at_ != tmstru.tm_hour) 
        {
            continue;
        }

        AddNewBloom();
    }
}

void BloomMgr::StartReloadMeta()
{
    reload_meta_thread_.reset(new boost::thread(tr1::bind(
        &BloomMgr::ReloadMetaHandle, this)));
    reload_meta_thread_->detach();
}

void BloomMgr::ReloadMetaHandle()
{
    string fname = prefix_ + "/.meta";
    struct stat sb;
    stat(fname.c_str(), &sb);
    last_mtime_ = sb.st_mtime; 

    while (1) 
    {
        sleep(600);

        stat(fname.c_str(), &sb);
        if (last_mtime_ != sb.st_mtime) 
        {
            ReloadMeta(); 
            last_mtime_ = sb.st_mtime;
        }
    }
}

void BloomMgr::ReloadMeta()
{
    string fname = prefix_ + "/.meta";
    fstream fin(fname, ios::binary | ios::in | ios::out);
    if (!fin.is_open() || !fin.good()) 
    {
        return;
    }

    while (!fin.eof()) 
    {
        string line;
        getline(fin, line);
        if (line.empty()) 
        {
            continue;
        } 

        vector<string> bloom_info;
        boost::split(bloom_info, line, boost::is_any_of("\t"));
        if (META_ITEMS != bloom_info.size()) 
        {
            continue;
        }

        string fname = bloom_info[0];

        MapBloomPtr newest_bloom;
        {
            boost::shared_lock<boost::shared_mutex> lock(mutex_);
            newest_bloom = *(blooms_.begin());
        }

        if (0 == fname.compare(newest_bloom->GetFileName())) 
        {
            break;
        } 

        MapBloomPtr bloom(new MapBloom);
        if (!bloom) 
        {
            break;
        }

        string bfname = prefix_ + "/" + fname;
        int64_t bloom_num = boost::lexical_cast<int64_t>(bloom_info[1]);    
        int64_t capacity = boost::lexical_cast<int64_t>(bloom_info[2]);    
        double fail_rate = boost::lexical_cast<double>(bloom_info[3]);    
        int64_t bit_num = boost::lexical_cast<int64_t>(bloom_info[4]);    

        if (!bloom->Init(bloom_num, capacity, fail_rate, bfname, bit_num)) 
        {
            break;
        }

        BloomIdxPtr bloom_idx(new bloom_index_t);
        if (!bloom_idx) 
        {
            break;
        }
        bloom_idx->fname = prefix_ + "/.idx_" + fname;
        bloom_idx->fsize = sizeof(int64_t) 
            + sizeof(bloom_offset_t) * bloom_num_;

        if (!LoadIndex(bloom_idx, fname)) 
        {
            break;
        }

        boost::unique_lock<boost::shared_mutex> lock(mutex_); 
        {
            blooms_.push_front(bloom);
            bloom_idxs_.push_front(bloom_idx);
            if (blooms_.size() > days_) 
            {
                MapBloomPtr oldest_bloom = blooms_.back();
                oldest_bloom->SetDelete(true);
                string bloom_name = oldest_bloom->GetFileName();

                blooms_.pop_back();

                BloomIdxPtr oldest_idx = bloom_idxs_.back();
                oldest_idx->need_del = true;
                bloom_idxs_.pop_back();

                StartDeleteBloomIdx(bloom_name);
            }
        }

        LOG(INFO) << "ReloadMeta\tbloom_name=" << bfname 
            << "\toffset_num=" << uid2offset_.size()
            << "\tbloom_num=" << bloom2uid_[fname].size()
            << "\tlast_idx=" << last_idxs_;

        break;
    }

    fin.close();
}

bool BloomMgr::Add(ContextPtr ctx)
{
    SyncBloomIndex();

    string key;
    string bloom_name;
    bool new_bloom = false;
    char *pIdx = NULL;
    int64_t valid_idx = 0;
    int64_t bloom_size = 0;
    int vid_num = ctx->finfo_.vid_size;

    BloomOffsetPtr newest_offset;
    MapBloomPtr newest_bloom; 
    BloomIdxPtr newest_idx;
    {
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        newest_bloom = *(blooms_.begin());
        newest_idx = *(bloom_idxs_.begin());

        bloom_size = newest_bloom->GetBitNum() / 8;
        bloom_name = newest_bloom->GetFileName();
        key = bloom_name + "_" + ctx->uid_;
        auto it = uid2offset_.find(key);
        if (it != uid2offset_.end()) 
        {
            newest_offset = *(it->second.begin());
            if ((newest_offset->adds + vid_num) > max_adds_) 
            {
                valid_idx = newest_offset->offset / bloom_size;
                pIdx = newest_idx->mptr + (sizeof(int64_t) 
                    + sizeof(bloom_offset_t) * valid_idx);

                memcpy(pIdx + UID_LEN + sizeof(int64_t) * 3, &max_adds_, 
                    sizeof(int64_t));
                newest_offset->adds = max_adds_;

                new_bloom = true;
            }
        } 
        else 
        {
            new_bloom = true;
        }
    }

    if (new_bloom) 
    {
        ScopedLock plock(proc_mutex_);
        {
            valid_idx = *(int64_t *)(newest_idx->mptr);
            valid_idx += 1;
            if (valid_idx > bloom_num_) 
            {
                ctx->err_ = eForbid;

                LOG(ERROR) << "bloom_overflow"
                    << "\tbloom_num=" << bloom_num_ << "\tuid=" << ctx->uid_
                    << "\tsid=" << ctx->sid_;

                return false;
            }

            memcpy(newest_idx->mptr, &valid_idx, sizeof(int64_t));
        }

        pIdx = newest_idx->mptr + (sizeof(int64_t) 
            + sizeof(bloom_offset_t) * (valid_idx - 1));

        newest_offset.reset(new bloom_offset_t);
        strncpy(newest_offset->uid, ctx->uid_.c_str(), ctx->uid_.size());
        memcpy(pIdx, newest_offset->uid, UID_LEN);
        pIdx += UID_LEN;

        newest_offset->offset = bloom_size * (valid_idx - 1);
        memcpy(pIdx, &newest_offset->offset, sizeof(int64_t));
        pIdx += sizeof(int64_t);

        newest_offset->len = bloom_size;
        memcpy(pIdx, &newest_offset->len, sizeof(int64_t));
        pIdx += sizeof(int64_t);

        newest_offset->max_adds = max_adds_;
        memcpy(pIdx, &newest_offset->max_adds, sizeof(int64_t));
        pIdx += sizeof(int64_t);

        newest_offset->adds = vid_num; 
        memcpy(pIdx, &newest_offset->adds, sizeof(int64_t));
        pIdx += sizeof(int64_t);

        boost::unique_lock<boost::shared_mutex> lock(mutex_); 
        {
            last_idxs_ = valid_idx;

            auto it_off = uid2offset_.find(key);
            if (it_off != uid2offset_.end()) 
            {
                it_off->second.push_front(newest_offset);
            } 
            else 
            {
                list<BloomOffsetPtr> boffset;
                boffset.push_front(newest_offset);
                uid2offset_[key] = boffset;
            }

            auto it_bloom = bloom2uid_.find(bloom_name);
            if (it_bloom != bloom2uid_.end()) 
            {
                it_bloom->second.push_back(ctx->uid_); 
            } 
            else 
            {
                list<string> uids;
                uids.push_back(ctx->uid_);
                bloom2uid_[bloom_name] = uids;
            }
        }
    } 
    else 
    {
        valid_idx = newest_offset->offset / bloom_size;
        pIdx = newest_idx->mptr + (sizeof(int64_t) 
            + sizeof(bloom_offset_t) * valid_idx);

        int64_t off = UID_LEN + sizeof(int64_t) * 3;
        int64_t adds = *(int64_t *)(pIdx + off);
        adds += vid_num;
        memcpy(pIdx + off, &adds, sizeof(int64_t));
        newest_offset->adds = adds;
    }

    vector<int64_t> hashs;
    hashs.reserve(8);
    for (int i = 0; i < ctx->finfo_.vids.size(); i++) 
    {
        list<string> lv = ctx->finfo_.vids[i];
        auto itl = lv.begin();
        for (int j = 0; itl != lv.end(); ++itl, j++) 
        {
            CalcHash(*itl, hashs);
            newest_bloom->Add(newest_offset->offset, hashs);
            hashs.clear();
            ctx->add_vids_ << *itl; 
            if (j < lv.size() - 1)
            {
                ctx->add_vids_ << ",";
            }
        }

        if (i < ctx->finfo_.vids.size() - 1)
        {
            ctx->add_vids_ << "|";
        }
    }

    return true;
}

void BloomMgr::CalcHash(string &vid, vector<int64_t> &hashs)
{
    for (auto h : hash_handles_) 
    {
        hashs.push_back(h(vid));
    }
}

void BloomMgr::Get(ContextPtr ctx)
{
    SyncBloomIndex();

    auto &finfo = ctx->finfo_;
    auto &res_infos = finfo.res_infos;
    auto &filtered_vids = ctx->filtered_vids_;
    if (filtered_vids.str().size() > 0)
    {
        filtered_vids << "_";
    }
    int days = ctx->days_ < 0 ? days_ : ctx->days_;

    for (int i = 0; i < finfo.vids.size(); i++) 
    {
        ResInfo res_info;
        list<string> lv = finfo.vids[i];
        auto itl = lv.begin();
        for (int j = 0; itl != lv.end(); ++itl, j++)
        {
            if (!(*itl).empty()) 
            {
                if (Lookup(ctx->uid_, *itl, days)) 
                {
                    filtered_vids << *itl;
                    if (j < lv.size() - 1)
                    {
                        filtered_vids << ",";
                    }
                } 
                else 
                {
                    res_info.vids.push_back(*itl);
                }
            }
        }

        if (res_infos.size() > i) 
        { 
            res_infos[i].push_back(res_info);  
        } 
        else 
        {
            vector<ResInfo> res;
            res.push_back(res_info);
            res_infos[i] = res;
        }

        if (i < finfo.vids.size() - 1)
        {
            filtered_vids << "|";
        }
    }
}

bool BloomMgr::Lookup(string &uid, string &vid, int days)
{
    vector<int64_t> hashs;
    hashs.reserve(8);
    CalcHash(vid, hashs);
    bool found = false;
    bool ret = false;
    
    boost::shared_lock<boost::shared_mutex> lock(mutex_);

    auto it = blooms_.begin();
    for (int i = 0; (it != blooms_.end() && !found && i < days); ++it, i++) 
    {
        string key = (*it)->GetFileName() + "_" + uid;
        auto it_off_m = uid2offset_.find(key);
        if (it_off_m == uid2offset_.end()) 
        {
            continue;
        }

        auto it_off_l = it_off_m->second.begin();
        for (; it_off_l != it_off_m->second.end() && !found; ++it_off_l) 
        {
            ret = (*it)->Lookup((*it_off_l)->offset, hashs);
            found = found || ret;
        }
    }

    return found;
}

void BloomMgr::GetBloom(ContextPtr ctx)
{
    SyncBloomIndex();

    // type,ts,bits,len
    int head_sz = sizeof(int32_t) + BLOOM_NAME_SZ + sizeof(int64_t) * 2;
    int32_t bloom_num = 0;
    char *last_bloom_ptr = NULL;
    int last_bloom_len = 0;

    boost::shared_lock<boost::shared_mutex> lock(mutex_);

    for (auto &b : blooms_) 
    {
        string bloom_name = b->GetFileName();
        if (0 == bloom_name.compare(ctx->ts_)) 
        {
            break;
        }

        string key = bloom_name + "_" + ctx->uid_;
        auto off = uid2offset_.find(key);
        if (off == uid2offset_.end()) 
        {
            continue;
        }

        char *last_ptr = NULL;
        int last_len = 0;

        for (auto &o : off->second) 
        {
            bloom_num++;

            int offset = 0;
            int len = head_sz + o->len;
            char *ptr = (char *)calloc(1, len);

            memcpy(ptr + offset, &type_, sizeof(int32_t));
            offset += sizeof(int32_t);

            strncpy(ptr + offset, bloom_name.c_str(), bloom_name.size());
            offset += BLOOM_NAME_SZ;

            int64_t bit_num = b->GetBitNum();
            memcpy(ptr + offset, &bit_num, sizeof(int64_t));
            offset += sizeof(int64_t);

            memcpy(ptr + offset, &o->len, sizeof(int64_t));
            offset += sizeof(int64_t);

            char *mptr = b->GetMapPtr();
            memcpy(ptr + offset, mptr + o->offset, o->len);
            offset += o->len;

            if (NULL == last_ptr && 0 == last_len) 
            {
                last_ptr = ptr;
                last_len = len;
            } 
            else 
            {
                int new_len = last_len + len;
                char *new_ptr = (char *)calloc(1, new_len);
                memcpy(new_ptr, last_ptr, last_len);
                memcpy(new_ptr + last_len, ptr, len);
                free(ptr);
                free(last_ptr);
                last_ptr = new_ptr;
                last_len = new_len;
            }
        }

        if (NULL == last_bloom_ptr && 0 == last_bloom_len) 
        {
            last_bloom_ptr = last_ptr;
            last_bloom_len = last_len;
        } 
        else 
        {
            int new_bloom_len = last_bloom_len + last_len;
            char *new_bloom_ptr = (char *)calloc(1, new_bloom_len);
            memcpy(new_bloom_ptr, last_bloom_ptr, last_bloom_len);
            memcpy(new_bloom_ptr + last_bloom_len, last_ptr, last_len);
            free(last_ptr);
            free(last_bloom_ptr);
            last_bloom_ptr = new_bloom_ptr;
            last_bloom_len = new_bloom_len;
        }
    }

    if (NULL == ctx->total_ptr_ && 0 == ctx->total_len_) 
    {
        int total_len = sizeof(int32_t) + last_bloom_len;
        char *total_ptr = (char *)calloc(1, total_len);

        memcpy(total_ptr, &bloom_num, sizeof(int32_t));

        if (last_bloom_len > 0) 
        {
            memcpy(total_ptr + sizeof(int32_t), 
                last_bloom_ptr, last_bloom_len);
            free(last_bloom_ptr);
        }

        ctx->total_ptr_ = total_ptr;
        ctx->total_len_ = total_len; 
        ctx->blooms_ = string(ctx->total_ptr_, ctx->total_len_);
    } 
    else 
    {
        if (last_bloom_len > 0) 
        {
            int32_t tmp = *(int32_t *)ctx->total_ptr_;
            tmp += bloom_num;
            memcpy(ctx->total_ptr_, &tmp, sizeof(int32_t));

            int total_len = ctx->total_len_ + last_bloom_len;
            char *total_ptr = (char *)calloc(1, total_len);
            memcpy(total_ptr, ctx->total_ptr_, ctx->total_len_);
            memcpy(total_ptr + ctx->total_len_, 
                last_bloom_ptr, last_bloom_len);
            free(last_bloom_ptr);
            free(ctx->total_ptr_);
            ctx->total_ptr_ = total_ptr;
            ctx->total_len_ = total_len; 
            ctx->blooms_ = string(ctx->total_ptr_, ctx->total_len_);
        }
    }
}

void BloomMgr::Sync2File()
{
    SyncBloomIndex();

    MapBloomPtr newest_bloom; 
    BloomIdxPtr newest_idx;
    {
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        newest_bloom = *(blooms_.begin());
        newest_idx = *(bloom_idxs_.begin());
    }

    newest_bloom->Sync2File();
    newest_idx->sync2file();
}

void BloomMgr::SyncBloomIndex()
{
    int64_t new_idx = 0;
    int64_t old_idx = 0;

    MapBloomPtr newest_bloom; 
    BloomIdxPtr newest_idx;
    {
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        newest_bloom = *(blooms_.begin());
        newest_idx = *(bloom_idxs_.begin());

        int64_t curr_idx = *(int64_t *)(newest_idx->mptr);
        if (curr_idx == last_idxs_) 
        {
            return;
        }

        new_idx = curr_idx - last_idxs_;
        old_idx = last_idxs_;
        last_idxs_ = curr_idx;
    }

    if (new_idx > 0) 
    {
        string bloom_name = newest_bloom->GetFileName();
        char *pIdx = newest_idx->mptr + (sizeof(int64_t) 
            + sizeof(bloom_offset_t) * old_idx);

        for (int i = 0; i < new_idx; i++) 
        {
            BloomOffsetPtr bloom_offset(new bloom_offset_t);
            strncpy(bloom_offset->uid, pIdx, UID_LEN);
            pIdx += UID_LEN;

            bloom_offset->offset = *(int64_t *)pIdx;
            pIdx += sizeof(int64_t);

            bloom_offset->len = *(int64_t *)pIdx;
            pIdx += sizeof(int64_t);

            bloom_offset->max_adds = *(int64_t *)pIdx;
            pIdx += sizeof(int64_t);

            bloom_offset->adds = *(int64_t *)pIdx; 
            pIdx += sizeof(int64_t);

            if (bloom_offset->len > 0) 
            {
                string uid = string(bloom_offset->uid);
                string key = bloom_name + "_" + uid;
                {
                    boost::unique_lock<boost::shared_mutex> lock(mutex_); 
                    auto it_off = uid2offset_.find(key);
                    if (it_off != uid2offset_.end()) 
                    {
                        it_off->second.push_front(bloom_offset);
                    } 
                    else 
                    {
                        list<BloomOffsetPtr> boffset;
                        boffset.push_front(bloom_offset);
                        uid2offset_[key] = boffset;
                    }

                    auto it_bloom = bloom2uid_.find(bloom_name);
                    if (it_bloom != bloom2uid_.end()) 
                    {
                        it_bloom->second.push_back(uid); 
                    } 
                    else 
                    {
                        list<string> uids;
                        uids.push_back(uid);
                        bloom2uid_[bloom_name] = uids;
                    }
                }

                //LOG(INFO) << "SyncBloomIndex\tbloom_name=" << bloom_name 
                //    << "\tlast_idxs=" << last_idxs_ << "\toffset_num=" 
                //    << uid2offset_.size() << "\tkey=" << key;
            }
        }
    }
}

void BloomMgr::StartDeleteBloomIdx(string &bloom_name)
{
    boost::shared_ptr<boost::thread> delete_idx_thread;
    delete_idx_thread.reset(new boost::thread(tr1::bind(
        &BloomMgr::DeleteBloomIdxHandle, this, bloom_name)));
    delete_idx_thread->detach();
}

void BloomMgr::DeleteBloomIdxHandle(string &bloom_name)
{
    list<string> uids;
    {
        boost::unique_lock<boost::shared_mutex> lock(mutex_); 
        auto it = bloom2uid_.find(bloom_name);
        if (it == bloom2uid_.end()) 
        {
            return;
        }
        uids = it->second;
        bloom2uid_.erase(it);
    }

    struct timeval tv; 
    gettimeofday(&tv, NULL); 
    int64_t start = tv.tv_sec * 1000000 + tv.tv_usec; 

    auto it = uids.begin();
    for (; it != uids.end(); ++it) 
    {
        string key = bloom_name + "_" + *it;
        {
            boost::unique_lock<boost::shared_mutex> lock(mutex_); 
            auto it_off = uid2offset_.find(key);
            if (it_off != uid2offset_.end()) 
            {
                it_off->second.clear();
                uid2offset_.erase(it_off);
            }
        }
    }

    gettimeofday(&tv, NULL);
    int64_t end =  tv.tv_sec * 1000000 + tv.tv_usec;

    LOG(INFO) << "DeleteBloomIdx\tbloom_name=" << bloom_name 
        << "\tbloom_num=" << uids.size() << "\ttype=" << type_
        << "\tcost=" << double(end - start) / 1000 << "ms";
}

NAME_SPACE_ES
