#ifndef MOD_FILTER_H
#define MOD_FILTER_H

#include "common.h"
#include "module.h"
#include "http_invoke_params.h"
#include "comm/config_engine.h"
#include "bloom_mgr.h"
#include "filter.h"
#include "filter_show.h"

using namespace shs;
using namespace std;
using namespace boost;

NAME_SPACE_BS

class FilterModule : public shs::Module
{
public:
    FilterModule();
    ~FilterModule();

    static void LogHandler(const char* category, int level, const char* msg);
    
    bool InitInMaster(const std::string& conf);
    bool InitInWorker(int *thread_num);
    
    void Filter(const std::map<std::string, std::string>& params, 
        const shs::InvokeCompleteHandler& cb,
        boost::shared_ptr<InvokeParams> invoke_params);
    void Get(const std::map<std::string, std::string>& params, 
        const shs::InvokeCompleteHandler& cb,
        boost::shared_ptr<InvokeParams> invoke_params);
    void Sync(const std::map<std::string, std::string>& params, 
        const shs::InvokeCompleteHandler& cb,
        boost::shared_ptr<InvokeParams> invoke_params);

private:
    bool InitBloomMgr();
    bool InitShowBloomMgr();

private:
    boost::shared_ptr<shs_conf::Config> cfg_;
    boost::shared_ptr<shs_conf::ConfigEngines> cfg_engines_;

    BloomMgrPtr show_bloom_mgr_;
    FilterShow *filter_show_;
};

NAME_SPACE_ES

#endif

