#include "mod_filter.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <sstream>
#include "comm/logging.h"
#include "comm/thread_key.h"
#include "output.h" 
#include "context.h"
#include "util.h"

LOG_NAME("Filter");

NAME_SPACE_BS

FilterModule::FilterModule()
{
    filter_show_ = NULL;
}

FilterModule::~FilterModule()
{
    if (NULL != filter_show_) 
    {
        delete filter_show_;
        filter_show_ = NULL;
    }
}

void FilterModule::LogHandler(const char* category,
    int level, const char* msg)
{
    if (NULL == category)
    {
        return;
    }

    NLOG(category, level) << msg;
}

bool FilterModule::InitInMaster(const std::string& conf)
{
    cfg_.reset(new shs_conf::Config);
    cfg_engines_.reset(new shs_conf::ConfigEngines);

    if (!cfg_->Init(conf)) 
    {
        cout << "cfg_->Init() failed, conf = " << conf << endl;

        return false;
    }
    
    string lf = cfg_->Section("settings")->Option()->GetStr("logging_conf");
    LOG_INIT(lf);

    if (!cfg_engines_->Init(cfg_)) 
    {
        LOG(ERROR) << "cfg_engines_->Init() failed";

        return false;
    }

    string cfg_output;
    cfg_->PrintToString(&cfg_output);
    LOG(INFO) << cfg_output;

    output.SetLevel(INFO);
    output.SetOutputFunction(FilterModule::LogHandler);

    if (!InitBloomMgr()) 
    {
        LOG(ERROR) << "InitBloomMgr() failed";

        return false;
    }

    LOG(INFO) << "InitInMaster done...";

    return true;
}

bool FilterModule::InitInWorker(int *thread_num)
{
    *thread_num = cfg_->section("settings")->GetInt("workers", 0, 24);

    if (0 != thread_key_create()) 
    {
        LOG(ERROR) << "thread_key_create() failed";

        return false;
    }
    
    Register("filter", std::tr1::bind(&FilterModule::Filter, this, 
        std::tr1::placeholders::_1, std::tr1::placeholders::_2, 
        std::tr1::placeholders::_3));
    Register("get", std::tr1::bind(&FilterModule::Get, this, 
        std::tr1::placeholders::_1, std::tr1::placeholders::_2, 
        std::tr1::placeholders::_3));
    Register("sync", std::tr1::bind(&FilterModule::Sync, this, 
        std::tr1::placeholders::_1, std::tr1::placeholders::_2, 
        std::tr1::placeholders::_3));

    show_bloom_mgr_->StartReloadMeta();

    filter_show_ = new FilterShow(show_bloom_mgr_);
    if (NULL == filter_show_) 
    {
        LOG(ERROR) << "new FilterShow() failed";

        return false;
    }

    LOG(INFO) << "InitInWorker done, workers = " << *thread_num;

    return true;
}

void FilterModule::Filter(const map<string, string>& params, 
    const InvokeCompleteHandler& cb,
    boost::shared_ptr<InvokeParams> invoke_params)
{
    ContextPtr ctx(new Context);
    ctx->params_ = params;
    ctx->cb_ = cb;
    ctx->ar_que_t_ = (invoke_params->get_enqueue_time() 
        - invoke_params->get_request_time()) / 1000;
    ctx->in_que_t_ = (invoke_params->get_dequeue_time() 
        - invoke_params->get_enqueue_time()) / 1000;

    filter_show_->StartFilter(ctx);
}

void FilterModule::Get(const map<string, string>& params, 
    const InvokeCompleteHandler& cb,
    boost::shared_ptr<InvokeParams> invoke_params)
{
    ContextPtr ctx(new Context);
    ctx->params_ = params;
    ctx->cb_ = cb;
    ctx->ar_que_t_ = (invoke_params->get_enqueue_time() 
        - invoke_params->get_request_time()) / 1000;
    ctx->in_que_t_ = (invoke_params->get_dequeue_time() 
        - invoke_params->get_enqueue_time()) / 1000;

    filter_show_->GetBloom(ctx);
}

void FilterModule::Sync(const map<string, string>& params, 
    const InvokeCompleteHandler& cb,
    boost::shared_ptr<InvokeParams> invoke_params)
{
    LOG(INFO) << "Got sync request...";

    show_bloom_mgr_->Sync2File();

    map<string, string> res;
    res["result"] = "sync done";
    InvokeResult result;
    result.set_results(res);
    cb(result);
}

bool FilterModule::InitBloomMgr()
{
    return InitShowBloomMgr();
}

bool FilterModule::InitShowBloomMgr()
{
    boost::shared_ptr<shs_conf::ConfigEngine> eng = 
        cfg_engines_->engine("show_bloom");
    if (NULL != eng && eng->enabled()) 
    {
        string prefix = eng->GetStr("prefix");
        int bloom_num = eng->GetInt("bloom_num");
        int capacity = eng->GetInt("capacity");
        double fail_rate = eng->GetNum("fail_rate");
        int days = eng->GetInt("days");
        int create_bloom_at = eng->GetInt("create_bloom_at");
            
        show_bloom_mgr_.reset(new BloomMgr(prefix, bloom_num, capacity, 
            fail_rate, days, create_bloom_at, TYPE_SHOW));

        return show_bloom_mgr_->InitBlooms();
    }

    return true;
}

NAME_SPACE_ES
EXPORT_MODULE(srec::FilterModule)

