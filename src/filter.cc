#include "filter.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>  
#include "comm/logging.h"
#include "util.h"

using namespace shs;

LOG_NAME("Filter");

NAME_SPACE_BS

Filter::Filter()
{
}

Filter::~Filter()
{
}

void Filter::CheckFilterInfo(ContextPtr ctx)
{
    ctx->timers_.Timer("total")->Start();
    ctx->err_ = eOk;

    ctx->uid_ = get_param(ctx->params_, "uid"); 
    ctx->sid_ = get_param(ctx->params_, "sid"); 

    string day = get_param(ctx->params_, "day");
    if ("" != day) 
    {
        ctx->days_ = atoi(day.c_str()); 
    }

    if ("" == ctx->uid_ || ctx->uid_.empty()) 
    {
        ctx->err_ = eUidEmpty;

        return;
    }

    if ("" == ctx->sid_ || ctx->sid_.empty()) 
    {
        ctx->err_ = eSidEmpty;

        return;
    }

    string action = get_param(ctx->params_, "action");
    if ("" == action || action.empty()) 
    {
        ctx->err_ = eAction;

        return;
    }

    if ("0" == action) 
    {
        ctx->finfo_.type = tGet;
    } 
    else if ("1" == action) 
    {
        ctx->finfo_.type = tAdd;
    }
    else 
    {
        ctx->finfo_.type = tNone;
        ctx->err_ = eAction;

        return;
    }

    string vid = get_param(ctx->params_, "vids");
    if ("" == vid || vid.empty()) 
    {
        ctx->err_ = eVidEmpty;

        return;
    }

    vector<string> vids;
    boost::split(vids, vid, boost::is_any_of("|"));
    if (0 == vids.size()) 
    {
        ctx->err_ = eVidEmpty;

        return;
    }

    int vEmpty = 0;
    ctx->finfo_.req_group_size = vids.size();
    for (auto &v : vids)
    {
        vector<string> vv;
        boost::split(vv, v, boost::is_any_of(","));
        list<string> lv;
        for (auto &vvv : vv)
        {
            lv.push_back(vvv);            
            ctx->finfo_.vid_size++;
        }

        if (lv.size() > 0)
        {
            ctx->finfo_.vids.push_back(lv);
        }
        else
        {
            vEmpty++;
        }
    }

    if (ctx->finfo_.req_group_size == vEmpty) 
    {
        ctx->err_ = eVidEmpty;

        return;
    }
}

void Filter::CheckGetBloomInfo(ContextPtr ctx)
{
    ctx->timers_.Timer("total")->Start();
    ctx->err_ = eOk;
    ctx->finfo_.type = tNone;

    ctx->uid_ = get_param(ctx->params_, "uid"); 
    ctx->sid_ = get_param(ctx->params_, "sid"); 
    ctx->ts_ = get_param(ctx->params_, "ts");

    if ("" == ctx->uid_ || ctx->uid_.empty()) 
    {
        ctx->err_ = eUidEmpty;

        return;
    }

    if ("" == ctx->sid_ || ctx->sid_.empty()) 
    {
        ctx->err_ = eSidEmpty;

        return;
    }

    if ("" == ctx->ts_ || ctx->ts_.empty()) 
    {
        ctx->err_ = eTsEmpty;

        return;
    }
}

void Filter::StartAdd(ContextPtr ctx)
{
    ctx->timers_.Timer("add")->Start();

    bloom_mgr_->Add(ctx);

    ctx->timers_.Timer("add")->Stop();
}

void Filter::StartGet(ContextPtr ctx)
{
    ctx->timers_.Timer("get")->Start();

    bloom_mgr_->Get(ctx);

    ctx->timers_.Timer("get")->Stop();
}

void Filter::StartGetBloom(ContextPtr ctx)
{
    ctx->timers_.Timer("get")->Start();

    bloom_mgr_->GetBloom(ctx);

    ctx->timers_.Timer("get")->Stop();
}

void Filter::DoAck(ContextPtr ctx, const string& type)
{
    InvokeResult result;
    map<string, string> resp;                                                                                                                                          

    if ("" == type) 
    {                                                          
        resp["result"] = ctx->resp_;
    } 
    else if ("GET" == type) 
    {
        if (eOk != ctx->err_) 
        {
            stringstream ss;
            ss << "error:" << ctx->err_;
            resp["result"] = ss.str();
        } 
        else 
        {
            resp["result"] = ctx->blooms_;
        }
    }

    result.set_results(resp);
    ctx->cb_(result);

    Logging(ctx);
}

void Filter::Logging(ContextPtr ctx)
{
    ctx->timers_.Timer("total")->Stop();                                        

    LOG(INFO) << "\terr=" << ctx->err_
        << "\tuid=" << ctx->uid_
        << "\tsid=" << ctx->sid_
        << "\taction=" << ctx->finfo_.type
        << "\tday=" << ctx->days_
        << "\tts=" << ctx->ts_
        << "\tblooms_sz=" << ctx->blooms_.size()
        << "\treq_group=" << ctx->finfo_.req_group_size
        << "\tar_que_t=" << ctx->ar_que_t_
        << "\tin_que_t=" << ctx->in_que_t_
        << "\tall_t=" << ctx->timers_.Timer("total")->Elapsed() * 1000
        << "\tadd_t=" << ((tAdd == ctx->finfo_.type) ? (ctx->timers_.Timer("add")->Elapsed() * 1000) : 0)
        << "\tget_t=" << ((tGet == ctx->finfo_.type) ? (ctx->timers_.Timer("get")->Elapsed() * 1000) : 0)
        << "\tpkg_t=" << ((tGet == ctx->finfo_.type) ? (ctx->timers_.Timer("pkg")->Elapsed() * 1000) : 0)
        << "\tadd_vid=" << ctx->add_vids_.str()
        << "\tfiltered=" << ctx->filtered_vids_.str();
}

NAME_SPACE_ES
