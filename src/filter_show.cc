#include "filter_show.h"

NAME_SPACE_BS

FilterShow::FilterShow(BloomMgrPtr bloom_mgr)
{
    bloom_mgr_ = bloom_mgr;
}

FilterShow::~FilterShow()
{
}

void FilterShow::StartFilter(ContextPtr ctx)
{
    CheckFilterInfo(ctx);

    if (eOk == ctx->err_ && tAdd == ctx->finfo_.type) 
    {
        StartAdd(ctx);
        ResponseAddAck(ctx);
    } 
    else if (eOk == ctx->err_ && tGet == ctx->finfo_.type) 
    {
        StartGet(ctx);
        ResponseGetAck(ctx);
    } 
    else 
    {
        ResponseAddAck(ctx);
    }
}

void FilterShow::GetBloom(ContextPtr ctx)
{
    CheckGetBloomInfo(ctx);

    if (eOk == ctx->err_) 
    {
        StartGetBloom(ctx);
    }

    DoAck(ctx, "GET");
}

void FilterShow::ResponseAddAck(ContextPtr ctx)
{
    stringstream ss;
    ss << ctx->err_;
    ctx->resp_ = ss.str();

    DoAck(ctx);
}

void FilterShow::ResponseGetAck(ContextPtr ctx)
{
    ctx->timers_.Timer("pkg")->Start();

    auto &finfo = ctx->finfo_;
    auto &res_infos = finfo.res_infos; 
    vector<string> pass_vec;
    stringstream ss;

    for (int i = 0; i < finfo.req_group_size; i++) 
    {
        ss << "group" << i << ":";

        vector<ResInfo> res = res_infos[i];
        for (int j = 0; j < res.size(); j++) 
        {
            for (int x = 0; x < res[j].vids.size(); x++) 
            {
                auto it = find(pass_vec.begin(), 
                    pass_vec.end(), res[j].vids[x]);
                if (it == pass_vec.end()) 
                {
                    ss << res[j].vids[x];
                    if (x < res[j].vids.size() - 1)
                    {
                        ss << ",";
                    }
                    pass_vec.push_back(res[j].vids[x]);
                }
            }
        }

        if (i < finfo.req_group_size - 1)
        {
            ss << "\n";
        }
    }

    ctx->resp_ = ss.str();

    ctx->timers_.Timer("pkg")->Stop();

    DoAck(ctx);
}

NAME_SPACE_ES
