#ifndef FILTER_H
#define FILTER_H

#include "common.h"
#include "context.h"
#include "bloom_mgr.h"

NAME_SPACE_BS

class Filter
{
public:
    Filter();
    virtual ~Filter();

    virtual void StartFilter(ContextPtr ctx) = 0;

    void CheckFilterInfo(ContextPtr ctx);
    void CheckGetBloomInfo(ContextPtr ctx);
    void StartAdd(ContextPtr ctx);
    void StartGet(ContextPtr ctx);
    void StartGetBloom(ContextPtr ctx);
    void DoAck(ContextPtr ctx, const string& type = "");
    void Logging(ContextPtr ctx);

protected:
    BloomMgrPtr bloom_mgr_;
};

typedef boost::shared_ptr<Filter> FilterPtr;

NAME_SPACE_ES

#endif

