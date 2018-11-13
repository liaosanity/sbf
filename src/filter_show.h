#ifndef FILTER_SHOW_H
#define FILTER_SHOW_H

#include "filter.h"

NAME_SPACE_BS

class FilterShow : public Filter 
{
public:
    FilterShow(BloomMgrPtr bloom_mgr);
    virtual ~FilterShow();

    void StartFilter(ContextPtr ctx);
    void GetBloom(ContextPtr ctx);

private:
    void ResponseAddAck(ContextPtr ctx);
    void ResponseGetAck(ContextPtr ctx);
};

NAME_SPACE_ES

#endif

