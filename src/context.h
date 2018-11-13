#ifndef CONTEXT_H
#define CONTEXT_H

#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <list>
#include "common.h"
#include "comm/timer.h"
#include "http_invoke_params.h"
#include "util.h"

using namespace std;
using namespace shs;

NAME_SPACE_BS

enum FilterType 
{
    tGet,
    tAdd,
    tNone
};

typedef struct _ResInfo 
{
    vector<string> vids;
} ResInfo;

typedef struct _FilterInfo 
{
    _FilterInfo() 
    {
        req_group_size = 0;
        vid_size = 0;
    }

    FilterType type;
    uint32_t req_group_size;
    uint32_t vid_size;
    vector<list<string> > vids;
    map<int, vector<ResInfo> > res_infos;
} FilterInfo;

class Context 
{
public:
    Context();
    ~Context();

public:
    Errno err_; 
    string uid_;
    string sid_;
    string ts_;
    string blooms_;
    string resp_;

    int days_;
    double ar_que_t_;
    double in_que_t_;

    int total_len_;
    char *total_ptr_;

    stringstream add_vids_;
    stringstream filtered_vids_;
    map<string, string> params_;

    FilterInfo finfo_;

    QTimerFactory timers_;
    shs::InvokeCompleteHandler cb_;
};

typedef boost::shared_ptr<Context> ContextPtr;

NAME_SPACE_ES

#endif

