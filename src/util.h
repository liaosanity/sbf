#ifndef UTIL_H
#define UTIL_H

#include "common.h"

using namespace std;

NAME_SPACE_BS

enum Errno 
{
    eOk,
    eParam,
    eParse,
    eAction,
    eUidEmpty,
    eSidEmpty,
    eVidEmpty,
    eTsEmpty,
    eForbid,
    eDownRequest,
    eDownRspEmpty,
    eNoResult
};

string get_param(const map<string, string> &params,
    const string &key, string defalut_value = "");

NAME_SPACE_ES

#endif

