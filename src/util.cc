#include "util.h"

NAME_SPACE_BS

string get_param(const map<string, string> &params,
    const string &key, string default_value)
{
    auto ite = params.find(key);

    return ite != params.end() ? ite->second : default_value;
}

NAME_SPACE_ES

