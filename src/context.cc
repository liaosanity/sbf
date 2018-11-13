#include "context.h"

NAME_SPACE_BS

Context::Context()
{
    days_ = -1;
    total_len_ = 0;
    total_ptr_ = NULL;
}

Context::~Context()
{
    if (NULL != total_ptr_) 
    {
        free(total_ptr_);
        total_ptr_ = NULL;
        total_len_ = 0;
    }
}

NAME_SPACE_ES
