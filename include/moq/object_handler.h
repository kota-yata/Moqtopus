#pragma once

#include "moq/errors.h"
#include "moq/types.h"

namespace moq {

class ObjectHandler {
public:
    virtual ~ObjectHandler() = default;

    virtual void on_object(Object object) = 0;
    virtual void on_publish_done(PublishDone done) = 0;
    virtual void on_error(ReceiveError error) = 0;
};

} // namespace moq
