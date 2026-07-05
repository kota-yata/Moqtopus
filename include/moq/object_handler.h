#pragma once

#include "moq/errors.h"
#include "moq/types.h"

namespace moq {

class ObjectHandler {
public:
  virtual ~ObjectHandler() = default;

  // Called on the transport thread. object.payload / object.properties are
  // views into receive buffers and expire when this call returns.
  virtual void on_object(const Object &object) = 0;
  virtual void on_publish_done(PublishDone done) = 0;
  virtual void on_error(ReceiveError error) = 0;
};

} // namespace moq
