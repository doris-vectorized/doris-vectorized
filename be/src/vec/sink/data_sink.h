#pragma once
#include "exec/data_sink.h"
#include "vec/core/block.h"

namespace doris {
namespace vectorized {
class VDataSink : public DataSink {
public:
    virtual Status send(RuntimeState* state, Block* block) = 0;
};
} // namespace vectorized
} // namespace doris
