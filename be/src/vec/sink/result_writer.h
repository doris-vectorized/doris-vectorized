#pragma once
#include "runtime/result_writer.h"
namespace doris {
namespace vectorized {
class VResultWriter: public ResultWriter {
public:
    VResultWriter(bool is_vec):ResultWriter(is_vec) {}

    virtual Status append_block(Block& block) = 0;
};
}
}