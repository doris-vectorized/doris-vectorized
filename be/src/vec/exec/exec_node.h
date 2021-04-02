#pragma once
#include "exec/exec_node.h"
#include "vec/core/block.h"

namespace doris {
class RowBatch;
class RuntimeState;
namespace vectorized {
class VExecNode : public ::doris::ExecNode {
public:
    VExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs) {}
    virtual Status get_next(RuntimeState* state, Block* block, bool* eos) = 0;
    // TODO: vconjuncts
};
} // namespace vectorized
} // namespace doris
