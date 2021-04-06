#pragma once
#include "vec/sink/result_writer.h"
#include "runtime/primitive_type.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
class BufferControlBlock;
class RowBatch;
class MysqlRowBuffer;

namespace vectorized {
class VExprContext;

class MysqlResultWriter final: public VResultWriter {
public:
    MysqlResultWriter(BufferControlBlock* sinker,
            const std::vector<vectorized::VExprContext*>& output_vexpr_ctxs, RuntimeProfile* parent_profile);

    virtual ~MysqlResultWriter();

    virtual Status init(RuntimeState* state) override;

    virtual Status append_row_batch(const RowBatch* batch) override;

    virtual Status append_block(Block& block) override;

    virtual Status close() override;
private:
    void _init_profile();

    template <PrimitiveType type, bool is_nullable>
    Status _add_one_column(const ColumnPtr& column_ptr); 
private:
    BufferControlBlock* _sinker;

    const std::vector<vectorized::VExprContext*>& _output_vexpr_ctxs;
    // std::vector<int> _result_column_ids;

    std::vector<MysqlRowBuffer*> _vec_buffers;

    RuntimeProfile* _parent_profile; // parent profile from result sink. not owned
    // total time cost on append batch operation
    RuntimeProfile::Counter* _append_row_batch_timer = nullptr;
    // tuple convert timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_row_batch_timer
    RuntimeProfile::Counter* _result_send_timer = nullptr;
    // number of sent rows
    RuntimeProfile::Counter* _sent_rows_counter = nullptr;
};
}
}