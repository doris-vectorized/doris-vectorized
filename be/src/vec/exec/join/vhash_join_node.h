#pragma once
#include <variant>

#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {

struct SerializedHashTableContext {
    using Mapped = RowRefList;
    using HashTable = HashMap<StringRef, Mapped>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;
    HashTable hash_table;
};
struct I32HashTableContext {
    using Mapped = RowRefList;
    using HashTable = HashMap<UInt32, Mapped, HashCRC32<UInt32>>;
    using State = ColumnsHashing::HashMethodOneNumber<typename HashTable::value_type, Mapped,
                                                      UInt32, false>;
    HashTable hash_table;
};

using HashTableVariants =
        std::variant<std::monostate, SerializedHashTableContext, I32HashTableContext>;

class VExprContext;

class HashJoinNode : public ::doris::ExecNode {
public:
    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HashJoinNode();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status get_next(RuntimeState* state, Block* block, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    using VExprContexts = std::vector<VExprContext*>;

    TJoinOp::type _join_op;
    // probe expr
    VExprContexts _probe_expr_ctxs;
    // build expr
    VExprContexts _build_expr_ctxs;
    // other expr
    VExprContexts _other_join_conjunct_ctxs;

    std::vector<bool> _is_null_safe_eq_join;

    DataTypes _right_table_data_types;
    DataTypes _left_table_data_types;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _build_table_timer;
    RuntimeProfile::Counter* _build_hash_calc_timer;
    RuntimeProfile::Counter* _build_bucket_calc_timer;
    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _build_table_insert_timer;
    RuntimeProfile::Counter* _build_table_spread_timer;
    RuntimeProfile::Counter* _build_table_expanse_timer;
    RuntimeProfile::Counter* _build_acquire_block_timer;
    RuntimeProfile::Counter* _probe_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;
    RuntimeProfile::Counter* _probe_hash_calc_timer;
    RuntimeProfile::Counter* _probe_gather_timer;
    RuntimeProfile::Counter* _probe_next_timer;
    RuntimeProfile::Counter* _probe_select_miss_timer;
    RuntimeProfile::Counter* _probe_select_zero_timer;
    RuntimeProfile::Counter* _probe_diff_timer;
    RuntimeProfile::Counter* _build_buckets_counter;

    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
    RuntimeProfile::Counter* _build_rows_counter;
    RuntimeProfile::Counter* _probe_rows_counter;

    bool _build_unique;

    int64_t _hash_table_rows;

    Arena _arena;
    HashTableVariants _hash_table_variants;
    AcquireList<Block> _acquire_list;

    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    int _probe_index;

private:
    Status _hash_table_build(RuntimeState* state);
    Status _process_build_block(Block& block);
};
} // namespace vectorized
} // namespace doris