#pragma once

#include <memory>
#include <vector>
namespace doris::vectorized {
template <typename Element, int batch_size = 8>
struct AcquireList {
    using Batch = Element[batch_size];
    Element& acquire(Element&& element) {
        if (_current_batch == nullptr) {
            _current_batch.reset(new Element[batch_size]);
        }
        if (current_full()) {
            _lst.emplace_back(std::move(_current_batch));
            _current_batch.reset(new Element[batch_size]);
            _current_offset = 0;
        }

        auto base_addr = _current_batch.get();
        base_addr[_current_offset] = std::move(element);
        auto& ref = base_addr[_current_offset];
        _current_offset++;
        return ref;
    }

private:
    bool current_full() { return _current_offset == batch_size; }
    std::vector<std::unique_ptr<Element[]>> _lst;
    std::unique_ptr<Element[]> _current_batch;
    int _current_offset = 0;
};
} // namespace doris::vectorized
