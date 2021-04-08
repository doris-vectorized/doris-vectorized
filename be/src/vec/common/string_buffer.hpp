// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once
#include <fmt/format.h>

#include <cstring>

namespace doris::vectorized {
class BufferWritable {
public:
    virtual void write(const char* data, int len) = 0;

    template <typename T>
    void write_number(T data) {
        fmt::memory_buffer buffer;
        fmt::format_to(buffer, "{}", data);
        buffer.push_back('\0');
        write(buffer.data(), buffer.size());
    }

    int count() const { return _writer_counter; }

protected:
    void update_writer_counter(int delta) { _writer_counter += delta; }

private:
    int _writer_counter = 0;
};

template <class T>
class VectorBufferWriter : public BufferWritable {
public:
    VectorBufferWriter(T& vector) : _vector(vector) {}

    void write(const char* data, int len) {
        _vector.insert(data, data + len);
        update_writer_counter(len);
    }

private:
    T& _vector;
};
} // namespace doris::vectorized
