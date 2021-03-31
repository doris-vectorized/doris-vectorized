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

#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>

/**
  * Struct containing a pipe with lazy initialization.
  * Use `open` and `close` methods to manipulate pipe and `fds_rw` field to access
  * pipe's file descriptors.
  */
struct LazyPipe {
    int fds_rw[2] = {-1, -1};

    LazyPipe() = default;

    void open();

    void close();

    virtual ~LazyPipe() = default;
};

/**
  * Struct which opens new pipe on creation and closes it on destruction.
  * Use `fds_rw` field to access pipe's file descriptors.
  */
struct Pipe : public LazyPipe {
    Pipe();

    ~Pipe();
};
