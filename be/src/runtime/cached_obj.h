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

#include <memory>
#include <vector>
#include <mutex>
#include <malloc.h>
#include <assert.h>
#include <sys/sysinfo.h>

namespace doris {

	template <class T>
	class CachedObj {
		static std::vector<std::allocator<T>> _alloc_mem;
		static std::vector<T*> _free_store;
		static std::vector<std::shared_ptr<std::mutex>> _lock;
		static std::once_flag _once_flag;
		static const std::size_t _chunk = 8;

	public:
		virtual ~CachedObj() {}

		void *operator new(std::size_t size) {

			std::call_once(_once_flag, []() { 
                int n = get_nprocs_conf();
                assert(n > 0);
                //printf("called n=%d\n", n);
                _alloc_mem.resize(n); 
                _free_store.resize(n);
                _lock.resize(n);
                for (int i = 0; i < n; ++i) {
                    _lock[i] = std::make_shared<std::mutex>();
                }
            });

			int cpu = sched_getcpu();
            if (cpu >= _lock.size()) {
                cpu %= _lock.size();
            }

			std::lock_guard<std::mutex> lg(*_lock[cpu]);

			if (UNLIKELY(_free_store[cpu] == nullptr)) {
				T *array = _alloc_mem[cpu].allocate(_chunk);
				size_t n = _chunk - 1;
				for (size_t i = 0; i < n; ++i) {
					array[i].CachedObj<T>::_next = &array[i+1];
				}
				array[n].CachedObj<T>::_next = nullptr;
				_free_store[cpu] = array;
			}

			T *p = _free_store[cpu];
			_free_store[cpu] = _free_store[cpu]->CachedObj<T>::_next;

            //printf("new %p\n", p);
			return p;
		}

		void operator delete(void *p, std::size_t size) {
            //printf("delete %p\n", p);
			if (LIKELY(p != nullptr)) {
				T* t = static_cast<T*>(p);
				{
					int cpu = sched_getcpu();
                    if (cpu >= _lock.size()) {
                        cpu %= _lock.size();
                    }
					std::lock_guard<std::mutex> lg(*_lock[cpu]);
					t->CachedObj<T>::_next = _free_store[cpu];
					_free_store[cpu] = t;
				}
			}
		}

	protected:
		T *_next = nullptr;
	};

	template <class T> std::vector<std::allocator<T>> CachedObj<T>::_alloc_mem;
	template <class T> std::vector<T*> CachedObj<T>::_free_store;
	template <class T> std::vector<std::shared_ptr<std::mutex>> CachedObj<T>::_lock;
    template <class T> std::once_flag CachedObj<T>::_once_flag;

    /*
     * example
     * struct Foo : CachedObj<Foo> {
     *      int a;
     * };
     *
     * Foo *p = new Foo;
     * delete p;
     * */
}

