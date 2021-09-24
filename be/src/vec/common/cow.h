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

#include <atomic>
#include <assert.h>
#include <memory>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <initializer_list>

/** Copy-on-write shared ptr.
  * Allows to work with shared immutable objects and sometimes unshare and mutate you own unique copy.
  *
  * Usage:

    class Column : public COW<Column>
    {
    private:
        friend class COW<Column>;

        /// Leave all constructors in private section. They will be avaliable through 'create' method.
        Column();

        /// Provide 'clone' method. It can be virtual if you want polymorphic behaviour.
        virtual Column * clone() const;
    public:
        /// Correctly use const qualifiers in your interface.

        virtual ~Column() {}
    };

  * It will provide 'create' and 'mutate' methods.
  * And 'Ptr' and 'MutablePtr' types.
  * Ptr is refcounted pointer to immutable object.
  * MutablePtr is refcounted noncopyable pointer to mutable object.
  * MutablePtr can be assigned to Ptr through move assignment.
  *
  * 'create' method creates MutablePtr: you cannot share mutable objects.
  * To share, move-assign to immutable pointer.
  * 'mutate' method allows to create mutable noncopyable object from immutable object:
  *   either by cloning or by using directly, if it is not shared.
  * These methods are thread-safe.
  *
  * Example:
  *
    /// Creating and assigning to immutable ptr.
    Column::Ptr x = Column::create(1);
    /// Sharing single immutable object in two ptrs.
    Column::Ptr y = x;

    /// Now x and y are shared.

    /// Change value of x.
    {
        /// Creating mutable ptr. It can clone an object under the hood if it was shared.
        Column::MutablePtr mutate_x = std::move(*x).mutate();
        /// Using non-const methods of an object.
        mutate_x->set(2);
        /// Assigning pointer 'x' to mutated object.
        x = std::move(mutate_x);
    }

    /// Now x and y are unshared and have different values.

  * Note. You may have heard that COW is bad practice.
  * Actually it is, if your values are small or if copying is done implicitly.
  * This is the case for string implementations.
  *
  * In contrast, COW is intended for the cases when you need to share states of large objects,
  * (when you usually will use std::shared_ptr) but you also want precise control over modification
  * of this shared state.
  *
  * Caveats:
  * - after a call to 'mutate' method, you can still have a reference to immutable ptr somewhere.
  * - as 'mutable_ptr' should be unique, it's refcount is redundant - probably it would be better
  *   to use std::unique_ptr for it somehow.
  */
template <typename Derived>
class COW {
public:
    std::atomic_uint use_count = 0;

    COW() = default;
    COW(const COW& rhs) { use_count = 0; }
    COW(COW&& rhs) { use_count = 0; }
    COW& operator=(const COW& rhs) { return *this; }
    COW& operator=(COW&& rhs) { return *this; }
    ~COW() { assert(use_count == 0); }
    
private:
    Derived* derived() { return static_cast<Derived*>(this); }
    const Derived* derived() const { return static_cast<const Derived*>(this); }

protected:
    template <typename T>
    class base_ptr {
    public:
        T* ptr;
        base_ptr(T* ptr) : ptr(ptr) {}

        T* operator->() { return ptr; }
        T* operator->() const { return ptr; }

        T* get() { return ptr; }
        T* get() const { return ptr; }

        operator bool() const { return ptr != nullptr; }

        T& operator*() const & { return *ptr; }
        T&& operator*() const && { return const_cast<typename std::remove_const<T>::type&&>(*ptr); }

        template <typename U> bool operator==(U* u) const { return ptr == u; }
        template <typename U> bool operator!=(U* u) const { return ptr != u; }

        bool operator==(std::nullptr_t) const { return ptr == nullptr; }
        bool operator!=(std::nullptr_t) const { return ptr != nullptr; }
    };

    template <typename T>
    class mutable_ptr : public base_ptr<T> {
    private:
        template <typename> friend class COW;
        template <typename, typename> friend class COWHelper;

        explicit mutable_ptr(T* ptr) : base_ptr<T>(ptr) {
            assert(ptr->use_count == 0);
            ptr->use_count++; 
        }

    public:
        mutable_ptr() : base_ptr<T>(nullptr) { }

        mutable_ptr(std::nullptr_t) : base_ptr<T>(nullptr) { }

        mutable_ptr(const mutable_ptr& rhs) = delete;

        mutable_ptr(mutable_ptr&& rhs) : base_ptr<T>(rhs.ptr) { rhs.ptr = nullptr; }

        mutable_ptr& operator=(mutable_ptr& rhs) = delete;

        mutable_ptr& operator=(mutable_ptr&& rhs) {
            if (this != &rhs) {
                if (this->template ptr->use_count.fetch_sub(1) == 1) {
                    delete this->template ptr;
                }

                this->template ptr = rhs.ptr;
                rhs.ptr = nullptr;
                assert(this->template ptr->use_count == 1);
            }
            return *this;
        }

        ~mutable_ptr() {
            if (this->template ptr->use_count.fetch_sub(1) == 1) {
                delete this->template ptr;
                this->template ptr = nullptr;
            }
        }

        /// Initializing from temporary of compatible type.
        template <typename U>
        mutable_ptr(mutable_ptr<U>&& other) : base_ptr<T>(other.template ptr) {
            other.template ptr = nullptr;
        }
    };

public:
    using MutablePtr = mutable_ptr<Derived>;

protected:
    template <typename T>
    class immutable_ptr : public base_ptr<const T> {
    private:
        template <typename> friend class COW;

        template <typename, typename> friend class COWHelper;

        explicit immutable_ptr(const T* ptr) : base_ptr<const T>(ptr) { ((T*)ptr)->use_count++; }

        T* t_ptr() { return (T*)(this->template ptr); }

    public:
        immutable_ptr() : base_ptr<const T>(nullptr) {}

        immutable_ptr(std::nullptr_t) : base_ptr<const T>(nullptr) {}

        /// Copy from immutable ptr: ok.
        immutable_ptr(const immutable_ptr& rhs) : base_ptr<const T>(rhs.ptr) { t_ptr()->use_count++; }

        /// Move: ok.
        immutable_ptr(immutable_ptr&& rhs) : base_ptr<const T>(rhs.ptr) { rhs.ptr = nullptr; }

        template <typename U>
        immutable_ptr(mutable_ptr<U>&& other) : base_ptr<const T>(other.template ptr) { other.template ptr = nullptr; }

        template <typename U>
        immutable_ptr(immutable_ptr<U>&& other) : base_ptr<const T>(other.template ptr) { other.template ptr = nullptr; }

        immutable_ptr& operator=(const immutable_ptr& rhs) {
            if (this != &rhs) {
                if (t_ptr()->use_count.fetch_sub(1) == 1) {
                    delete this->template ptr;
                }

                this->template ptr = rhs.ptr;
                t_ptr()->use_count++;
            }
            return *this;
        }

        immutable_ptr& operator=(immutable_ptr&& rhs) {
            if (this != &rhs) {
                if (t_ptr()->use_count.fetch_sub(1) == 1) {
                    delete this->template ptr;
                }
                
                this->template ptr = rhs.ptr;
                rhs.ptr = nullptr;
            }
            return *this;
        }
    };

public:
    using Ptr = immutable_ptr<Derived>;

    template <typename... Args>
    static MutablePtr create(Args&&... args) {
        return MutablePtr(new Derived(std::forward<Args>(args)...));
    }

    template <typename T>
    static MutablePtr create(std::initializer_list<T>&& arg) {
        return create(std::forward<std::initializer_list<T>>(arg));
    }

public:
    Ptr get_ptr() const { return Ptr(derived()); }
    MutablePtr get_ptr() { return MutablePtr(derived()); }

protected:
    MutablePtr shallow_mutate() const {
        if (this->use_count > 1)
            return derived()->clone();
        else
            return assume_mutable();
    }

public:
    MutablePtr mutate() const&& { return shallow_mutate(); }

    MutablePtr assume_mutable() const { return const_cast<COW*>(this)->get_ptr(); }

    Derived& assume_mutable_ref() const { return const_cast<Derived&>(*derived()); }

protected:
    /// It works as immutable_ptr if it is const and as mutable_ptr if it is non const.
    template <typename T>
    class chameleon_ptr {
    private:
        immutable_ptr<T> value;

    public:
        template <typename... Args>
        chameleon_ptr(Args&&... args) : value(std::forward<Args>(args)...) {}

        template <typename U>
        chameleon_ptr(std::initializer_list<U>&& arg)
                : value(std::forward<std::initializer_list<U>>(arg)) {}

        const T* get() const { return value.get(); }
        T* get() { return &value->assume_mutable_ref(); }

        const T* operator->() const { return get(); }
        T* operator->() { return get(); }

        const T& operator*() const { return *value; }
        T& operator*() { return value->assume_mutable_ref(); }

        operator const immutable_ptr<T> &() const { return value; }
        operator immutable_ptr<T> &() { return value; }

        operator bool() const { return value != nullptr; }
        bool operator!() const { return value == nullptr; }

        bool operator==(const chameleon_ptr& rhs) const { return value == rhs.value; }
        bool operator!=(const chameleon_ptr& rhs) const { return value != rhs.value; }
    };

public:
    /** Use this type in class members for compositions.
      *
      * NOTE:
      * For classes with WrappedPtr members,
      * you must reimplement 'mutate' method, so it will call 'mutate' of all subobjects (do deep mutate).
      * It will guarantee, that mutable object have all subobjects unshared.
      *
      * NOTE:
      * If you override 'mutate' method in inherited classes, don't forget to make it virtual in base class or to make it call a virtual method.
      * (COW itself doesn't force any methods to be virtual).
      *
      * See example in "cow_compositions.cpp".
      */
    using WrappedPtr = chameleon_ptr<Derived>;
};

/** Helper class to support inheritance.
  * Example:
  *
  * class IColumn : public COW<IColumn>
  * {
  *     friend class COW<IColumn>;
  *     virtual MutablePtr clone() const = 0;
  *     virtual ~IColumn() {}
  * };
  *
  * class ConcreteColumn : public COWHelper<IColumn, ConcreteColumn>
  * {
  *     friend class COWHelper<IColumn, ConcreteColumn>;
  * };
  *
  * Here is complete inheritance diagram:
  *
  * ConcreteColumn
  *  COWHelper<IColumn, ConcreteColumn>
  *   IColumn
  *    CowPtr<IColumn>
  *     boost::intrusive_ref_counter<IColumn>
  *
  * See example in "cow_columns.cpp".
  */
template <typename Base, typename Derived>
class COWHelper : public Base {
private:
    Derived* derived() { return static_cast<Derived*>(this); }
    const Derived* derived() const { return static_cast<const Derived*>(this); }

public:
    using Ptr = typename Base::template immutable_ptr<Derived>;
    using MutablePtr = typename Base::template mutable_ptr<Derived>;

    template <typename... Args>
    static MutablePtr create(Args&&... args) {
        return MutablePtr(new Derived(std::forward<Args>(args)...));
    }

    template <typename T>
    static MutablePtr create(std::initializer_list<T>&& arg) {
        return create(std::forward<std::initializer_list<T>>(arg));
    }

    typename Base::MutablePtr clone() const override {
        return typename Base::MutablePtr(new Derived(*derived()));
    }

protected:
    MutablePtr shallow_mutate() const {
        return MutablePtr(static_cast<Derived*>(Base::shallow_mutate().get()));
    }
};
