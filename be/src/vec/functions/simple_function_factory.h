#pragma once
#include <mutex>
#include <string>

#include "vec/functions/abs.hpp"
#include "vec/functions/function.h"

namespace DB {
class SimpleFunctionFactory {
    using Functions = std::unordered_map<std::string, FunctionPtr>;

public:
    void registerFunction(const std::string& name, FunctionPtr ptr) { functions[name] = ptr; }
    template <class T>
    void registerFunction() {
        registerFunction(T::name,T::create());
    }

    FunctionPtr get(const std::string& name) { return functions[name]; }

private:
    Functions functions;

public:
    static SimpleFunctionFactory& instance() {
        static std::once_flag oc;
        static SimpleFunctionFactory instance;
        std::call_once(oc, [&]() {
            instance.registerFunction<FunctionAbs>();
        });
        return instance;
    }
};
} // namespace DB
