#include "Exception.h"

#include <string.h>
#include <cxxabi.h>
#include <iostream>
// #include <Poco/String.h>
// #include <vec/common/logger_useful.h>
// #include <IO/WriteHelpers.h>
// #include <IO/Operators.h>
// #include <IO/ReadBufferFromString.h>
// #include <vec/common/demangle.h>
// #include <vec/Common/config_version.h>
// #include <vec/Common/formatReadable.h>
// #include <vec/Common/DiskSpaceMonitor.h>
#include <filesystem>

#include <string>
#include <typeinfo>


namespace Poco {


Exception::Exception(int code): _pNested(0), _code(code)
{
}


Exception::Exception(const std::string& msg, int code): _msg(msg), _pNested(0), _code(code)
{
}


Exception::Exception(const std::string& msg, const std::string& arg, int code): _msg(msg), _pNested(0), _code(code)
{
	if (!arg.empty())
	{
		_msg.append(": ");
		_msg.append(arg);
	}
}


Exception::Exception(const std::string& msg, const Exception& nested, int code): _msg(msg), _pNested(nested.clone()), _code(code)
{
}


Exception::Exception(const Exception& exc):
	std::exception(exc),
	_msg(exc._msg),
	_code(exc._code)
{
	_pNested = exc._pNested ? exc._pNested->clone() : 0;
}

	
Exception::~Exception() throw()
{
	delete _pNested;
}


Exception& Exception::operator = (const Exception& exc)
{
	if (&exc != this)
	{
		Exception* newPNested = exc._pNested ? exc._pNested->clone() : 0;
		delete _pNested;
		_msg     = exc._msg;
		_pNested = newPNested;
		_code    = exc._code;
	}
	return *this;
}


const char* Exception::name() const throw()
{
	return "Exception";
}


const char* Exception::className() const throw()
{
	return typeid(*this).name();
}

	
const char* Exception::what() const throw()
{
	return name();
}

	
std::string Exception::displayText() const
{
	std::string txt = name();
	if (!_msg.empty())
	{
		txt.append(": ");
		txt.append(_msg);
	}
	return txt;
}


void Exception::extendedMessage(const std::string& arg)
{
	if (!arg.empty())
	{
		if (!_msg.empty()) _msg.append(": ");
		_msg.append(arg);
	}
}


Exception* Exception::clone() const
{
	return new Exception(*this);
}


void Exception::rethrow() const
{
	throw *this;
}


POCO_IMPLEMENT_EXCEPTION(LogicException, Exception, "Logic exception")
POCO_IMPLEMENT_EXCEPTION(AssertionViolationException, LogicException, "Assertion violation")
POCO_IMPLEMENT_EXCEPTION(NullPointerException, LogicException, "Null pointer")
POCO_IMPLEMENT_EXCEPTION(NullValueException, LogicException, "Null value")
POCO_IMPLEMENT_EXCEPTION(BugcheckException, LogicException, "Bugcheck")
POCO_IMPLEMENT_EXCEPTION(InvalidArgumentException, LogicException, "Invalid argument")
POCO_IMPLEMENT_EXCEPTION(NotImplementedException, LogicException, "Not implemented")
POCO_IMPLEMENT_EXCEPTION(RangeException, LogicException, "Out of range")
POCO_IMPLEMENT_EXCEPTION(IllegalStateException, LogicException, "Illegal state")
POCO_IMPLEMENT_EXCEPTION(InvalidAccessException, LogicException, "Invalid access")
POCO_IMPLEMENT_EXCEPTION(SignalException, LogicException, "Signal received")
POCO_IMPLEMENT_EXCEPTION(UnhandledException, LogicException, "Unhandled exception")

POCO_IMPLEMENT_EXCEPTION(RuntimeException, Exception, "Runtime exception")
POCO_IMPLEMENT_EXCEPTION(NotFoundException, RuntimeException, "Not found")
POCO_IMPLEMENT_EXCEPTION(ExistsException, RuntimeException, "Exists")
POCO_IMPLEMENT_EXCEPTION(TimeoutException, RuntimeException, "Timeout")
POCO_IMPLEMENT_EXCEPTION(SystemException, RuntimeException, "System exception")
POCO_IMPLEMENT_EXCEPTION(RegularExpressionException, RuntimeException, "Error in regular expression")
POCO_IMPLEMENT_EXCEPTION(LibraryLoadException, RuntimeException, "Cannot load library")
POCO_IMPLEMENT_EXCEPTION(LibraryAlreadyLoadedException, RuntimeException, "Library already loaded")
POCO_IMPLEMENT_EXCEPTION(NoThreadAvailableException, RuntimeException, "No thread available")
POCO_IMPLEMENT_EXCEPTION(PropertyNotSupportedException, RuntimeException, "Property not supported")
POCO_IMPLEMENT_EXCEPTION(PoolOverflowException, RuntimeException, "Pool overflow")
POCO_IMPLEMENT_EXCEPTION(NoPermissionException, RuntimeException, "No permission")
POCO_IMPLEMENT_EXCEPTION(OutOfMemoryException, RuntimeException, "Out of memory")
POCO_IMPLEMENT_EXCEPTION(DataException, RuntimeException, "Data error")

POCO_IMPLEMENT_EXCEPTION(DataFormatException, DataException, "Bad data format")
POCO_IMPLEMENT_EXCEPTION(SyntaxException, DataException, "Syntax error")
POCO_IMPLEMENT_EXCEPTION(CircularReferenceException, DataException, "Circular reference")
POCO_IMPLEMENT_EXCEPTION(PathSyntaxException, SyntaxException, "Bad path syntax")
POCO_IMPLEMENT_EXCEPTION(IOException, RuntimeException, "I/O error")
POCO_IMPLEMENT_EXCEPTION(ProtocolException, IOException, "Protocol error")
POCO_IMPLEMENT_EXCEPTION(FileException, IOException, "File access error")
POCO_IMPLEMENT_EXCEPTION(FileExistsException, FileException, "File exists")
POCO_IMPLEMENT_EXCEPTION(FileNotFoundException, FileException, "File not found")
POCO_IMPLEMENT_EXCEPTION(PathNotFoundException, FileException, "Path not found")
POCO_IMPLEMENT_EXCEPTION(FileReadOnlyException, FileException, "File is read-only")
POCO_IMPLEMENT_EXCEPTION(FileAccessDeniedException, FileException, "Access to file denied")
POCO_IMPLEMENT_EXCEPTION(CreateFileException, FileException, "Cannot create file")
POCO_IMPLEMENT_EXCEPTION(OpenFileException, FileException, "Cannot open file")
POCO_IMPLEMENT_EXCEPTION(WriteFileException, FileException, "Cannot write file")
POCO_IMPLEMENT_EXCEPTION(ReadFileException, FileException, "Cannot read file")
POCO_IMPLEMENT_EXCEPTION(DirectoryNotEmptyException, FileException, "Directory not empty")
POCO_IMPLEMENT_EXCEPTION(UnknownURISchemeException, RuntimeException, "Unknown URI scheme")
POCO_IMPLEMENT_EXCEPTION(TooManyURIRedirectsException, RuntimeException, "Too many URI redirects")
POCO_IMPLEMENT_EXCEPTION(URISyntaxException, SyntaxException, "Bad URI syntax")

POCO_IMPLEMENT_EXCEPTION(ApplicationException, Exception, "Application exception")
POCO_IMPLEMENT_EXCEPTION(BadCastException, RuntimeException, "Bad cast exception")


} // namespace Poco


namespace DB
{

namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int NOT_IMPLEMENTED;
}

//TODO: use fmt
std::string errnoToString(int code, int e)
{
    const size_t buf_size = 128;
    char buf[buf_size];
    return "errno: " + std::to_string(e) + ", strerror: " + std::string(strerror_r(e, buf, sizeof(buf)));
}

void throwFromErrno(const std::string & s, int code, int e)
{
    throw ErrnoException(s + ", " + errnoToString(code, e), code, e);
}

void throwFromErrnoWithPath(const std::string & s, const std::string & path, int code, int the_errno)
{
    throw ErrnoException(s + ", " + errnoToString(code, the_errno), code, the_errno, path);
}

void tryLogCurrentException(const char * log_name, const std::string & start_of_message)
{
    // tryLogCurrentException(&Logger::get(log_name), start_of_message);
    std::cout << "[TODO] should use glog here :" << start_of_message << std::endl;
}

// void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message)
// {
//     try
//     {
//         LOG_ERROR(logger, start_of_message << (start_of_message.empty() ? "" : ": ") << getCurrentExceptionMessage(true));
//     }
//     catch (...)
//     {
//     }
// }

// void getNoSpaceLeftInfoMessage(std::filesystem::path path, std::string & msg)
// {
//     path = std::filesystem::absolute(path);
//     /// It's possible to get ENOSPC for non existent file (e.g. if there are no free inodes and creat() fails)
//     /// So try to get info for existent parent directory.
//     while (!std::filesystem::exists(path) && path.has_relative_path())
//         path = path.parent_path();

//     auto fs = DiskSpace::getStatVFS(path);
//     msg += "\nTotal space: "      + formatReadableSizeWithBinarySuffix(fs.f_blocks * fs.f_bsize)
//          + "\nAvailable space: "  + formatReadableSizeWithBinarySuffix(fs.f_bavail * fs.f_bsize)
//          + "\nTotal inodes: "     + formatReadableQuantity(fs.f_files)
//          + "\nAvailable inodes: " + formatReadableQuantity(fs.f_favail);

//     auto mount_point = DiskSpace::getMountPoint(path).string();
//     msg += "\nMount point: " + mount_point;
// #if defined(__linux__)
//     msg += "\nFilesystem: " + DiskSpace::getFilesystemName(mount_point);
// #endif
// }

std::string getExtraExceptionInfo(const std::exception & e)
{
    std::string msg;
    // try
    // {
    //     if (auto file_exception = dynamic_cast<const Poco::FileException *>(&e))
    //     {
    //         if (file_exception->code() == ENOSPC)
    //             getNoSpaceLeftInfoMessage(file_exception->message(), msg);
    //     }
    //     else if (auto errno_exception = dynamic_cast<const DB::ErrnoException *>(&e))
    //     {
    //         if (errno_exception->getErrno() == ENOSPC && errno_exception->getPath())
    //             getNoSpaceLeftInfoMessage(errno_exception->getPath().value(), msg);
    //     }
    // }
    // catch (...)
    // {
    //     msg += "\nCannot print extra info: " + getCurrentExceptionMessage(false, false, false);
    // }

    return msg;
}

std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace /*= false*/, bool with_extra_info /*= true*/)
{
    std::stringstream stream;

    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        stream << getExceptionMessage(e, with_stacktrace, check_embedded_stacktrace)
               << (with_extra_info ? getExtraExceptionInfo(e) : "")
               << " (version " << "VERSION_STRING" << "VERSION_OFFICIAL" << ")";
    }
    catch (const Poco::Exception & e)
    {
        try
        {
            stream << "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
                << ", e.displayText() = " << e.displayText()
                << (with_extra_info ? getExtraExceptionInfo(e) : "")
                << " (version " << "VERSION_STRING" << "VERSION_OFFICIAL";
        }
        catch (...) {}
    }
    catch (const std::exception & e)
    {
        try
        {
            // int status = 0;
            // auto name = demangle(typeid(e).name(), status);

            // if (status)
            //     name += " (demangling status: " + std::to_string(status) + ")";

            // stream << "std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", type: " << name << ", e.what() = " << e.what()
            //        << (with_extra_info ? getExtraExceptionInfo(e) : "")
            //        << ", version = " << "VERSION_STRING" << "VERSION_OFFICIAL";
        }
        catch (...) {}
    }
    catch (...)
    {
        try
        {
            // int status = 0;
            // auto name = demangle(abi::__cxa_current_exception_type()->name(), status);

            // if (status)
            //     name += " (demangling status: " + std::to_string(status) + ")";

            // stream << "Unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ", type: " << name << " (version " << "VERSION_STRING" << "VERSION_OFFICIAL" << ")";
        }
        catch (...) {}
    }

    return stream.str();
}


// int getCurrentExceptionCode()
// {
//     try
//     {
//         throw;
//     }
//     catch (const Exception & e)
//     {
//         return e.code();
//     }
//     catch (const Poco::Exception &)
//     {
//         return ErrorCodes::POCO_EXCEPTION;
//     }
//     catch (const std::exception &)
//     {
//         return ErrorCodes::STD_EXCEPTION;
//     }
//     catch (...)
//     {
//         return ErrorCodes::UNKNOWN_EXCEPTION;
//     }
// }


// void rethrowFirstException(const Exceptions & exceptions)
// {
//     for (size_t i = 0, size = exceptions.size(); i < size; ++i)
//         if (exceptions[i])
//             std::rethrow_exception(exceptions[i]);
// }


// void tryLogException(std::exception_ptr e, const char * log_name, const std::string & start_of_message)
// {
//     try
//     {
//         std::rethrow_exception(std::move(e));
//     }
//     catch (...)
//     {
//         tryLogCurrentException(log_name, start_of_message);
//     }
// }

// void tryLogException(std::exception_ptr e, Poco::Logger * logger, const std::string & start_of_message)
// {
//     try
//     {
//         std::rethrow_exception(std::move(e));
//     }
//     catch (...)
//     {
//         tryLogCurrentException(logger, start_of_message);
//     }
// }

std::string getExceptionMessage(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace)
{
    std::stringstream stream;

    try
    {
        std::string text = e.displayText();

        bool has_embedded_stack_trace = false;
        if (check_embedded_stacktrace)
        {
            auto embedded_stack_trace_pos = text.find("Stack trace");
            has_embedded_stack_trace = embedded_stack_trace_pos != std::string::npos;
            if (!with_stacktrace && has_embedded_stack_trace)
            {
                text.resize(embedded_stack_trace_pos);
                // Poco::trimRightInPlace(text);
            }
        }

        stream << "Code: " << e.code() << ", e.displayText() = " << text;

        if (with_stacktrace && !has_embedded_stack_trace)
            stream << ", Stack trace:\n\n" << e.getStackTrace().value();
    }
    catch (...) {}

    return stream.str();
}

std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (...)
    {
        return getCurrentExceptionMessage(with_stacktrace);
    }
}


// std::string ExecutionStatus::serializeText() const
// {
//     WriteBufferFromOwnString wb;
//     wb << code << "\n" << escape << message;
//     return wb.str();
// }

// void ExecutionStatus::deserializeText(const std::string & data)
// {
//     ReadBufferFromString rb(data);
//     rb >> code >> "\n" >> escape >> message;
// }

// bool ExecutionStatus::tryDeserializeText(const std::string & data)
// {
//     try
//     {
//         deserializeText(data);
//     }
//     catch (...)
//     {
//         return false;
//     }

//     return true;
// }

// ExecutionStatus ExecutionStatus::fromCurrentException(const std::string & start_of_message)
// {
//     String msg = (start_of_message.empty() ? "" : (start_of_message + ": ")) + getCurrentExceptionMessage(false, true);
//     return ExecutionStatus(getCurrentExceptionCode(), msg);
// }


}
