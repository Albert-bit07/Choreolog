#include "choreoos/storage/directory_lease.hpp"

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace choreoos::storage {

struct DirectoryLease::Impl {
#ifdef _WIN32
  HANDLE handle = INVALID_HANDLE_VALUE;
  ~Impl() {
    if (handle != INVALID_HANDLE_VALUE) CloseHandle(handle);
  }
#else
  int descriptor = -1;
  ~Impl() {
    if (descriptor >= 0) close(descriptor);
  }
#endif
};

state::Result<DirectoryLease> DirectoryLease::acquire(const std::filesystem::path& directory) {
  std::error_code error;
  std::filesystem::create_directories(directory, error);
  if (error) return state::Error{state::ErrorCode::StoreError, "cannot create node data directory"};
  const auto path = directory / ".choreoos-writer.lock";
  auto owner = std::make_shared<Impl>();
#ifdef _WIN32
  owner->handle = CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, OPEN_ALWAYS,
                              FILE_ATTRIBUTE_NORMAL, nullptr);
  const bool acquired = owner->handle != INVALID_HANDLE_VALUE;
#else
  owner->descriptor = open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
  const bool acquired = owner->descriptor >= 0 && flock(owner->descriptor, LOCK_EX | LOCK_NB) == 0;
#endif
  if (!acquired)
    return state::Error{state::ErrorCode::StoreError,
                        "node data directory is locked by another writer or inaccessible"};
  return DirectoryLease(std::move(owner));
}

}  // namespace choreoos::storage
