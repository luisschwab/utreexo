package swisstable

import "syscall"

// mmapFlags are the flags passed to syscall.Mmap for shared, read-write mappings.
// MAP_POPULATE pre-faults pages on Linux, avoiding page faults on first access.
const mmapFlags = syscall.MAP_SHARED | syscall.MAP_POPULATE
