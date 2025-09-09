#ifndef ALLOC_H
#define ALLOC_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

// Function prototypes for the real functions (to avoid infinite recursion)
extern void* __libc_malloc(size_t);
extern void* __libc_calloc(size_t, size_t);
extern void* __libc_realloc(void*, size_t);
extern void __libc_free(void*);

// Debug malloc wrapper
void* MALLOC(size_t size);

// Debug calloc wrapper
void* CALLOC(size_t count, size_t nmemb);

// Debug realloc wrapper
void* REALLOC(void* ptr, size_t size);

// Debug free wrapper
void FREE(void* ptr);

// Debug function to print stats
void PRINT_ALLOC_STATS(void);

#endif /* ALLOC_H */
