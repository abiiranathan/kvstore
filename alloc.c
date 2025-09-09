#include "alloc.h"

// Global counters for debugging
static size_t total_allocated = 0;
static size_t peak_allocated  = 0;
static size_t num_allocs      = 0;
static size_t num_frees       = 0;

// Debug malloc wrapper
void* MALLOC(size_t size) {
    if (size == 0) {
        return NULL;  // Or handle as per implementation-defined behavior
    }
    void* ptr = __libc_malloc(size);
    if (ptr != NULL) {
        total_allocated += size;
        if (total_allocated > peak_allocated) {
            peak_allocated = total_allocated;
        }
        num_allocs++;
        fprintf(stderr, "[DEBUG] malloc(%zu) = %p (total: %zu, peak: %zu, allocs: %zu)\n", size, ptr,
                total_allocated, peak_allocated, num_allocs);
    } else {
        fprintf(stderr, "[DEBUG] malloc(%zu) failed\n", size);
    }
    return ptr;
}

// Debug calloc wrapper
void* CALLOC(size_t count, size_t nmemb) {
    if (count == 0 || nmemb == 0) {
        return NULL;
    }

    total_allocated += count * nmemb;
    void* ptr = __libc_calloc(count, nmemb);
    if (ptr != NULL) {
        if (total_allocated > peak_allocated) {
            peak_allocated = total_allocated;
        }
        num_allocs++;
        fprintf(stderr, "[DEBUG] calloc(%zu) = %p (total: %zu, peak: %zu, allocs: %zu)\n", total_allocated,
                ptr, total_allocated, peak_allocated, num_allocs);
    } else {
        fprintf(stderr, "[DEBUG] calloc(%zu) failed\n", total_allocated);
    }
    return ptr;
}

// Debug realloc wrapper
void* REALLOC(void* ptr, size_t size) {
    if (ptr == NULL) {
        return malloc(size);  // Behaves like malloc
    }
    if (size == 0) {
        free(ptr);  // Behaves like free
        return NULL;
    }
    // To track size change, we could store size in a header, but for simplicity, assume no size tracking
    // If you want size tracking, implement a custom allocator with metadata
    void* new_ptr = __libc_realloc(ptr, size);
    if (new_ptr != NULL && new_ptr != ptr) {
        // Size changed, but without metadata, we can't subtract old size accurately
        // For basic: just note the call
        fprintf(stderr, "[DEBUG] realloc(%p, %zu) = %p\n", ptr, size, new_ptr);
    } else if (new_ptr == NULL) {
        fprintf(stderr, "[DEBUG] realloc(%p, %zu) failed\n", ptr, size);
    }
    return new_ptr;
}

// Debug free wrapper
void FREE(void* ptr) {
    if (ptr == NULL) {
        return;
    }

    // Without metadata, can't subtract exact size; for demo, assume placeholder or skip delta
    // In a full debug malloc, you'd have a map or header to track and verify
    fprintf(stderr, "[DEBUG] free(%p) (frees: %zu)\n", ptr, ++num_frees);
    __libc_free(ptr);
    // Simulate delta (placeholder; in real, subtract allocated size)
    // total_allocated -= some_size;  // Requires tracking
}

// Debug function to print stats
void PRINT_ALLOC_STATS(void) {
    fprintf(stderr, "[DEBUG STATS] Total allocated: %zu bytes\n", total_allocated);
    fprintf(stderr, "[DEBUG STATS] Peak allocated: %zu bytes\n", peak_allocated);
    fprintf(stderr, "[DEBUG STATS] Allocations: %zu\n", num_allocs);
    fprintf(stderr, "[DEBUG STATS] Frees: %zu\n", num_frees);
    if (num_allocs != num_frees) {
        fprintf(stderr, "[DEBUG WARNING] Possible memory leak: %zu more allocs than frees\n",
                num_allocs - num_frees);
    }
}
