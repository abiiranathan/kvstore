#include <stdlib.h>
#include <string.h>

#include "kvstore.h"

#define ARENA_DEFAULT_BLOCK_SIZE (64 * 1024)  // 64KB

arena_t* arena_create(size_t block_size) {
    if (block_size == 0) {
        block_size = ARENA_DEFAULT_BLOCK_SIZE;
    }

    arena_t* arena = malloc(sizeof(arena_t));
    if (!arena) return NULL;

    arena->block_size = block_size;
    arena->first      = NULL;
    arena->current    = NULL;

    return arena;
}

void arena_destroy(arena_t* arena) {
    if (!arena) return;

    arena_block_t* block = arena->first;
    while (block) {
        arena_block_t* next = block->next;
        free(block);
        block = next;
    }
    free(arena);
}

void* arena_alloc(arena_t* arena, size_t size) {
    if (!arena || size == 0) return NULL;

    // Align to 8 bytes for better performance
    size = (size + 7) & ~7;

    if (!arena->current || (arena->current->used + size) > arena->current->size) {
        // Need a new block
        size_t block_size        = (size > arena->block_size) ? size : arena->block_size;
        arena_block_t* new_block = malloc(sizeof(arena_block_t) + block_size);
        if (!new_block) return NULL;

        new_block->next = NULL;
        new_block->used = 0;
        new_block->size = block_size;

        if (arena->current) {
            arena->current->next = new_block;
        } else {
            arena->first = new_block;
        }
        arena->current = new_block;
    }

    void* ptr = arena->current->data + arena->current->used;
    arena->current->used += size;
    return ptr;
}

void arena_reset(arena_t* arena) {
    if (!arena) return;

    arena_block_t* block = arena->first;
    while (block) {
        block->used = 0;
        block       = block->next;
    }
    arena->current = arena->first;
}
