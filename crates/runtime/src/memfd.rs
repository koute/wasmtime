//! memfd support.

use crate::module_id::CompiledModuleId;
use anyhow::Result;
use memfd::{Memfd, MemfdOptions};
use rustix::fs::FileExt;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::{Arc, Mutex},
};
use wasmtime_environ::{
    DefinedMemoryIndex, MemoryInitialization, Module, PrimaryMap, WASM_PAGE_SIZE,
};

/// A registry of MemFD images.
pub struct MemFdRegistry {
    memfds: Mutex<HashMap<CompiledModuleId, Arc<ModuleMemFds>>>,
}

/// MemFDs containing backing images for certain memories in a module.
pub struct ModuleMemFds {
    memories: PrimaryMap<DefinedMemoryIndex, Option<Arc<MemoryMemFd>>>,
}

const MAX_MEMFD_IMAGE_SIZE: usize = 1024 * 1024 * 1024; // limit to 1GiB.

impl ModuleMemFds {
    pub(crate) fn get_memory_image(
        &self,
        defined_index: DefinedMemoryIndex,
    ) -> Option<&Arc<MemoryMemFd>> {
        self.memories[defined_index].as_ref()
    }
}

/// One backing image for one memory.
#[derive(Debug)]
pub(crate) struct MemoryMemFd {
    pub(crate) fd: Memfd,
    pub(crate) len: usize,
}

impl MemFdRegistry {
    /// Create a new MemFD image registry.
    pub fn new() -> Self {
        Self {
            memfds: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create MemFD images for a module's memories.
    pub fn get_or_create(
        &self,
        id: CompiledModuleId,
        module: &Module,
        wasm_data: &[u8],
    ) -> Result<Arc<ModuleMemFds>> {
        // Lock once and check the hashmap; take a clone of the Arc
        // right away and return it if present.
        if let Some(memfds) = self.memfds.lock().unwrap().get(&id) {
            return Ok(memfds.clone());
        }

        // If not, construct the payload (bundle of MemFds) and try
        // inserting. If we raced with another instantiation, drop the
        // thing that we built and just return the one that won
        // insertion.
        let memfds = Self::build_memfds(module, wasm_data)?;
        match self.memfds.lock().unwrap().entry(id) {
            Entry::Vacant(v) => {
                v.insert(memfds.clone());
                Ok(memfds)
            }
            Entry::Occupied(o) => {
                drop(memfds);
                Ok(o.get().clone())
            }
        }
    }

    fn build_memfds(module: &Module, wasm_data: &[u8]) -> Result<Arc<ModuleMemFds>> {
        let num_defined_memories = module.memory_plans.len() - module.num_imported_memories;

        // For each memory, determine the size of an image that will
        // contain all initializers.
        let mut sizes: PrimaryMap<DefinedMemoryIndex, usize> =
            PrimaryMap::with_capacity(num_defined_memories);
        let mut excluded_memories: HashSet<DefinedMemoryIndex> = HashSet::new();
        let mut segments_per_memory: PrimaryMap<DefinedMemoryIndex, Vec<usize>> = PrimaryMap::new();
        for _ in 0..num_defined_memories {
            sizes.push(0);
            segments_per_memory.push(vec![]);
        }

        match &module.memory_initialization {
            &MemoryInitialization::Segmented(ref segments) => {
                for (i, segment) in segments.iter().enumerate() {
                    if let Some(defined_memory) = module.defined_memory_index(segment.memory_index)
                    {
                        if segment.base.is_some() {
                            excluded_memories.insert(defined_memory);
                            continue;
                        }

                        let top = match (segment.offset as usize).checked_add(segment.data.len()) {
                            Some(top) => top,
                            None => {
                                excluded_memories.insert(defined_memory);
                                continue;
                            }
                        };
                        let min_pages = module.memory_plans[segment.memory_index].memory.minimum;
                        if top > (min_pages as usize) * WASM_PAGE_SIZE as usize {
                            excluded_memories.insert(defined_memory);
                            continue;
                        }

                        if top > MAX_MEMFD_IMAGE_SIZE {
                            excluded_memories.insert(defined_memory);
                            continue;
                        }

                        sizes[defined_memory] = std::cmp::max(sizes[defined_memory], top);
                        segments_per_memory[defined_memory].push(i);
                    }
                }
            }
            &MemoryInitialization::Paged { ref map, .. } => {
                for (defined_memory, ref pages) in map {
                    let top = pages
                        .iter()
                        .map(|(base, range)| *base as usize + range.len())
                        .max()
                        .unwrap_or(0);
                    sizes[defined_memory] = top;
                }
            }
        }

        // Now allocate MemFDs for each memory that does not have any
        // dynamically-initialized (depending on global values)
        // content.
        let mut memories: PrimaryMap<DefinedMemoryIndex, Option<Arc<MemoryMemFd>>> =
            PrimaryMap::default();
        let page_size = region::page::size();
        for (defined_memory, size) in sizes {
            memories.push(None);
            if excluded_memories.contains(&defined_memory) {
                continue;
            }
            if size == 0 {
                continue;
            }

            // Create the memfd. It needs a name, but the
            // documentation for `memfd_create()` says that names can
            // be duplicated with no issues.
            let memfd = MemfdOptions::new()
                .allow_sealing(true)
                .create("wasm-memory-image")?;

            // Round up size to nearest page boundary, and resize memfd to that size.
            let size = (size + page_size - 1) & !(page_size - 1);
            let file = memfd.as_file();
            file.set_len(size as u64)?;

            // Write data into it.
            match &module.memory_initialization {
                &MemoryInitialization::Segmented(ref segments) => {
                    for &i in &segments_per_memory[defined_memory] {
                        let base = segments[i].offset;
                        let data = &wasm_data
                            [segments[i].data.start as usize..segments[i].data.end as usize];
                        file.write_at(data, base)?;
                    }
                }
                &MemoryInitialization::Paged { ref map, .. } => {
                    for (base, range) in &map[defined_memory] {
                        let data = &wasm_data[range.start as usize..range.end as usize];
                        file.write_at(data, *base)?;
                    }
                }
            }

            // Seal the memfd's data and length.
            memfd.add_seal(memfd::FileSeal::SealGrow)?;
            memfd.add_seal(memfd::FileSeal::SealShrink)?;
            memfd.add_seal(memfd::FileSeal::SealWrite)?;
            memfd.add_seal(memfd::FileSeal::SealSeal)?;

            memories[defined_memory] = Some(Arc::new(MemoryMemFd {
                fd: memfd,
                len: size,
            }));
        }

        Ok(Arc::new(ModuleMemFds { memories }))
    }
}
