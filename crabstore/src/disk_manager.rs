use std::{
    fs::*,
    io::{self, Write},
    os::windows::prelude::FileExt,
    path::Path,
};

use crate::{page::PhysicalPage, PAGE_SIZE};

pub struct DiskManager {
    file: File,
    next_free_page: usize,
}

impl DiskManager {
    pub fn new(file_path: &Path) -> Result<Self, io::Error> {
        Ok(DiskManager {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(file_path)?,
            next_free_page: 0,
        })
    }

    pub fn flush(&mut self) {
        self.file.flush().expect("Failed to flush file to disk");
    }

    #[cfg(target_os = "windows")]
    pub fn read_page(&self, page_id: usize, page: &mut [u8; PAGE_SIZE]) {
        self.file
            .seek_read(page, (page_id * PAGE_SIZE) as u64)
            .expect("Failed to read page");
    }

    #[cfg(target_os = "linux")]
    fn read_page(&self, page_id: usize, block: &mut [u8; PAGE_SIZE]) {
        self.file
            .read_exact_at(&mut page.page, (page_id * PAGE_SIZE) as u64)
            .expect("Failed to read page");
    }

    #[cfg(target_os = "windows")]
    pub fn write_page(&self, page_id: usize, page: &PhysicalPage) {
        self.file
            .seek_write(&page.page, (page_id * PAGE_SIZE) as u64)
            .expect("Failed to write page");
    }

    #[cfg(target_os = "linux")]
    pub fn write_page(&self, page_id: usize, page: &PhysicalPage) {
        self.file
            .write_all_at(&page.page, (page_id * PAGE_SIZE) as u64)
            .expect("Failed to write page");
    }

    pub fn reserve_page(&mut self) -> usize {
        self.next_free_page += 1;
        self.next_free_page
    }

    pub fn reserve_range(&mut self, pages: usize) -> usize {
        self.next_free_page += pages;
        self.next_free_page
    }

    pub fn free_page_pointer(&self) -> usize {
        self.next_free_page
    }
}
