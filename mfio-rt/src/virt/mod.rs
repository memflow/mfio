//! Virtual Runtime
//!
//! This module implements a virtual runtime (and in-memory filesystem) that can be used as basis
//! for no_std implementations.
//!
//! Note that at the current moment this can be considered more as a toy example, rather than fully
//! featured implementation.

use alloc::{collections::BTreeMap, string::ToString, vec, vec::Vec};
use core::future::{pending, ready, Future, Ready};
use core::mem::drop;
use core::ops::Bound;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures::Stream;

use log::*;
use slab::Slab;

use mfio::backend::{BackendContainer, BackendHandle, DynBackend, IoBackend, PollingHandle};
use mfio::error::Result;
use mfio::io::*;
use mfio::mferr;
use mfio::stdeq::Seekable;

use crate::{
    util::path_filename_str, Component, DirEntry, DirHandle, DirOp, FileType, Fs, Metadata,
    OpenOptions, Path, PathBuf, Permissions,
};

#[cfg(not(feature = "virt-sync"))]
use alloc::rc::Rc;
#[cfg(feature = "virt-sync")]
use alloc::sync::Arc as Rc;

#[cfg(feature = "virt-sync")]
use mfio::locks::RwLock;

#[cfg(not(feature = "virt-sync"))]
use core::cell::{Ref, RefCell, RefMut};
#[cfg(not(feature = "virt-sync"))]
struct RwLock<T>(RefCell<T>);

#[cfg(not(feature = "virt-sync"))]
impl<T> RwLock<T> {
    fn new(v: T) -> Self {
        Self(RefCell::new(v))
    }

    fn read(&self) -> Ref<T> {
        self.0.borrow()
    }

    fn write(&self) -> RefMut<T> {
        self.0.borrow_mut()
    }
}

type Shared<T> = Rc<RwLock<T>>;
type InodeId = usize;

pub struct VirtRt {
    cwd: VirtDir,
    backend: BackendContainer<DynBackend>,
}

impl Default for VirtRt {
    fn default() -> Self {
        Self::new()
    }
}

impl VirtRt {
    pub fn new() -> Self {
        Self {
            cwd: VirtDir::default(),
            backend: BackendContainer::new_dyn(pending()),
        }
    }

    pub fn build<'a>(
        dirs: impl Iterator<Item = &'a str> + 'a,
        files: impl Iterator<Item = (&'a str, &'a [u8])> + 'a,
    ) -> Self {
        Self {
            cwd: VirtDir::from_fs(VirtFs::build(dirs, files).into()),
            backend: BackendContainer::new_dyn(pending()),
        }
    }
}

impl IoBackend for VirtRt {
    type Backend = DynBackend;

    fn polling_handle(&self) -> Option<PollingHandle> {
        None
    }

    fn get_backend(&self) -> BackendHandle<Self::Backend> {
        self.backend.acquire(None)
    }
}

impl Fs for VirtRt {
    type DirHandle<'a> = VirtDir;

    fn current_dir(&self) -> &Self::DirHandle<'_> {
        &self.cwd
    }
}

pub struct VirtDir {
    inode: Shared<Inode>,
    fs: Rc<VirtFs>,
}

impl Default for VirtDir {
    fn default() -> Self {
        Self::from_fs(Rc::new(VirtFs::default()))
    }
}

impl VirtDir {
    fn from_fs(fs: Rc<VirtFs>) -> Self {
        Self {
            inode: fs.root_dir.clone(),
            fs,
        }
    }

    // Internal sync implementations of directory-relative fs operations

    fn get_path(&self) -> Result<PathBuf> {
        Inode::path(self.inode.clone(), &self.fs.inodes.read())
            .ok_or_else(|| mferr!(Path, Invalid, Filesystem))
    }

    fn do_open_file(
        &self,
        path: &Path,
        options: OpenOptions,
    ) -> Result<<Self as DirHandle>::FileHandle> {
        path.parent()
            .and_then(|path| {
                let inodes = self.fs.inodes.read();
                Inode::walk_rel(self.inode.clone(), &self.fs.root_dir, path, &inodes)
            })
            // TODO: do we want to filter here, since we are returning None later anyways. The only
            // difference here is that we are doing so with read-only lock.
            .filter(|dir| matches!(dir.read().entry, InodeData::Dir(_)))
            .ok_or(mferr!(Directory, NotFound, Filesystem))
            .and_then(|dir| {
                let mut dir_guard = dir.write();
                let dir_guard = &mut *dir_guard;

                let InodeData::Dir(dir_entry) = &mut dir_guard.entry else {
                    unreachable!();
                };

                let Some(parent_id) = dir_guard.id else {
                    return Err(mferr!(Path, Removed, Filesystem));
                };

                let filename = path_filename_str(path).ok_or(mferr!(Path, Invalid, Filesystem))?;

                // creating new file may get triggered from 2 branches
                let new_file = |entries: &mut BTreeMap<_, _>| {
                    let mut inodes = self.fs.inodes.write();
                    let entry = inodes.vacant_entry();
                    let id = entry.key();

                    let name: Rc<str> = filename.into();

                    let inode = entry
                        .insert(Rc::new(RwLock::new(Inode {
                            id: Some(id),
                            parent_link: Some(ParentLink {
                                name: name.clone(),
                                parent: parent_id,
                            }),
                            entry: InodeData::File(VirtFileInner { data: vec![] }),
                            metadata: Metadata::empty_file(Permissions {}, None),
                        })))
                        .clone();

                    entries.insert(name.clone(), id);

                    Ok(VirtFile { inode, options })
                };

                // Verify open options
                if options.create_new {
                    if options.write && !dir_entry.entries.contains_key(filename) {
                        new_file(&mut dir_entry.entries)
                    } else {
                        Err(mferr!(File, AlreadyExists, Filesystem))
                    }
                } else if let Some(&id) = dir_entry.entries.get(filename) {
                    let inodes = self.fs.inodes.read();
                    // Different from file not found, because it's the whole inode that's gone, and
                    // this is not expected.
                    let inode = inodes
                        .get(id)
                        .ok_or(mferr!(Entry, NotFound, Filesystem))?
                        .clone();

                    // Verify that we've got a file in both of the branches, except we are holding
                    // different types of locks (read vs write).
                    if options.truncate {
                        if options.write {
                            let mut inode = inode.write();
                            let InodeData::File(file) = &mut inode.entry else {
                                return Err(mferr!(Entry, Invalid, Filesystem));
                            };
                            file.data.clear();
                            inode.metadata.len = 0;
                        } else {
                            return Err(mferr!(Argument, Unsupported, Filesystem));
                        }
                    } else if !matches!(inode.read().entry, InodeData::File(_)) {
                        return Err(mferr!(Entry, Invalid, Filesystem));
                    }

                    Ok(VirtFile { inode, options })
                } else if options.create && options.write {
                    new_file(&mut dir_entry.entries)
                } else {
                    Err(mferr!(File, NotFound, Filesystem))
                }
            })
            .map(Seekable::from)
    }

    fn set_permissions(&self, path: &Path, permissions: Permissions) -> Result<()> {
        let inodes = self.fs.inodes.read();
        let node = Inode::walk_rel(self.inode.clone(), &self.fs.root_dir, path, &inodes)
            .ok_or(mferr!(Path, NotFound, Filesystem))?;
        let mut node = node.write();
        // TODO: check whether the caller is authorized
        node.metadata.permissions = permissions;
        // TODO: change mtime
        Ok(())
    }

    /// Unlink from given parent
    ///
    /// # Panics
    ///
    /// This function panics if `parent` is not the parent of `node`.
    fn unlink_with_parent(node: &mut Inode, parent_node: &mut Inode) {
        if let Some(ParentLink { name, parent }) = node.parent_link.take() {
            assert_eq!(parent, parent_node.id.unwrap());
            let InodeData::Dir(parent) = &mut parent_node.entry else {
                panic!("Parent changed from dir to file")
            };
            parent.entries.remove(&name);
        }
    }

    fn unlink(node: &mut Inode, inodes: &mut Slab<Shared<Inode>>) {
        if let Some(ParentLink { parent, .. }) = &node.parent_link {
            let parent = inodes
                .get(*parent)
                .expect("No parent inode found. This indicates buggy unlinking")
                .clone();
            let mut parent = parent.write();
            Self::unlink_with_parent(node, &mut parent)
        }
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        let mut inodes = self.fs.inodes.write();
        let node = Inode::walk_rel(self.inode.clone(), &self.fs.root_dir, path, &inodes)
            .ok_or(mferr!(Path, NotFound, Filesystem))?;
        let mut node = node.write();

        let InodeData::Dir(dir) = &node.entry else {
            return Err(mferr!(Entry, Invalid, Filesystem));
        };

        if !dir.entries.is_empty() {
            return Err(mferr!(Directory, InUse, Filesystem));
        }

        if let Some(id) = node.id.take() {
            inodes.remove(id);
        }

        Self::unlink(&mut node, &mut inodes);

        Ok(())
    }

    fn remove_dir_all(&self, path: &Path) -> Result<()> {
        let mut inodes = self.fs.inodes.write();
        let node = Inode::walk_rel(self.inode.clone(), &self.fs.root_dir, path, &inodes)
            .ok_or(mferr!(Path, NotFound, Filesystem))?;

        let mut stack = vec![node];

        while let Some(head) = stack.pop() {
            let mut node = head.write();

            let InodeData::Dir(dir) = &mut node.entry else {
                return Err(mferr!(Entry, Invalid, Filesystem));
            };

            while let Some((_, id)) = dir.entries.pop_last() {
                let inode = inodes.get(id).expect("entry exists, with no inode").clone();
                let mut inode_guard = inode.write();
                match inode_guard.entry {
                    InodeData::File(_) => {
                        inodes.remove(inode_guard.id.take().expect("parent does not exist"));
                    }
                    InodeData::Dir(_) => {
                        drop(inode_guard);
                        stack.push(head.clone());
                        stack.push(inode);
                        break;
                    }
                }
            }

            if dir.entries.is_empty() {
                if let Some(id) = node.id.take() {
                    inodes.remove(id);
                }

                Self::unlink(&mut node, &mut inodes);
            }
        }

        Ok(())
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let filename = path_filename_str(path).ok_or(mferr!(Path, Invalid, Filesystem))?;

        let mut inodes = self.fs.inodes.write();
        let node = Inode::walk_rel(
            self.inode.clone(),
            &self.fs.root_dir,
            path.parent().ok_or(mferr!(Path, NotFound, Filesystem))?,
            &inodes,
        )
        .ok_or(mferr!(Path, NotFound, Filesystem))?;

        let mut node = node.write();

        let Some(parent) = node.id else {
            return Err(mferr!(Path, Removed, Filesystem));
        };

        let InodeData::Dir(parent_dir) = &mut node.entry else {
            return Err(mferr!(Path, NotFound, Filesystem));
        };

        if parent_dir.entries.contains_key(filename) {
            Err(mferr!(Directory, AlreadyExists, Filesystem))
        } else {
            let filename: Rc<str> = Rc::from(filename);

            let entry = inodes.vacant_entry();
            let id = entry.key();
            entry.insert(Rc::new(RwLock::new(Inode {
                id: Some(id),
                metadata: Metadata::empty_dir(Permissions {}, None),
                entry: InodeData::Dir(VirtDirInner {
                    entries: Default::default(),
                }),
                parent_link: Some(ParentLink {
                    name: filename.clone(),
                    parent,
                }),
            })));

            parent_dir.entries.insert(filename, id);

            Ok(())
        }
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let mut inodes = self.fs.inodes.write();
        let mut cur_node = self.inode.clone();

        let components = path.components();

        for component in components {
            let mut cur_inode = cur_node.write();

            let Some(parent) = cur_inode.id else {
                return Err(mferr!(Path, Removed, Filesystem));
            };

            let next_node = match component {
                Component::RootDir => self.fs.root_dir.clone(),
                Component::ParentDir => {
                    // If we are at the root, then going up means cycling back.
                    if let Some(link) = &cur_inode.parent_link {
                        inodes
                            .get(link.parent)
                            .ok_or(mferr!(Path, Invalid, Filesystem))?
                            .clone()
                    } else {
                        continue;
                    }
                }
                Component::Normal(n) => {
                    #[cfg(feature = "std")]
                    let n = n.to_str().ok_or(mferr!(Path, Invalid, Filesystem))?;
                    #[cfg(not(feature = "std"))]
                    let n =
                        core::str::from_utf8(n).map_err(|_| mferr!(Path, Invalid, Filesystem))?;

                    let InodeData::Dir(d) = &mut cur_inode.entry else {
                        return Err(mferr!(Path, Invalid, Filesystem));
                    };
                    let inode = d.entries.get(n);

                    if let Some(&inode) = inode {
                        inodes
                            .get(inode)
                            .ok_or(mferr!(Path, Invalid, Filesystem))?
                            .clone()
                    } else {
                        let entry = inodes.vacant_entry();
                        let id = entry.key();
                        let name: Rc<str> = Rc::from(n);
                        d.entries.insert(name.clone(), id);
                        let entry = entry
                            .insert(Rc::new(RwLock::new(Inode {
                                id: Some(id),
                                parent_link: Some(ParentLink { parent, name }),
                                entry: InodeData::Dir(VirtDirInner {
                                    entries: Default::default(),
                                }),
                                metadata: Metadata::empty_dir(Permissions {}, None),
                            })))
                            .clone();
                        entry
                    }
                }
                _ => continue,
            };

            drop(cur_inode);
            cur_node = next_node;
        }

        Ok(())
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        let mut inodes = self.fs.inodes.write();
        let node = Inode::walk_rel(self.inode.clone(), &self.fs.root_dir, path, &inodes)
            .ok_or(mferr!(Path, NotFound, Filesystem))?;
        let mut node = node.write();

        let InodeData::File(_) = &node.entry else {
            return Err(mferr!(Entry, Invalid, Filesystem));
        };

        if let Some(id) = node.id.take() {
            inodes.remove(id);
        }

        Self::unlink(&mut node, &mut inodes);

        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let mut inodes = self.fs.inodes.write();

        let from = Inode::walk_rel(self.inode.clone(), &self.fs.root_dir, from, &inodes)
            .ok_or(mferr!(Path, NotFound, Filesystem))?;

        let to_parent = to.parent().ok_or(mferr!(Path, Unavailable, Filesystem))?;
        let to_filename = to.file_name().ok_or(mferr!(Path, Invalid, Filesystem))?;
        #[cfg(feature = "std")]
        let to_filename = to_filename
            .to_str()
            .ok_or(mferr!(Path, Invalid, Filesystem))?;
        #[cfg(not(feature = "std"))]
        let to_filename =
            core::str::from_utf8(to_filename).map_err(|_| mferr!(Path, Invalid, Filesystem))?;

        let to_parent = Inode::walk_rel(self.inode.clone(), &self.fs.root_dir, to_parent, &inodes)
            .ok_or(mferr!(Path, NotFound, Filesystem))?;

        // We need to make sure that `to_parent` is not child of `from`.
        // I wonder, what if we didn't check for this? We could create "hidden islands" of sorts.
        {
            let mut tmp = from.clone();
            while let Some(node) =
                Inode::walk_rel(tmp, &self.fs.root_dir, Path::new("../"), &inodes)
            {
                if Rc::as_ptr(&node) == Rc::as_ptr(&to_parent) {
                    return Err(mferr!(Path, Unavailable, Filesystem));
                } else if Rc::as_ptr(&node) == Rc::as_ptr(&self.fs.root_dir) {
                    break;
                } else {
                    tmp = node;
                }
            }
        }

        let mut to_parent = to_parent.write();
        let InodeData::Dir(to_parent_dir) = &mut to_parent.entry else {
            return Err(mferr!(Path, Invalid, Filesystem));
        };

        let mut from = from.write();

        // Remove old entry, if we can
        // This way we can also reuse the old arc for the name, without performing a new allocation
        let to_filename =
            if let Some((name, entry)) = to_parent_dir.entries.get_key_value(to_filename) {
                let entry = inodes
                    .get(*entry)
                    .ok_or(mferr!(Path, Unavailable, Filesystem))?
                    .clone();
                let mut entry = entry.write();
                if let InodeData::Dir(v) = &entry.entry {
                    if !v.entries.is_empty() || matches!(from.entry, InodeData::File(_)) {
                        return Err(mferr!(Directory, InUse, Filesystem));
                    }
                }
                let name = name.clone();
                Self::unlink_with_parent(&mut entry, &mut to_parent);
                name
            } else {
                Rc::from(to_filename)
            };

        let InodeData::Dir(to_parent_dir) = &mut to_parent.entry else {
            unreachable!()
        };

        Self::unlink(&mut from, &mut inodes);

        // Finally, relink to the parent
        to_parent_dir.entries.insert(
            to_filename.clone(),
            from.id
                .expect("moving a file with no inode, this shouldn't happen"),
        );

        from.parent_link = Some(ParentLink {
            name: to_filename,
            parent: to_parent
                .id
                .expect("parent dir with no inode, this shouldn't happen"),
        });

        Ok(())
    }

    fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let mut inodes = self.fs.inodes.write();

        let from = Inode::walk_rel(self.inode.clone(), &self.fs.root_dir, from, &inodes)
            .ok_or(mferr!(Path, NotFound, Filesystem))?;

        let to_parent = to.parent().ok_or(mferr!(Path, Unavailable, Filesystem))?;
        let to_filename = to.file_name().ok_or(mferr!(Path, Invalid, Filesystem))?;
        #[cfg(feature = "std")]
        let to_filename = to_filename
            .to_str()
            .ok_or(mferr!(Path, Invalid, Filesystem))?;
        #[cfg(not(feature = "std"))]
        let to_filename =
            core::str::from_utf8(to_filename).map_err(|_| mferr!(Path, Invalid, Filesystem))?;

        let to_parent = Inode::walk_rel(self.inode.clone(), &self.fs.root_dir, to_parent, &inodes)
            .ok_or(mferr!(Path, NotFound, Filesystem))?;

        let mut to_parent = to_parent.write();
        let InodeData::Dir(to_parent_dir) = &mut to_parent.entry else {
            return Err(mferr!(Path, Invalid, Filesystem));
        };

        let mut from = from.write();
        let InodeData::File(from_file) = &mut from.entry else {
            return Err(mferr!(Path, Invalid, Filesystem));
        };

        // Remove old entry, if we can
        // This way we can also reuse the old arc for the name, without performing a new allocation
        let to_filename =
            if let Some((name, entry)) = to_parent_dir.entries.get_key_value(to_filename) {
                let entry = inodes
                    .get(*entry)
                    .ok_or(mferr!(Path, Unavailable, Filesystem))?
                    .clone();
                let mut entry = entry.write();
                if let InodeData::Dir(v) = &entry.entry {
                    if !v.entries.is_empty() {
                        return Err(mferr!(Directory, InUse, Filesystem));
                    }
                }
                let name = name.clone();
                Self::unlink_with_parent(&mut entry, &mut to_parent);
                name
            } else {
                Rc::from(to_filename)
            };

        let Some(parent) = to_parent.id else {
            return Err(mferr!(Path, Removed, Filesystem));
        };

        let InodeData::Dir(to_parent_dir) = &mut to_parent.entry else {
            unreachable!()
        };

        let new_entry = inodes.vacant_entry();
        let id = new_entry.key();
        new_entry.insert(Rc::new(RwLock::new(Inode {
            entry: InodeData::File(VirtFileInner {
                data: from_file.data.clone(),
            }),
            id: Some(id),
            parent_link: Some(ParentLink {
                name: to_filename.clone(),
                parent,
            }),
            // TODO: we need to update time here (when we have access to time).
            metadata: from.metadata.clone(),
        })));

        // Finally, link up to the parent
        to_parent_dir.entries.insert(to_filename.clone(), id);

        Ok(())
    }
}

impl DirHandle for VirtDir {
    type FileHandle = Seekable<VirtFile, u64>;
    type OpenFileFuture<'a> = OpenFileFuture<'a>;
    type PathFuture<'a> = Ready<Result<PathBuf>>;
    type OpenDirFuture<'a> = Ready<Result<Self>>;
    type ReadDir<'a> = ReadDir<'a>;
    type ReadDirFuture<'a> = Ready<Result<ReadDir<'a>>>;
    type MetadataFuture<'a> = Ready<Result<Metadata>>;
    type OpFuture<'a> = OpFuture<'a>;

    /// Gets the absolute path to this `DirHandle`.
    fn path(&self) -> Self::PathFuture<'_> {
        ready(self.get_path())
    }

    /// Reads the directory contents.
    ///
    /// Iterating directories has the complexity of `O(n log(n))`, because we are not maintaining
    /// the cursor to the directory.
    ///
    /// TODO: do not go back for entries after every iteration, instead, cache up a few entries.
    fn read_dir(&self) -> Self::ReadDirFuture<'_> {
        ready(Ok(ReadDir {
            dir: self,
            cur: None,
        }))
    }

    /// Opens a file.
    ///
    /// This function accepts an absolute or relative path to a file for reading. If the path is
    /// relative, it is opened relative to this `DirHandle`.
    fn open_file<'a, P: AsRef<Path> + ?Sized>(
        &'a self,
        path: &'a P,
        options: OpenOptions,
    ) -> Self::OpenFileFuture<'a> {
        OpenFileFuture {
            dir: self,
            path: path.as_ref(),
            options,
        }
    }

    /// Opens a directory.
    ///
    /// This function accepts an absolute or relative path to a directory for reading. If the path
    /// is relative, it is opened relative to this `DirHandle`.
    fn open_dir<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::OpenDirFuture<'a> {
        // We do not write anything here, therefore
        let inode = {
            let inodes = self.fs.inodes.read();
            Inode::walk_rel(
                self.inode.clone(),
                &self.fs.root_dir,
                path.as_ref(),
                &inodes,
            )
        };

        let ret = inode
            // TODO: do we want to filter here, since we are returning None later anyways. The only
            // difference here is that we are doing so with read-only lock.
            .filter(|dir| matches!(dir.read().entry, InodeData::Dir(_)))
            .ok_or(mferr!(Directory, NotFound, Filesystem))
            .map(|inode| VirtDir {
                inode,
                fs: self.fs.clone(),
            });

        ready(ret)
    }

    fn metadata<'a, P: AsRef<Path> + ?Sized>(&'a self, path: &'a P) -> Self::MetadataFuture<'a> {
        let inodes = self.fs.inodes.read();

        let ret = Inode::walk_rel(
            self.inode.clone(),
            &self.fs.root_dir,
            path.as_ref(),
            &inodes,
        )
        .map(|v| v.read().metadata.clone())
        .ok_or(mferr!(Entry, NotFound, Filesystem));

        ready(ret)
    }

    /// Do an operation.
    ///
    /// This function performs an operation from the [`DirOp`] enum.
    fn do_op<'a, P: AsRef<Path> + ?Sized>(&'a self, operation: DirOp<&'a P>) -> Self::OpFuture<'a> {
        OpFuture {
            dir: self,
            operation: Some(operation.into_path()),
        }
    }
}

// VirtDir's DirHandle futures:
// TODO: Once impl_trait_in_assoc_type is stabilized (https://github.com/rust-lang/rust/issues/63063),
// we can remove these in favor of async fns in traits.

pub struct OpFuture<'a> {
    dir: &'a VirtDir,
    operation: Option<DirOp<&'a Path>>,
}

impl Future for OpFuture<'_> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context) -> Poll<Self::Output> {
        //let this = unsafe { self.get_unchecked_mut() }
        //Poll::Ready(self.dir.do_open_file(self.path, self.options))
        let ret = match self.operation.take().unwrap() {
            DirOp::SetPermissions { path, permissions } => {
                self.dir.set_permissions(path, permissions)
            }
            DirOp::RemoveDir { path } => self.dir.remove_dir(path),
            DirOp::RemoveDirAll { path } => self.dir.remove_dir_all(path),
            DirOp::CreateDir { path } => self.dir.create_dir(path),
            DirOp::CreateDirAll { path } => self.dir.create_dir_all(path),
            DirOp::RemoveFile { path } => self.dir.remove_file(path),
            DirOp::Rename { from, to } => self.dir.rename(from, to),
            DirOp::Copy { from, to } => self.dir.copy(from, to),
            DirOp::HardLink { .. } => Err(mferr!(Operation, NotImplemented, Filesystem)),
        };

        Poll::Ready(ret)
    }
}

pub struct OpenFileFuture<'a> {
    dir: &'a VirtDir,
    path: &'a Path,
    options: OpenOptions,
}

impl Future for OpenFileFuture<'_> {
    type Output = Result<<VirtDir as DirHandle>::FileHandle>;

    fn poll(self: Pin<&mut Self>, _: &mut Context) -> Poll<Self::Output> {
        Poll::Ready(self.dir.do_open_file(self.path, self.options))
    }
}

pub struct ReadDir<'a> {
    dir: &'a VirtDir,
    cur: Option<Rc<str>>,
}

impl Stream for ReadDir<'_> {
    type Item = Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        let dir = this.dir.inode.read();

        let InodeData::Dir(dir) = &dir.entry else {
            return Poll::Ready(None);
        };

        let next = if let Some(cur) = this.cur.clone() {
            dir.entries
                .range((Bound::Excluded(cur), Bound::Unbounded))
                .next()
        } else {
            dir.entries.iter().next()
        };

        Poll::Ready(next.map(|(k, v)| {
            this.cur = Some(k.clone());
            this.dir
                .fs
                .inodes
                .read()
                .get(*v)
                .map(|v| v.read())
                .as_ref()
                .map(|v| v.entry.to_dir_entry(k.clone()))
                .ok_or(mferr!(Entry, Corrupted, Filesystem))
        }))
    }
}

// Core filesystem container:

pub struct VirtFs {
    inodes: RwLock<Slab<Shared<Inode>>>,
    root_dir: Shared<Inode>,
}

impl VirtFs {
    /// Build a filesystem from directory and file list.
    ///
    /// Builds a filesystem instance from given data. This is generally more efficient than
    /// manually creating all entries (without caching), because the build step is able to directly
    /// access all inodes.
    pub fn build<'a>(
        dirs: impl Iterator<Item = &'a str> + 'a,
        files: impl Iterator<Item = (&'a str, &'a [u8])> + 'a,
    ) -> Self {
        let mut inode_cache = BTreeMap::new();

        let mut inodes = Slab::new();

        let entry = inodes.vacant_entry();
        let id = entry.key();

        let root_dir = Rc::new(RwLock::new(Inode {
            id: Some(id),
            parent_link: None,
            entry: InodeData::Dir(VirtDirInner {
                entries: Default::default(),
            }),
            metadata: Metadata::empty_dir(Permissions {}, None),
        }));

        entry.insert(root_dir.clone());

        let insert_entry = |inodes: &mut Slab<_>, parent_id, p: &str, entry, metadata| {
            let new_entry = inodes.vacant_entry();
            let id = new_entry.key();

            let name: Rc<str> = p.into();

            new_entry.insert(Rc::new(RwLock::new(Inode {
                id: Some(id),
                parent_link: Some(ParentLink {
                    parent: parent_id,
                    name: name.clone(),
                }),
                entry,
                metadata,
            })));

            let parent = inodes.get(parent_id).unwrap();
            let mut parent = parent.write();
            let InodeData::Dir(d) = &mut parent.entry else {
                unreachable!()
            };
            d.entries.insert(name, id);

            id
        };

        for dir in dirs {
            let mut d = String::new();
            // Root inode ID
            let mut parent_id = id;
            for p in dir.split('/') {
                if !d.is_empty() {
                    d.push('/');
                }
                d.push_str(p);

                parent_id = *inode_cache.entry(d.clone()).or_insert_with(|| {
                    insert_entry(
                        &mut inodes,
                        parent_id,
                        p,
                        InodeData::Dir(VirtDirInner::default()),
                        Metadata::empty_dir(Permissions {}, None),
                    )
                });
            }
        }

        for (path, data) in files {
            let mut d = String::new();
            // Root inode ID
            let mut parent_id = id;
            for p in path.split('/') {
                if !d.is_empty() {
                    d.push('/');
                }
                d.push_str(p);

                parent_id = *inode_cache.entry(d.clone()).or_insert_with(|| {
                    insert_entry(
                        &mut inodes,
                        parent_id,
                        p,
                        InodeData::Dir(VirtDirInner::default()),
                        Metadata::empty_dir(Permissions {}, None),
                    )
                });
            }

            let inode = inode_cache.get(path).unwrap();
            let inode = inodes.get(*inode).unwrap();
            let mut inode = inode.write();

            {
                let InodeData::Dir(v) = &inode.entry else {
                    unreachable!()
                };
                assert!(v.entries.is_empty());
            }

            inode.metadata.len = data.len() as _;
            inode.entry = InodeData::File(VirtFileInner { data: data.into() });
        }

        let inodes = RwLock::new(inodes);

        Self { inodes, root_dir }
    }
}

impl Default for VirtFs {
    fn default() -> Self {
        let mut inodes = Slab::new();

        let entry = inodes.vacant_entry();
        let id = entry.key();

        let root_dir = Rc::new(RwLock::new(Inode {
            id: Some(id),
            parent_link: None,
            entry: InodeData::Dir(VirtDirInner {
                entries: Default::default(),
            }),
            metadata: Metadata::empty_dir(Permissions {}, None),
        }));

        entry.insert(root_dir.clone());

        let inodes = RwLock::new(inodes);

        Self { inodes, root_dir }
    }
}

// File implementation:

pub struct VirtFile {
    inode: Shared<Inode>,
    options: OpenOptions,
}

impl PacketIo<Write, u64> for VirtFile {
    fn send_io(&self, pos: u64, pkt: BoundPacketView<Write>) {
        if self.options.read {
            let node = self.inode.read();

            let InodeData::File(file) = &node.entry else {
                core::mem::drop(node);
                pkt.error(mferr!(File, Unreadable, Filesystem));
                return;
            };

            if pos < file.data.len() as u64 {
                let split_pos = (file.data.len() as u64).saturating_sub(pos);
                if split_pos < pkt.len() {
                    let (a, b) = pkt.split_at(split_pos);

                    let transferred =
                        unsafe { a.transfer_data(file.data[(pos as usize)..].as_ptr().cast()) };

                    core::mem::drop(node);
                    core::mem::drop(transferred);
                    b.error(mferr!(Position, Outside, Filesystem));
                } else {
                    let transferred =
                        unsafe { pkt.transfer_data(file.data[(pos as usize)..].as_ptr().cast()) };
                    core::mem::drop(node);
                    core::mem::drop(transferred);
                }
            } else {
                core::mem::drop(node);
                pkt.error(mferr!(Position, Outside, Filesystem));
            }
        } else {
            pkt.error(mferr!(Io, PermissionDenied, Filesystem));
        }
    }
}

impl PacketIo<Read, u64> for VirtFile {
    fn send_io(&self, pos: u64, pkt: BoundPacketView<Read>) {
        if self.options.write {
            let mut node = self.inode.write();

            let InodeData::File(file) = &mut node.entry else {
                core::mem::drop(node);
                pkt.error(mferr!(File, Unreadable, Filesystem));
                return;
            };

            // TODO: we may want to have sparse files
            let needed_len = (pos + pkt.len()) as usize;
            if needed_len > file.data.len() {
                file.data.resize(needed_len, 0);
            }

            let transferred =
                unsafe { pkt.transfer_data(file.data[(pos as usize)..].as_mut_ptr().cast()) };

            core::mem::drop(node);
            core::mem::drop(transferred);
        } else {
            pkt.error(mferr!(Io, PermissionDenied, Filesystem));
        }
    }
}

// Internal data structures:

struct Inode {
    // handles to this Inode's arc. Therefore, we need to signify somehow that this inode was
    // already deleted, and that it cannot be relinked anywhere.
    id: Option<InodeId>,
    parent_link: Option<ParentLink>,
    entry: InodeData,
    metadata: Metadata,
}

impl Inode {
    /// Walks the filesystem to reach a specified directory.
    ///
    /// Returns `None` if the resulting path, or given inode are not directories.
    fn walk_rel(
        mut cur_node: Shared<Inode>,
        root_dir: &Shared<Inode>,
        path: &Path,
        inodes: &Slab<Shared<Inode>>,
    ) -> Option<Shared<Inode>> {
        trace!("Walk rel: {path:?}");

        let components = path.components();

        for component in components {
            let inode = cur_node.read();

            let next_node = match component {
                Component::RootDir => root_dir.clone(),
                Component::ParentDir => {
                    // If we are at the root, then going up means cycling back.
                    if let Some(link) = &inode.parent_link {
                        inodes.get(link.parent)?.clone()
                    } else {
                        continue;
                    }
                }
                Component::Normal(n) => {
                    #[cfg(feature = "std")]
                    let n = n.to_str()?;
                    #[cfg(not(feature = "std"))]
                    let n = core::str::from_utf8(n).ok()?;

                    let InodeData::Dir(d) = &inode.entry else {
                        return None;
                    };

                    trace!("Entries: {:?}", d.entries);

                    let inode = *d.entries.get(n)?;

                    inodes.get(inode)?.clone()
                }
                _ => continue,
            };

            drop(inode);
            cur_node = next_node;
        }

        Some(cur_node)
    }

    fn path(mut cur_node: Shared<Inode>, inodes: &Slab<Shared<Inode>>) -> Option<PathBuf> {
        let mut segments = vec![];

        loop {
            let cn = cur_node.read();

            if let Some(link) = &cn.parent_link {
                segments.push(link.name.clone());
                let parent = inodes.get(link.parent)?.clone();
                drop(cn);
                cur_node = parent;
            } else {
                break;
            }
        }

        let mut res = PathBuf::new();

        while let Some(seg) = segments.pop() {
            res.push(&*seg);
        }

        Some(res)
    }
}

#[derive(Clone)]
struct ParentLink {
    name: Rc<str>,
    parent: InodeId,
}

enum InodeData {
    File(VirtFileInner),
    Dir(VirtDirInner),
}

impl InodeData {
    fn to_dir_entry(&self, name: Rc<str>) -> DirEntry {
        DirEntry {
            name: name.to_string(),
            ty: match self {
                Self::File(_) => FileType::File,
                Self::Dir(_) => FileType::Directory,
            },
        }
    }
}

#[derive(Default)]
struct VirtDirInner {
    entries: BTreeMap<Rc<str>, InodeId>,
}

struct VirtFileInner {
    // TODO: support sparse files
    data: Vec<u8>,
}
#[cfg(not(miri))]
macro_rules! test_suite {
    ($($tt:tt)*) => {
        #[cfg(test)]
        $crate::test_suite!($($tt)*);
    }
}

// Walking directories is currently extremely slow. So let's not do O(n^2) tests for now.
#[cfg(miri)]
macro_rules! test_suite {
    ($($tt:tt)*) => {
        #[cfg(test)]
        $crate::test_suite_base!(
            $($tt)*,
            files_equal,
            files_equal_rel,
            dirs_equal,
            writes_equal,
            writes_equal_rel
        );
    }
}

test_suite!(tests_default, |_, closure| {
    use super::VirtRt;
    #[cfg(not(miri))]
    let _ = ::env_logger::builder().is_test(true).try_init();
    let mut rt = VirtRt::build(
        CTX.dirs().iter().map(|v| v.as_str()),
        CTX.files().iter().map(|(n, d)| (n.as_str(), &d[..])),
    );
    let rt = staticify(&mut rt);

    pub fn run<'a, Func: FnOnce(&'a VirtRt) -> F, F: Future>(
        fs: &'a mut VirtRt,
        func: Func,
    ) -> F::Output {
        fs.block_on(func(fs))
    }

    run(rt, |rt| async move {
        let run = TestRun::assume_built(rt, ());
        closure(run).await
    });
});
