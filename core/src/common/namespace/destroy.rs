//! Filesystem ownership and durability for namespace destruction.
use super::{NsLifecycleState, NsRecord};
use crate::common::{
    error::{Result, corrupt},
    universe,
};
use cap_fs_ext::{DirExt, MetadataExt, OpenOptionsMaybeDirExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, DirBuilder, Metadata, OpenOptions},
};
use std::{
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
};

fn existing<T>(result: io::Result<T>) -> io::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn parent_and_name(path: &Path) -> io::Result<(&Path, &OsStr)> {
    path.parent().zip(path.file_name()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "namespace root has no parent entry",
        )
    })
}

fn sync_dir(dir: &Dir) -> io::Result<()> {
    // A capability may be an O_PATH descriptor. Open the same directory for I/O.
    dir.open_with(".", OpenOptions::new().read(true).maybe_dir(true))?
        .sync_all()
}

fn sync_existing_dir(dir: &Dir) -> io::Result<()> {
    match sync_dir(dir) {
        Ok(()) => Ok(()),
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::PermissionDenied
                    | io::ErrorKind::InvalidInput
                    | io::ErrorKind::ReadOnlyFilesystem
            ) =>
        {
            Ok(())
        }
        Err(err) => Err(err),
    }
}

/// Retain a parent handle only when it can also be opened to synchronize
/// directory-entry changes. On Linux, `open_ambient_dir` returns an `O_PATH`
/// descriptor even on `--x` (`0o111`) or `-wx` (`0o311`) directories whose
/// entries cannot subsequently be fenced with `fsync`.
fn syncable_parent(dir: Dir) -> io::Result<Option<Dir>> {
    #[cfg(not(windows))]
    {
        match dir.open_with(".", OpenOptions::new().read(true).maybe_dir(true)) {
            Ok(file) => Ok(Some(Dir::from_std_file(file.into_std()))),
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => Ok(None),
            Err(err) => Err(err),
        }
    }
    #[cfg(windows)]
    {
        Ok(Some(dir))
    }
}

fn sync_absence(path: &Path) -> io::Result<()> {
    for ancestor in path.ancestors().skip(1) {
        let ancestor = if ancestor.as_os_str().is_empty() {
            Path::new(".")
        } else {
            ancestor
        };
        let dir = match Dir::open_ambient_dir(ancestor, ambient_authority()) {
            Ok(dir) => dir,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        return sync_existing_dir(&dir);
    }
    Err(io::ErrorKind::NotFound.into())
}

pub(super) struct Target {
    directory: Option<(Dir, Name)>,
    alias: Option<(Name, Identity)>,
}

struct Identity {
    metadata: Metadata,
    link: Option<PathBuf>,
}

impl Identity {
    fn matches(&self, dir: &Dir, name: &OsStr) -> io::Result<bool> {
        if !existing(dir.symlink_metadata(name))?
            .is_some_and(|m| same_entry(&m, &self.metadata))
        {
            return Ok(false);
        }
        // A removed symlink's inode can be reused while the target is held.
        match &self.link {
            Some(target) => {
                Ok(existing(dir.read_link_contents(name))?.as_ref() == Some(target))
            }
            None => Ok(true),
        }
    }
}

/// Pin the directory whose entry will be removed and fenced. An adopted
/// root under a traverse-only parent may have to stay in place.
struct Name {
    path: PathBuf,
    parent: Option<Dir>,
}

impl Name {
    fn lock_directory(placement: &Path) -> io::Result<(Dir, Self)> {
        let path = placement.canonicalize()?;
        let (parent, leaf) = parent_and_name(&path)?;
        let parent_cap = match Dir::open_ambient_dir(parent, ambient_authority()) {
            Ok(dir) => Some(dir),
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => None,
            Err(e) => return Err(e),
        };
        let dir = match &parent_cap {
            Some(parent) => parent.open_dir_nofollow(leaf)?,
            None => Dir::open_ambient_dir(&path, ambient_authority())?,
        };
        let parent = match parent_cap {
            Some(parent) => syncable_parent(parent)?,
            None => None,
        };
        let lease = universe::lock_directory(dir)?;
        Ok((Dir::from_std_file(lease), Self { path, parent }))
    }
}

impl Target {
    pub(super) fn check(root: &Path, record: &NsRecord) -> Result<Self> {
        let mut placement = root.to_owned();
        let alias =
            if existing(fs::symlink_metadata(root))?.is_some_and(|m| m.is_symlink()) {
                let (parent_path, leaf) = parent_and_name(root)?;
                let parent = Dir::open_ambient_dir(parent_path, ambient_authority())?;
                if let Some(meta) =
                    existing(parent.symlink_metadata(leaf))?.filter(Metadata::is_symlink)
                    && let Some(link) = existing(parent.read_link_contents(leaf))?
                {
                    placement = parent_path.join(&link);
                    Some((
                        Name {
                            path: root.to_owned(),
                            parent: syncable_parent(parent)?,
                        },
                        Identity {
                            metadata: meta,
                            link: Some(link),
                        },
                    ))
                } else {
                    None
                }
            } else {
                None
            };
        let directory = existing(Name::lock_directory(&placement))?;
        if let Some((dir, name)) = &directory {
            universe::validate_namespace_dir(
                dir,
                &name.path,
                universe::active()?.id,
                record.id,
                record.shards as usize,
                record.state == NsLifecycleState::Pending,
            )?;
        } else if alias.is_some() {
            return Err(corrupt(format!(
                "dangling symlink at namespace root {}",
                root.display()
            )));
        } else {
            // Fence the missing entry while a failed call can still be retried by id.
            sync_absence(&placement)?;
        }
        Ok(Self { directory, alias })
    }

    pub(super) fn remove(self) -> Result<()> {
        if let Some((dir, name)) = self.directory {
            #[cfg(test)]
            super::tests::before_root_wipe(&name.path);
            for entry in dir.entries()? {
                let entry = entry?;
                let removed = match entry.file_type() {
                    Ok(kind) if kind.is_dir() => dir.remove_dir_all(entry.file_name()),
                    Ok(_) => dir.remove_file_or_symlink(entry.file_name()),
                    Err(err) => Err(err),
                };
                existing(removed)?;
            }
            if dir.entries()?.next().transpose()?.is_some() {
                return Err(io::Error::from(io::ErrorKind::DirectoryNotEmpty).into());
            }
            let identity = Identity {
                metadata: dir.dir_metadata()?,
                link: None,
            };
            let lease = dir.into_std_file();
            // The directory may survive under another name or as a mount point.
            lease.sync_all()?;
            #[cfg(windows)]
            drop(lease); // A directory capability denies rename while it is open.
            remove_entry(name, &identity)?;
        }
        if let Some((name, identity)) = self.alias {
            remove_entry(name, &identity)?;
        }
        Ok(())
    }
}

fn same_entry(a: &Metadata, b: &Metadata) -> bool {
    a.file_type() == b.file_type() && a.dev() == b.dev() && a.ino() == b.ino()
}

/// Move a public name into a private directory before its final unlink. A
/// rename between the initial comparison and our rename is detected inside
/// the private directory, while the captured object can still be restored.
fn remove_entry(location: Name, expected: &Identity) -> io::Result<()> {
    let path = &location.path;
    let (parent_path, name) = parent_and_name(path)?;
    let Some(parent) = location.parent else {
        if !expected.metadata.is_dir() {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        // An unreadable parent cannot be opened to fence a directory-entry
        // change, so keep the emptied, synced adopted root in place.
        if let Some(current) =
            existing(Dir::open_ambient_dir(path, ambient_authority()))?
        {
            let _ = current.dir_metadata()?;
        }
        return Ok(());
    };
    if !expected.matches(&parent, name)? {
        return sync_existing_dir(&parent);
    }
    let staging = format!(".vsdb-destroy-{:032x}", rand::random::<u128>());
    let mut options = DirBuilder::new();
    let options = options.recursive(false);
    #[cfg(unix)]
    cap_std::fs::DirBuilderExt::mode(options, 0o700);
    if let Err(err) = parent.create_dir_with(&staging, options) {
        if expected.metadata.is_dir() && keep_root(&err) {
            return Ok(());
        }
        return Err(err);
    }
    let result = (|| {
        let private = parent.open_dir_nofollow(&staging)?;
        match parent.rename(name, &private, "entry") {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) if expected.metadata.is_dir() && keep_root(&e) => {
                return Ok(());
            }
            Err(e) => return Err(e),
        }
        finish_captured(
            &private,
            &parent,
            name,
            expected,
            &parent_path.join(&staging),
        )
    })();
    // A nonempty staging directory retains the captured entry on error.
    let cleanup = parent.remove_dir(&staging);
    let fence = sync_dir(&parent);
    result.and(cleanup).and(fence)
}

fn keep_root(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::PermissionDenied
            | io::ErrorKind::ResourceBusy
            | io::ErrorKind::ReadOnlyFilesystem
            | io::ErrorKind::CrossesDevices
    )
}

fn finish_captured(
    private: &Dir,
    parent: &Dir,
    name: &OsStr,
    expected: &Identity,
    staging: &Path,
) -> io::Result<()> {
    let removed = expected
        .matches(private, OsStr::new("entry"))
        .and_then(|matches| {
            if !matches {
                return Ok(false);
            }
            if expected.metadata.is_dir() {
                private.remove_dir("entry")?;
            } else {
                private.remove_file_or_symlink("entry")?;
            }
            Ok(true)
        });
    if !matches!(removed, Ok(true)) {
        // Put back a replacement or an entry whose removal failed. If another
        // name is already present, keep both and report the recovery location.
        if let Err(err) = restore_entry(private, parent, name) {
            let source = sync_dir(private).err().unwrap_or(err);
            return Err(io::Error::new(
                source.kind(),
                RetainedEntry {
                    path: staging.join("entry"),
                    source,
                },
            ));
        }
    }
    removed.map(drop)
}

#[derive(Debug, thiserror::Error)]
#[error("namespace cleanup retained an entry at {path}: {source}")]
struct RetainedEntry {
    path: PathBuf,
    source: io::Error,
}

fn restore_entry(private: &Dir, parent: &Dir, name: &OsStr) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    {
        use rustix::fs::{RenameFlags, renameat_with};
        Ok(renameat_with(
            private,
            "entry",
            parent,
            name,
            RenameFlags::NOREPLACE,
        )?)
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_vendor = "apple"
    )))]
    {
        // link creation never overwrites an intervening replacement. If the
        // filesystem cannot link this object, keep it in the private directory.
        private.hard_link("entry", parent, name)?;
        private.remove_file_or_symlink("entry")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captured_replacement_is_restored_without_overwriting_a_new_name() {
        let root = std::env::temp_dir()
            .join(format!("vsdb-captured-{}", rand::random::<u128>()));
        let staging = root.join("private");
        fs::create_dir_all(&staging).unwrap();
        let parent = Dir::open_ambient_dir(&root, ambient_authority()).unwrap();
        let private = parent.open_dir("private").unwrap();
        let expected = Identity {
            metadata: private.dir_metadata().unwrap(),
            link: None,
        };
        let name = OsStr::new("public");

        private.write("entry", b"replacement").unwrap();
        finish_captured(&private, &parent, name, &expected, &staging).unwrap();
        assert_eq!(parent.read(name).unwrap(), b"replacement");
        private.write("entry", b"another entry").unwrap();
        let err =
            finish_captured(&private, &parent, name, &expected, &staging).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(parent.read(name).unwrap(), b"replacement");
        assert_eq!(private.read("entry").unwrap(), b"another entry");
        drop(private);
        drop(parent);
        fs::remove_dir_all(root).unwrap();
    }
}
