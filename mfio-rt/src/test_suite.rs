pub use crate::{DirHandle, Fs, OpenOptions};
pub use core::future::Future;
pub use futures::StreamExt;
pub use mfio::backend::IoBackend;
pub use mfio::traits::{IoRead, IoWrite};
pub use once_cell::sync::Lazy;
pub use std::collections::BTreeSet;
pub use std::fs;
pub use std::path::Path;
pub use tempdir::TempDir;

const FILES: &[(&str, &str)] = &[
    ("Cargo.toml", include_str!("../Cargo.toml")),
    ("src/lib.rs", include_str!("lib.rs")),
    ("src/util.rs", include_str!("util.rs")),
    ("src/native/mod.rs", include_str!("native/mod.rs")),
    (
        "src/native/impls/mod.rs",
        include_str!("native/impls/mod.rs"),
    ),
    (
        "src/native/impls/thread.rs",
        include_str!("native/impls/thread.rs"),
    ),
    (
        "src/native/impls/unix_extra.rs",
        include_str!("native/impls/unix_extra.rs"),
    ),
    ("p1/p2/p3/a.txt", "TEST TEST TEST"),
];

const DIRECTORIES: &[&str] = &[
    "src/native/impls/io_uring",
    "src/native/impls/mio",
    "p1/p2/p3/p4/p5/p6",
];

static CTX: Lazy<TestCtx> = Lazy::new(TestCtx::new);

const fn hash(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9u64);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111ebu64);
    x ^ (x >> 31)
}

pub struct TestCtx {
    files: Vec<(String, Vec<u8>)>,
    dirs: Vec<String>,
}

impl Default for TestCtx {
    fn default() -> Self {
        Self::new()
    }
}

impl TestCtx {
    pub fn new() -> Self {
        let mut files = vec![];

        for (p, c) in FILES {
            files.push((p.to_string(), c.to_string().into_bytes()));
        }

        let mut dirs = vec![];

        for d in DIRECTORIES {
            dirs.push(d.to_string());
        }

        // Create a few "large" random files

        #[cfg(not(miri))]
        for f in 0..4 {
            files.push((
                format!("large/{f}"),
                (0..0x4000000)
                    .map(|v| hash((v as u64) << (f * 8)) as u8)
                    .collect::<Vec<_>>(),
            ))
        }

        Self { files, dirs }
    }

    pub fn list(&self, path: &str) -> BTreeSet<String> {
        let path = path.trim_start_matches('.').trim_start_matches('/');
        self.files
            .iter()
            .map(|v| v.0.as_str())
            .filter_map(move |v| v.strip_prefix(path))
            .map(|v| v.trim_start_matches('/'))
            .map(|v| v.split_once('/').map(|(a, _)| a).unwrap_or(v))
            .chain(
                self.dirs
                    .iter()
                    .map(|v| v.as_str())
                    .filter_map(move |v| v.strip_prefix(path))
                    .map(|v| v.trim_start_matches('/'))
                    .map(|v| v.split_once('/').map(|(a, _)| a).unwrap_or(v)),
            )
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    pub fn all_dirs(&self) -> BTreeSet<String> {
        let mut dirs = BTreeSet::new();

        for dir in &self.dirs {
            let mut d = ".".to_string();
            dirs.insert(d.clone());
            for p in dir.split('/') {
                d.push('/');
                d.push_str(p);
                dirs.insert(d.clone());
            }
        }

        for dir in self
            .files
            .iter()
            .filter_map(|(a, _)| a.rsplit_once('/').map(|(a, _)| a))
        {
            let mut d = ".".to_string();
            dirs.insert(d.clone());
            for p in dir.split('/') {
                d.push('/');
                d.push_str(p);
                dirs.insert(d.clone());
            }
        }

        dirs
    }

    pub fn build_in_path(&self, path: &Path) {
        for d in &self.dirs {
            let _ = fs::create_dir_all(path.join(d));
        }

        for (p, data) in &self.files {
            if let Some((d, _)) = p.rsplit_once('/') {
                let _ = fs::create_dir_all(path.join(d));
            }
            fs::write(path.join(p), data).unwrap();
        }
    }
}

pub struct TestRun<'a, T> {
    ctx: &'a TestCtx,
    rt: &'a T,
    dir: TempDir,
}

impl<'a, T: Fs> TestRun<'a, T> {
    pub fn new(rt: &'a T, dir: TempDir) -> Self {
        CTX.build_in_path(dir.path());

        Self { rt, dir, ctx: &CTX }
    }

    pub fn files_equal(&self) -> impl Iterator<Item = impl Future<Output = ()> + '_> + '_ {
        self.ctx.files.iter().map(move |(p, data)| async move {
            let path = &self.dir.path().join(p);
            let fh = self
                .rt
                .open(path, OpenOptions::new().read(true))
                .await
                .unwrap();
            let mut buf = vec![];
            fh.read_to_end(0, &mut buf).await.unwrap();
            assert!(&buf == data, "File {p} does not match!");
        })
    }

    pub fn files_equal_rel(&self) -> impl Iterator<Item = impl Future<Output = ()> + '_> + '_ {
        self.ctx.files.iter().map(move |(p, data)| async move {
            let path = &self.dir.path().join(p);
            let dh = self
                .rt
                .current_dir()
                .open_dir(path.parent().unwrap())
                .await
                .unwrap();
            let fh = dh
                .open_file(path.file_name().unwrap(), OpenOptions::new().read(true))
                .await
                .unwrap();
            let mut buf = vec![];
            fh.read_to_end(0, &mut buf).await.unwrap();
            assert!(&buf == data, "File {p} does not match!");
        })
    }

    pub fn writes_equal<'b>(
        &'b self,
        tdir: &'b TempDir,
    ) -> impl Iterator<Item = impl Future<Output = ()> + 'b> + 'b {
        self.ctx.files.iter().map(move |(p, data)| async move {
            let path = &tdir.path().join(p);
            let _ = fs::create_dir_all(path.parent().unwrap());

            let fh = self
                .rt
                .open(path, OpenOptions::new().create(true).write(true))
                .await
                .unwrap();

            fh.write_all(0, data).await.unwrap();
            let buf = fs::read(path).unwrap();
            assert!(&buf == data, "File {p} does not match!");
        })
    }

    pub fn writes_equal_rel<'b>(
        &'b self,
        tdir: &'b TempDir,
    ) -> impl Iterator<Item = impl Future<Output = ()> + 'b> + 'b {
        self.ctx.files.iter().map(move |(p, data)| async move {
            let path = &tdir.path().join(p);
            let _ = fs::create_dir_all(path.parent().unwrap());

            let dh = self
                .rt
                .current_dir()
                .open_dir(path.parent().unwrap())
                .await
                .unwrap();

            let fh = dh
                .open_file(
                    path.file_name().unwrap(),
                    OpenOptions::new().create(true).write(true),
                )
                .await
                .unwrap();

            fh.write_all(0, data).await.unwrap();
            let buf = fs::read(path).unwrap();
            assert!(&buf == data, "File {p} does not match!");
        })
    }

    pub fn dirs_equal(&self) -> impl Iterator<Item = impl Future<Output = ()> + '_> + '_ {
        let all_dirs = self.ctx.all_dirs();
        log::error!("{all_dirs:?}");
        all_dirs.into_iter().map(move |d| async move {
            log::error!("Join with {d}");
            let path = &self.dir.path().join(&d);
            log::error!("Open dir: {path:?}");
            let dh = self
                .rt
                .current_dir()
                .open_dir(path)
                .await
                .unwrap_or_else(|_| panic!("{path:?}"));
            log::error!("Opened dir: {path:?}");
            let dir = dh
                .read_dir()
                .await
                .unwrap()
                .map(|v| v.unwrap().name)
                .collect::<BTreeSet<String>>()
                .await;
            assert_eq!(dir, self.ctx.list(&d), "{d}");
        })
    }

    pub fn walk_dirs(&self) -> impl Iterator<Item = impl Future<Output = ()> + '_> + '_ {
        let curdir = self.rt.current_dir();
        self.ctx.all_dirs().into_iter().flat_map(move |d1| {
            self.ctx.all_dirs().into_iter().map(move |d2| {
                let d1 = d1.clone();
                async move {
                    let path1 = &self.dir.path().join(&d1);
                    let path2 = &self.dir.path().join(&d2);

                    let relpath1 = pathdiff::diff_paths(path1, path2).unwrap();
                    let relpath2 = pathdiff::diff_paths(&d1, &d2).unwrap();
                    assert_eq!(&relpath1, &relpath2);

                    let dh1 = curdir.open_dir(path1).await.unwrap();
                    let dh2 = curdir.open_dir(path2).await.unwrap();

                    let relpath3 =
                        pathdiff::diff_paths(dh1.path().await.unwrap(), dh2.path().await.unwrap())
                            .unwrap();

                    assert_eq!(&relpath1, &relpath3);
                }
            })
        })
    }
}

#[macro_export]
macro_rules! test_suite {
    ($test_ident:ident, $fs_builder:expr) => {
        #[cfg(test)]
        mod $test_ident {
            use $crate::test_suite::*;

            async fn seq(i: impl Iterator<Item = impl Future<Output = ()> + '_> + '_) {
                for i in i {
                    i.await;
                }
            }

            async fn con(i: impl Iterator<Item = impl Future<Output = ()> + '_> + '_) {
                let unordered = i.collect::<futures::stream::FuturesUnordered<_>>();
                unordered.count().await;
            }

            fn staticify<T>(val: &mut T) -> &'static mut T {
                unsafe { core::mem::transmute(val) }
            }

            #[cfg(not(miri))]
            #[test]
            fn all_tests_seq() {
                $fs_builder(|rt| async move {
                    let dir = TempDir::new("mfio-testsuite").unwrap();
                    let tdir = TempDir::new("mfio-testsuite-writes").unwrap();
                    let tdir2 = TempDir::new("mfio-testsuite-writes").unwrap();
                    let run = TestRun::new(rt, dir);
                    seq(run.files_equal()).await;
                    seq(run.files_equal_rel()).await;
                    seq(run.dirs_equal()).await;
                    seq(run.walk_dirs()).await;
                    seq(run.writes_equal(&tdir)).await;
                    seq(run.writes_equal_rel(&tdir2)).await;
                });
            }

            #[cfg(not(miri))]
            #[test]
            fn all_tests_con() {
                $fs_builder(|rt| async move {
                    let dir = TempDir::new("mfio-testsuite").unwrap();
                    let tdir = TempDir::new("mfio-testsuite-writes").unwrap();
                    let tdir2 = TempDir::new("mfio-testsuite-writes").unwrap();
                    let run = TestRun::new(rt, dir);
                    futures::join! {
                        con(run.files_equal()),
                        con(run.files_equal_rel()),
                        con(run.dirs_equal()),
                        con(run.walk_dirs()),
                        seq(run.writes_equal(&tdir)),
                        seq(run.writes_equal_rel(&tdir2)),
                    }
                });
            }

            #[test]
            fn files_equal() {
                $fs_builder(|rt| async move {
                    let dir = TempDir::new("mfio-testsuite").unwrap();
                    let run = TestRun::new(rt, dir);
                    seq(run.files_equal()).await;
                });
            }

            #[test]
            fn files_equal_rel() {
                $fs_builder(|rt| async move {
                    let dir = TempDir::new("mfio-testsuite").unwrap();
                    let run = TestRun::new(rt, dir);
                    seq(run.files_equal_rel()).await;
                });
            }

            #[test]
            fn dirs_equal() {
                $fs_builder(|rt| async move {
                    let dir = TempDir::new("mfio-testsuite").unwrap();
                    let run = TestRun::new(rt, dir);
                    seq(run.dirs_equal()).await;
                });
            }

            #[test]
            fn walk_dirs() {
                $fs_builder(|rt| async move {
                    let dir = TempDir::new("mfio-testsuite").unwrap();
                    let run = TestRun::new(rt, dir);
                    seq(run.walk_dirs()).await;
                });
            }

            #[test]
            fn writes_equal() {
                $fs_builder(|rt| async move {
                    let dir = TempDir::new("mfio-testsuite").unwrap();
                    let tdir = TempDir::new("mfio-testsuite-writes").unwrap();
                    let run = TestRun::new(rt, dir);
                    seq(run.writes_equal(&tdir)).await;
                });
            }

            #[test]
            fn writes_equal_rel() {
                $fs_builder(|rt| async move {
                    let dir = TempDir::new("mfio-testsuite").unwrap();
                    let tdir = TempDir::new("mfio-testsuite-writes").unwrap();
                    let run = TestRun::new(rt, dir);
                    seq(run.writes_equal_rel(&tdir)).await;
                });
            }
        }
    };
}

test_suite!(tests_default, |closure| {
    let mut rt = crate::NativeRt::default();
    let rt = staticify(&mut rt);
    rt.run(closure);
});

test_suite!(tests_all, |closure| {
    for (name, rt) in crate::NativeRt::builder().enable_all().build_each() {
        println!("{name}");
        if let Ok(mut rt) = rt {
            let rt = staticify(&mut rt);
            rt.run(closure);
        }
    }
});
