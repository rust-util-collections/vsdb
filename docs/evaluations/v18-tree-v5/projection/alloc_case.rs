// Linked after the caller-thread counting allocator from v18-adapter/alloc.rs.
use std::{env, hint::black_box, path::PathBuf};
use vsdb::{PersistentBTree, VsdbOptions, vsdb_configure};

fn main() {
    let dir = PathBuf::from(env::args_os().nth(1).unwrap());
    vsdb_configure(VsdbOptions::new(dir).with_mem_budget_mb(2048)).unwrap();
    let mut tree = PersistentBTree::new();
    let root = tree.bulk_load((0u64..32768).map(|i| {
        let mut value = [i as u8; 32];
        value[..8].copy_from_slice(&i.to_be_bytes());
        ((i * 2).to_be_bytes().to_vec(), value.to_vec())
    }));
    tree.namespace().flush();
    assert_eq!(tree.iter(root).count(), 32768);
    for (name, hit, contains) in [
        ("get-hit", true, false),
        ("get-miss", false, false),
        ("contains-hit", true, true),
        ("contains-miss", false, true),
    ] {
        // Visit disjoint leaf slots and all internal cache partitions.
        let run = || {
            let mut checksum = 0;
            for i in 0u64..8192 {
                let index = (i * 65537 + i * 877) % 32768;
                let key = (2 * index + u64::from(!hit)).to_be_bytes();
                if contains {
                    let present = tree.contains_key(root, &key);
                    assert_eq!(present, hit);
                    checksum += usize::from(present);
                } else {
                    let value = tree.get(root, &key);
                    if hit {
                        let value = value.unwrap();
                        assert_eq!(value.len(), 32);
                        assert_eq!(&value[..8], index.to_be_bytes());
                        checksum += value[8] as usize;
                    } else {
                        assert!(value.is_none());
                    }
                }
            }
            checksum
        };
        black_box(run());
        capture(name, 8192, run);
    }
}
