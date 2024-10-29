pub mod raw;
pub mod rawkey;

pub type DagMapId = [u8];

pub fn gen_dag_map_id_num() -> u128 {
    use crate::{Orphan, ValueEnDe};
    use once_cell::sync::Lazy;
    use parking_lot::Mutex;
    use ruc::*;
    use std::{fs, io::ErrorKind};

    static ID_NUM: Lazy<Mutex<Orphan<u128>>> = Lazy::new(|| {
        let mut meta_path = vsdb_core::vsdb_get_custom_dir().to_owned();
        meta_path.push("id_num");

        match fs::read(&meta_path) {
            Ok(m) => Mutex::new(ValueEnDe::decode(&m).unwrap()),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    let i = Orphan::new(0);
                    fs::write(&meta_path, i.encode()).unwrap();
                    Mutex::new(i)
                }
                _ => {
                    pnk!(Err(eg!("The fucking world is over!")))
                }
            },
        }
    });

    let mut hdr = ID_NUM.lock();
    let mut hdr = hdr.get_mut();
    *hdr += 1;
    *hdr
}
