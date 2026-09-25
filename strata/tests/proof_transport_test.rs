use std::{env, fs, process::Command};
use vsdb::{MptCalc, MptProof, SmtCalc, SmtProof};

#[test]
fn proof_transport_across_processes() {
    const DIRECTORY: &str = "VSDB_PROOF_TRANSPORT_DIR";
    if let Some(dir) = env::var_os(DIRECTORY) {
        let dir = std::path::PathBuf::from(dir);
        for kind in ["mpt", "smt"] {
            let root: [u8; 32] = fs::read(dir.join(format!("{kind}.root")))
                .unwrap()
                .try_into()
                .unwrap();
            for (key, exists) in [(b"alice".as_slice(), true), (b"carol", false)] {
                let bytes =
                    fs::read(dir.join(format!("{kind}-{exists}.proof"))).unwrap();
                if kind == "mpt" {
                    let proof = MptProof::from_bytes(&bytes).unwrap();
                    assert_eq!(proof.value().is_some(), exists);
                    assert!(MptCalc::verify_proof(&root, key, &proof).unwrap());
                    assert!(
                        !MptCalc::verify_proof(&root, b"wrong-key", &proof).unwrap()
                    );
                } else {
                    let proof = SmtProof::from_bytes(&bytes).unwrap();
                    assert_eq!(proof.value().is_some(), exists);
                    assert!(SmtCalc::verify_proof(&root, key, &proof).unwrap());
                    assert!(
                        !SmtCalc::verify_proof(&root, b"wrong-key", &proof).unwrap()
                    );
                }
            }
        }
        return;
    }

    let dir = env::temp_dir().join(format!("vsdb-proof-{}", rand::random::<u128>()));
    fs::create_dir(&dir).unwrap();
    let mut mpt = MptCalc::new();
    let mut smt = SmtCalc::new();
    for key in [b"alice".as_slice(), b"bob"] {
        mpt.insert(key, b"value\0bytes").unwrap();
        smt.insert(key, b"value\0bytes").unwrap();
    }
    fs::write(dir.join("mpt.root"), mpt.root_hash().unwrap()).unwrap();
    fs::write(dir.join("smt.root"), smt.root_hash().unwrap()).unwrap();
    for (key, exists) in [(b"alice".as_slice(), true), (b"carol", false)] {
        let mpt = mpt.prove(key).unwrap();
        let smt = smt.prove(key).unwrap();
        assert_eq!(
            mpt.to_bytes().unwrap(),
            postcard::to_allocvec(&mpt).unwrap()
        );
        assert_eq!(
            smt.to_bytes().unwrap(),
            postcard::to_allocvec(&smt).unwrap()
        );
        assert_eq!(
            postcard::from_bytes::<MptProof>(&mpt.to_bytes().unwrap()).unwrap(),
            mpt
        );
        assert_eq!(
            postcard::from_bytes::<SmtProof>(&smt.to_bytes().unwrap()).unwrap(),
            smt
        );
        fs::write(
            dir.join(format!("mpt-{exists}.proof")),
            mpt.to_bytes().unwrap(),
        )
        .unwrap();
        fs::write(
            dir.join(format!("smt-{exists}.proof")),
            smt.to_bytes().unwrap(),
        )
        .unwrap();
    }
    let output = Command::new(env::current_exe().unwrap())
        .args(["--exact", "proof_transport_across_processes", "--nocapture"])
        .env(DIRECTORY, &dir)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn proof_transport_fixtures_and_frame_errors() {
    let mpt = MptCalc::new().prove(b"").unwrap();
    let mpt_bytes = [b"VSMPTP01".as_slice(), &[0, 0, 1, 1, 0]].concat();
    assert_eq!(mpt.to_bytes().unwrap(), mpt_bytes);
    assert_eq!(MptProof::from_bytes(&mpt_bytes).unwrap(), mpt);

    let smt = SmtProof {
        key_hash: [0; 32],
        leaf: None,
        siblings: Vec::new(),
    };
    let smt_bytes = [b"VSSMTP01".as_slice(), &[0; 34]].concat();
    assert_eq!(smt.to_bytes().unwrap(), smt_bytes);
    assert_eq!(SmtProof::from_bytes(&smt_bytes).unwrap(), smt);
    // Transport decoding alone does not establish the expected key/root.
    assert!(!SmtCalc::verify_proof(&[0; 32], b"key", &smt).unwrap());
    assert!(MptProof::from_bytes(&smt_bytes).is_err());
    assert!(SmtProof::from_bytes(&mpt_bytes).is_err());

    for bytes in [mpt_bytes, smt_bytes] {
        for end in 0..bytes.len() {
            assert!(MptProof::from_bytes(&bytes[..end]).is_err());
            assert!(SmtProof::from_bytes(&bytes[..end]).is_err());
        }
        let mut future = bytes.clone();
        future[7] = b'2';
        assert!(MptProof::from_bytes(&future).is_err());
        assert!(SmtProof::from_bytes(&future).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(MptProof::from_bytes(&trailing).is_err());
        assert!(SmtProof::from_bytes(&trailing).is_err());
    }
}
