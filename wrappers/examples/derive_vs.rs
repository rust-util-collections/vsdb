use vsdb::Vs;

#[derive(Vs, Debug, Default)]
struct VsDerive {
    a: i32,
    b: u64,
}

fn main() {
    dbg!(VsDerive::default());
}
