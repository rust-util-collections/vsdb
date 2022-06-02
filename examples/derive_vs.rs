use vsdb::Vs;

#[derive(Vs, Debug, Default)]
struct A {
    a: i32,
    b: u64,
}

fn main() {
    dbg!(A::default());
}
