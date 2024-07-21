#[cfg(not(feature = "vs"))]
fn main() {}

#[cfg(feature = "vs")]
fn main() {
    real::run();
}

#[cfg(feature = "vs")]
mod real {
    use vsdb::Vs;

    #[derive(Vs, Debug, Default)]
    struct VsDerive {
        a: i32,
        b: u64,
    }

    pub fn run() {
        dbg!(VsDerive::default());
    }
}
