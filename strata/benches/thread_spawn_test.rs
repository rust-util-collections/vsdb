use std::time::Instant;

fn main() {
    let thread_counts = [2, 4, 8, 16];
    for &num_threads in &thread_counts {
        let start = Instant::now();
        let iters = 1000;
        for _ in 0..iters {
            let mut handles = vec![];
            for _ in 0..num_threads {
                handles.push(std::thread::spawn(|| {}));
            }
            for h in handles {
                h.join().unwrap();
            }
        }
        let elapsed = start.elapsed();
        println!("{} threads: {:?} per batch", num_threads, elapsed / iters);
    }
}
