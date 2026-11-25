use std::{
    sync::{Arc, Barrier},
    time::Instant,
};

fn main() {
    let thread_counts = [2, 4, 8, 16];
    for &num_threads in &thread_counts {
        // Time spawning and joining the workers.
        let start = Instant::now();
        let mut handles = vec![];
        for _ in 0..num_threads {
            handles.push(std::thread::spawn(|| {}));
        }
        for h in handles {
            h.join().unwrap();
        }
        let time_with_spawn = start.elapsed();

        // Spawn the workers before starting the timer.
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let mut handles = vec![];
        for _ in 0..num_threads {
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait(); // Wait for all workers and the main thread to be ready.
                // Worker tasks would run here.
                b.wait(); // Signal that this worker has finished.
            }));
        }
        barrier.wait(); // Release the workers together.
        let start_pure = Instant::now();
        barrier.wait(); // Wait for every worker to finish.
        let time_pure = start_pure.elapsed();

        for h in handles {
            h.join().unwrap();
        }

        println!(
            "{} threads: \n  With spawn: {:?}\n  Pure exec:  {:?}\n",
            num_threads, time_with_spawn, time_pure
        );
    }
}
