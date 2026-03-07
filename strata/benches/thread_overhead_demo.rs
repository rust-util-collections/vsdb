use std::sync::{Arc, Barrier};
use std::time::Instant;

fn main() {
    let thread_counts = [2, 4, 8, 16];
    for &num_threads in &thread_counts {
        // 方法1: 包含 spawn 的耗时 (模拟当前 benchmark)
        let start = Instant::now();
        let mut handles = vec![];
        for _ in 0..num_threads {
            handles.push(std::thread::spawn(|| {}));
        }
        for h in handles {
            h.join().unwrap();
        }
        let time_with_spawn = start.elapsed();

        // 方法2: 排除 spawn 的耗时
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let mut handles = vec![];
        for _ in 0..num_threads {
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait(); // 等待主线程发令
                // 实际工作内容...
                b.wait(); // 等待结束
            }));
        }
        barrier.wait(); // 开始计时!
        let start_pure = Instant::now();
        barrier.wait(); // 结束计时!
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
