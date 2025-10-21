use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct FrameEnvelope<T> {
    pub frame: T,
    pub airtime_us: u32,
    pub enqueue_time: Instant,
}

#[derive(Debug, Clone)]
pub struct PerNextHopQueue<N, T>
where
    N: Eq + Hash + Copy,
{
    pub nhop: N,
    pub deficit: i32,
    pub quantum: i32,
    pub queue: VecDeque<FrameEnvelope<T>>,
}

impl<N, T> PerNextHopQueue<N, T>
where
    N: Eq + Hash + Copy,
{
    fn new(nhop: N, quantum: i32) -> Self {
        Self {
            nhop,
            deficit: 0,
            quantum,
            queue: VecDeque::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

pub struct DrrScheduler<N, T>
where
    N: Eq + Hash + Copy,
{
    quantum_scale: i32,
    queues: Vec<PerNextHopQueue<N, T>>,
    lookup: HashMap<N, usize>,
    last_index: usize,
    guard: Duration,
}

impl<N, T> DrrScheduler<N, T>
where
    N: Eq + Hash + Copy,
{
    pub fn new(quantum_scale: i32, guard: Duration) -> Self {
        Self {
            quantum_scale: quantum_scale.max(1),
            queues: Vec::new(),
            lookup: HashMap::new(),
            last_index: 0,
            guard,
        }
    }

    fn ensure_queue(&mut self, nhop: N, airtime_us: u32) -> usize {
        if let Some(idx) = self.lookup.get(&nhop) {
            return *idx;
        }

        let quantum = compute_quantum(self.quantum_scale, airtime_us);
        let idx = self.queues.len();
        self.queues.push(PerNextHopQueue::new(nhop, quantum));
        self.lookup.insert(nhop, idx);
        idx
    }

    pub fn enqueue(&mut self, nhop: N, frame: T, airtime_us: u32, now: Instant) {
        let idx = self.ensure_queue(nhop, airtime_us);
        let q = &mut self.queues[idx];
        let quantum = compute_quantum(self.quantum_scale, airtime_us);
        if q.quantum != quantum {
            q.quantum = quantum;
        }
        q.queue.push_back(FrameEnvelope {
            frame,
            airtime_us,
            enqueue_time: now,
        });
    }

    pub fn pick_next(&mut self, now: Instant) -> Option<(N, FrameEnvelope<T>)> {
        if self.queues.is_empty() {
            return None;
        }

        let mut scanned = 0;
        let total = self.queues.len();

        while scanned < total {
            self.last_index %= self.queues.len();
            let idx = self.last_index;
            self.last_index = (self.last_index + 1) % self.queues.len();
            scanned += 1;

            let queue = &mut self.queues[idx];
            if queue.is_empty() {
                continue;
            }

            queue.deficit += queue.quantum;

            if let Some(front) = queue.queue.front() {
                let airtime = front.airtime_us as i32;
                if queue.deficit < airtime {
                    continue;
                }

                if now.duration_since(front.enqueue_time) < self.guard {
                    continue;
                }

                queue.deficit -= airtime;
                return queue.queue.pop_front().map(|f| (queue.nhop, f));
            }
        }

        None
    }
}

fn compute_quantum(scale: i32, airtime_us: u32) -> i32 {
    let airtime = airtime_us.max(1) as i32;
    (scale / airtime).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_rotates() {
        let mut sched: DrrScheduler<u8, u8> = DrrScheduler::new(50_000, Duration::from_millis(0));
        let now = Instant::now();
        sched.enqueue(1, 10, 400, now);
        sched.enqueue(2, 11, 100, now);

        let mut seen = Vec::new();
        for i in 1..=10 {
            if let Some((nhop, _)) = sched.pick_next(now + Duration::from_millis(i)) {
                seen.push(nhop);
            }
        }
        assert!(seen.contains(&1));
        assert!(seen.contains(&2));
    }
}
