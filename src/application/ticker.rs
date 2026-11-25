use crate::ports::Clock;

/// Converts wall-clock progress from a `Clock` port into Raft ticks.
pub struct Ticker<C: Clock> {
    clock: C,
    tick_ms: u64,
    last: u64,
}

impl<C: Clock> Ticker<C> {
    pub fn new(clock: C, tick_ms: u64) -> Self {
        let last = clock.now_millis();
        Self {
            clock,
            tick_ms: tick_ms.max(1),
            last,
        }
    }

    /// Number of whole ticks elapsed since the previous call.
    pub fn due(&mut self) -> u64 {
        let now = self.clock.now_millis();
        let n = now.saturating_sub(self.last) / self.tick_ms;
        self.last += n * self.tick_ms;
        n
    }

    pub fn clock(&self) -> &C {
        &self.clock
    }
}
