import type { ReactNode } from "react";

const BACKEND = `http://${window.location.hostname}:3000`;

async function post(action: string, body: Record<string, unknown>) {
  await fetch(`${BACKEND}/control/${action}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

export const ctl = {
  setRate: (n: number) => post("rate", { events_per_sec: n }),
  setPurchaseRate: (n: number) => post("purchase-rate", { events_per_sec: n }),
  setLatePct: (pct: number, max_delay_ms = 30000) =>
    post("late-events", { percentage: pct, max_delay_ms }),
  spike: (duration_s = 5, multiplier = 5) =>
    post("spike", { duration_s, multiplier }),
  fraudBurst: (rate = 30, duration_s = 3) =>
    post("fraud-burst", { rate, duration_s }),
};

export type Quiz = {
  q: string;
  choices: string[];
  correct: number;
  explain: string;
};

export type LessonStep = {
  title: string;
  body: ReactNode;
  /** Auto-runs when the user advances to this step (fire-and-forget). */
  setup?: () => Promise<void> | void;
  /** Manual action button label + handler shown inside the step. */
  action?: { label: string; run: () => Promise<void> | void };
  /** Wait N ms before showing the "Next" button (lets the cluster react). */
  pauseMs?: number;
  quiz?: Quiz;
};

export type Lesson = { steps: LessonStep[] };

export const LESSONS: Record<string, Lesson> = {
  // ------------------------------------------------------------------ dag
  dag: {
    steps: [
      {
        title: "What you're looking at",
        body: (
          <>
            Each box on the right is a real Flink <b>operator</b>. The arrows
            are streams between them. Numbers update in real time — records
            <i> read</i> and <i>written</i>.
          </>
        ),
      },
      {
        title: "Operators run in parallel",
        body: (
          <>
            See the <code>parallelism</code> number? That's how many{" "}
            <i>subtasks</i> of the operator are actually running. With keyBy,
            Flink hashes the key and sends each record to one subtask — so
            state stays local.
          </>
        ),
      },
      {
        title: "Let's push more traffic and watch numbers spike",
        body: <>I'll bump page-view rate to 1000/sec for a few seconds.</>,
        setup: async () => {
          await ctl.spike(8, 4);
        },
        pauseMs: 4000,
      },
      {
        quiz: {
          q: "Why does keyBy + windowed aggregate make Flink scale?",
          choices: [
            "It eliminates shuffling between subtasks",
            "Each subtask only processes its own keys, so state stays local and parallel",
            "It uses RocksDB, which is faster than RAM",
            "It enables exactly-once semantics",
          ],
          correct: 1,
          explain:
            "After keyBy, each subtask owns a partition of keys. State for those keys lives on that subtask only — you scale by adding more subtasks, no cross-talk required.",
        },
        title: "Quick check",
        body: null,
      },
    ],
  },

  // ------------------------------------------------------------------ watermarks
  watermarks: {
    steps: [
      {
        title: "What's a watermark?",
        body: (
          <>
            A watermark is a special record saying <i>"no event with timestamp
            ≤ T will arrive after me."</i> Each colored bar above represents
            one subtask's current watermark. Green = healthy lag, amber = slow,
            red = stuck.
          </>
        ),
      },
      {
        title: "Watch them advance",
        body: (
          <>
            With 300 events/sec arriving roughly in order, watermarks tick
            forward every second or so — always 5s behind the latest seen
            timestamp (that's our <code>forBoundedOutOfOrderness(5s)</code>).
          </>
        ),
        setup: async () => {
          await ctl.setLatePct(0);
          await ctl.setRate(300);
        },
      },
      {
        title: "Now let's break it",
        body: (
          <>
            I'll backdate 50% of events by up to 30s. The bars should stop
            sliding cleanly — late events <i>don't</i> push the watermark
            forward, but they create a stream of "would-be-dropped" events.
          </>
        ),
        setup: async () => {
          await ctl.setLatePct(50);
        },
        pauseMs: 5000,
      },
      {
        title: "Different subtasks, different watermarks",
        body: (
          <>
            Notice subtasks of the <i>same operator</i> can hold different
            watermarks. The <b>downstream</b> watermark is the minimum — so a
            single slow partition holds up the whole job. That's the
            "straggler" pain point Flink ops people complain about.
          </>
        ),
      },
      {
        title: "Cleaning up",
        body: <>Disabling late events.</>,
        setup: async () => {
          await ctl.setLatePct(0);
        },
        quiz: {
          q: "An upstream operator has 4 subtasks with watermarks 100, 102, 105, 90. What watermark does the downstream operator see?",
          choices: ["105 (max)", "99.25 (mean)", "90 (min)", "Whichever arrives first"],
          correct: 2,
          explain:
            "Downstream watermark = min over all inputs. Correctness needs 'no events before T' to hold across ALL inputs, so the slowest input wins.",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ late events
  "late-events": {
    steps: [
      {
        title: "Late = event time < current watermark",
        body: (
          <>
            Right now the late-event rain is empty because all events are
            in-time. I'll inject 20% late events with up to 30s backdating.
          </>
        ),
        setup: async () => {
          await ctl.setLatePct(20);
        },
        pauseMs: 4000,
      },
      {
        title: "Drops — what Flink does by default",
        body: (
          <>
            Each red dot you see falling is one event whose timestamp is older
            than the watermark. Default policy: <b>silently drop</b>. Your
            counters under-count, but throughput stays high.
          </>
        ),
      },
      {
        title: "Three escape hatches",
        body: (
          <ul className="list-disc list-inside space-y-1">
            <li>
              <b>allowedLateness(T)</b> — keep window state alive for T past
              close; re-fire on every late event in that span.
            </li>
            <li>
              <b>sideOutputLateData(tag)</b> — route to a separate stream for
              reconciliation (cheap, no extra state).
            </li>
            <li>
              <b>Larger watermark out-of-orderness</b> — pay extra
              window-close latency to lose nothing.
            </li>
          </ul>
        ),
      },
      {
        title: "Crank it up",
        body: <>50% late, big delay. The rain gets dense.</>,
        setup: async () => {
          await ctl.setLatePct(50, 60000);
        },
        pauseMs: 4000,
        quiz: {
          q: "Your interviewer asks 'why not just set out-of-orderness to 10 minutes to catch everything?'",
          choices: [
            "Because watermarks only advance every 10 minutes",
            "Because windows take 10 more minutes to close, hurting end-to-end latency",
            "Because Kafka can't replay 10 minutes",
            "Because checkpoints would fail",
          ],
          correct: 1,
          explain:
            "Larger out-of-orderness = window close is delayed by that much. You catch more late events but downstream consumers wait longer. It's a latency-vs-completeness dial.",
        },
      },
      {
        title: "Reset",
        body: <>Turning off late events.</>,
        setup: async () => {
          await ctl.setLatePct(0);
        },
      },
    ],
  },

  // ------------------------------------------------------------------ tumbling
  tumbling: {
    steps: [
      {
        title: "10-second buckets",
        body: (
          <>
            Each rectangle is one closed 10-second tumbling window. Color
            intensity = event count. With Zipfian-skewed page views at 300/s,
            each bucket holds ~3000 events.
          </>
        ),
        setup: async () => {
          await ctl.setRate(300);
        },
      },
      {
        title: "Spike incoming",
        body: <>I'll multiply traffic by 5× for 8 seconds. Watch one bucket get noticeably darker.</>,
        setup: async () => {
          await ctl.spike(8, 5);
        },
        pauseMs: 10000,
      },
      {
        title: "Why tumbling is the easiest window",
        body: (
          <>
            Each event lives in <b>exactly one</b> window. No double-counting,
            no overlap state, simplest possible mental model. Use it whenever
            you don't need smoother updates.
          </>
        ),
        quiz: {
          q: "If your window is 10s and your sliding step is 10s, what kind of window is it?",
          choices: [
            "Sliding",
            "Tumbling (sliding = step makes it tumbling)",
            "Session",
            "Global",
          ],
          correct: 1,
          explain:
            "Tumbling is a degenerate sliding window where the slide equals the size. Flink has both APIs but they produce the same plan in that case.",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ sliding
  sliding: {
    steps: [
      {
        title: "Why overlap?",
        body: (
          <>
            A 30-second window sliding every 10 seconds means each event lives
            in <b>three</b> windows. Counts update every 10s but each count is
            a smooth 30-second average. Good for dashboards.
          </>
        ),
      },
      {
        title: "Memory price",
        body: (
          <>
            Each event sits in <code>size / slide</code> windows of state. Our
            30/10 setup = 3× the per-event state cost of a tumbling window
            with the same size. At scale, this matters.
          </>
        ),
        quiz: {
          q: "What's the trade-off vs tumbling?",
          choices: [
            "Lower memory, higher latency",
            "Higher memory, smoother per-second results",
            "Higher accuracy, lower throughput",
            "Same memory, just more outputs",
          ],
          correct: 1,
          explain:
            "Sliding stores each event in multiple windows simultaneously. State grows by the size/slide ratio. The upside is much smoother per-window results.",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ session
  session: {
    steps: [
      {
        title: "Sessions are user-shaped",
        body: (
          <>
            Each track above is one user's session. Length = duration of
            activity, number inside = events. The window closes after 30s of
            <b> no activity</b> for that user.
          </>
        ),
      },
      {
        title: "Variable shape, automatic merge",
        body: (
          <>
            Unlike fixed windows, two sessions can <b>merge</b> if a late
            event lands between them. Flink handles this via a
            MergingWindowAssigner — your state goes from two values to one
            combined value at fire time.
          </>
        ),
        quiz: {
          q: "When would you NOT use a session window?",
          choices: [
            "Per-user activity grouping",
            "Cart-abandonment detection",
            "Hourly revenue reporting",
            "Conversational support sessions",
          ],
          correct: 2,
          explain:
            "Hourly reports want fixed time boundaries — tumbling. Sessions are for variable-length, gap-based grouping.",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ checkpoints
  checkpoints: {
    steps: [
      {
        title: "Watch the bars stack up",
        body: (
          <>
            Every 10 seconds Flink injects a <b>barrier</b> at the source.
            Each operator snapshots its state when the barrier passes, then
            forwards it. When all operators ack, the checkpoint is durable.
            New checkpoint = violet flash.
          </>
        ),
      },
      {
        title: "Why they're cheap",
        body: (
          <>
            Snapshots are <b>asynchronous</b>: the operator copies state in
            the background while normal processing continues. That's why bar
            heights here are tens of milliseconds even when state grows.
          </>
        ),
      },
      {
        title: "The exactly-once trick",
        body: (
          <>
            A checkpoint includes Kafka source offsets. On crash, Flink
            rewinds Kafka to those offsets, restores operator state, replays
            forward. With a transactional Kafka sink, your downstream sees
            each record exactly once — guaranteed.
          </>
        ),
        quiz: {
          q: "What invalidates exactly-once?",
          choices: [
            "Non-replayable source (e.g. a flaky HTTP poll)",
            "Non-idempotent / non-transactional sink",
            "Both of the above",
            "Async I/O calls",
          ],
          correct: 2,
          explain:
            "Both. To replay safely you need to rewind input (source) AND tolerate the replay downstream (sink). Async I/O is fine — it has its own AsyncFunction story.",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ keyed state
  "keyed-state": {
    steps: [
      {
        title: "Keyed = partitioned",
        body: (
          <>
            After <code>keyBy(product_id)</code>, the state for each product
            lives on exactly one subtask. The heatmap shows skew — darker
            cells = more events for that key recently.
          </>
        ),
      },
      {
        title: "Why this is great",
        body: (
          <>
            You never have to think about locking. The operator code reads
            and writes state for "the current key" — Flink routes records to
            the subtask that owns that key. Add more parallelism → more
            subtasks → state spreads automatically.
          </>
        ),
      },
      {
        title: "Why this is dangerous",
        body: (
          <>
            Zipfian skew (which our generator simulates) makes one subtask
            hold most of the state. That subtask becomes a bottleneck.
            Mitigations: salt the key, two-stage aggregation, or use
            ReducingState to keep per-key state bounded.
          </>
        ),
        quiz: {
          q: "Hot key dominating one subtask. Fastest mitigation?",
          choices: [
            "Increase parallelism (won't help — same key still hashes to one subtask)",
            "Salt the key with random suffix, partial aggregate, then re-key",
            "Switch state backend",
            "Disable checkpointing",
          ],
          correct: 1,
          explain:
            "Hot-key skew survives any parallelism bump because keyBy is deterministic. Salting splits the hot key across N subtasks for the first stage, then re-keys for the final merge.",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ savepoints
  savepoints: {
    steps: [
      {
        title: "Savepoint vs checkpoint",
        body: (
          <>
            Same machinery, different policy. Checkpoints expire when the job
            dies. Savepoints are durable, manually triggered, used for
            upgrades, A/B job migrations, or moving state between Flink
            versions.
          </>
        ),
      },
      {
        title: "The recovery loop",
        body: (
          <>
            Kill a TaskManager: <code>kill -9 &lt;pid&gt;</code>. The
            JobManager detects the loss, rescues the work to another TM,
            replays from the last completed checkpoint. Kafka rewinds. Your
            counters resume exactly where they were.
          </>
        ),
      },
      {
        title: "Upgrading without losing state",
        body: (
          <>
            Trigger a savepoint → stop the job →{" "}
            <code>flink run -s &lt;savepoint-path&gt; new.jar</code>. New
            code, old state. With Avro schemas, Flink even migrates state
            shape between versions.
          </>
        ),
        quiz: {
          q: "You're rolling out a new version of a stateful job. What's the safe sequence?",
          choices: [
            "Cancel job → submit new",
            "Hot-swap the jar",
            "Trigger savepoint → cancel job → run new jar from savepoint",
            "Pause traffic, run, resume",
          ],
          correct: 2,
          explain:
            "Savepoint captures state + offsets. New job resumes from the snapshot — no lost state, no double counting.",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ topk
  topk: {
    steps: [
      {
        title: "Top-K, two stages",
        body: (
          <>
            Per-key window first: each product has its own count, computed
            in parallel. Then a single <code>windowAll</code> at parallelism
            1 sorts and picks the top K. Two stages because doing the sort
            keyed by product would be... useless.
          </>
        ),
      },
      {
        title: "Watch the leaderboard react to a spike",
        body: (
          <>
            I'll cause a 5× traffic spike. Some products that weren't in the
            top-10 may jump up; you'll see ▲/▼ badges on rank changes.
          </>
        ),
        setup: async () => {
          await ctl.spike(8, 5);
        },
        pauseMs: 12000,
      },
      {
        title: "Approximate top-K at real scale",
        body: (
          <>
            With billions of keys, an exact sort per window is too much. The
            production answer: <b>Count-Min Sketch</b> for approximate counts
            + a fixed-size heap. Memory bounded, accuracy tunable.
          </>
        ),
        quiz: {
          q: "Why does Top-K use windowAll for the final step?",
          choices: [
            "Performance optimization",
            "windowAll is keyed by product, so it parallelizes",
            "windowAll has parallelism 1, so all per-product counts converge into one window for the final sort",
            "It's a Flink limitation",
          ],
          correct: 2,
          explain:
            "windowAll = non-keyed window = parallelism 1. All upstream per-key window outputs land in one place where we can sort globally.",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ stream-join
  "stream-join": {
    steps: [
      {
        title: "What an interval join means",
        body: (
          <>
            For every page view, find any purchase from the same user within
            the next 5 minutes (or up to 30s before — sometimes purchases
            arrive slightly out of order). Both streams keyed by{" "}
            <code>user_id</code>.
          </>
        ),
        setup: async () => {
          await ctl.setPurchaseRate(30);
        },
      },
      {
        title: "Same product vs different",
        body: (
          <>
            Green rows: user bought the same product they viewed. Amber rows:
            they viewed one product, bought a different one (or arrived via
            organic search). The mix tells you about your funnel quality.
          </>
        ),
      },
      {
        title: "Memory characteristics",
        body: (
          <>
            Each side keeps records only as long as a match is possible (5
            min for purchases, 30s for views). State grows with key count ×
            join window, not unbounded.
          </>
        ),
        quiz: {
          q: "Interval join vs window join?",
          choices: [
            "Same thing, different syntax",
            "Window join aligns to fixed time boundaries; interval join is per-element with a relative bound",
            "Interval join only works on keyed streams; window join on non-keyed",
            "Window join is faster",
          ],
          correct: 1,
          explain:
            "Window join joins two streams within the same fixed window (analytics). Interval join is per-element with relative bounds (impression → click).",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ cep-fraud
  "cep-fraud": {
    steps: [
      {
        title: "Patterns, not predicates",
        body: (
          <>
            CEP lets you describe a <i>sequence</i> of events. Ours: "any
            purchase, followed by 2+ more purchases by the same user within
            10 seconds." Keyed by user_id so we check each user in parallel.
          </>
        ),
      },
      {
        title: "Trigger one now",
        body: <>I'll inject a fraud burst — 30 purchases/sec from one synthetic user.</>,
        setup: async () => {
          await ctl.fraudBurst(30, 3);
        },
        pauseMs: 5000,
      },
      {
        title: "What happens under the hood",
        body: (
          <>
            CEP compiles your pattern to an <b>NFA</b> (non-deterministic
            finite automaton). Each user has a per-key NFA state. When the
            NFA reaches an accepting state, the match fires. Time bounds
            <code> within()</code> are enforced by event-time timers, not
            wall-clock.
          </>
        ),
        quiz: {
          q: "When would you NOT use CEP?",
          choices: [
            "Detecting a specific sequence of order events",
            "Pattern A followed by B then C within 1 minute",
            "Counting events per second",
            "Login → password-change → withdrawal anti-pattern",
          ],
          correct: 2,
          explain:
            "Simple per-event aggregations (counts, sums) are way cheaper as keyed windows. CEP shines when you need sequence + timing semantics.",
        },
      },
    ],
  },

  // ------------------------------------------------------------------ sessionization
  "sessionization-pattern": {
    steps: [
      {
        title: "What you're seeing",
        body: (
          <>
            Each track is one user. The bar length is how long they were
            active before their 30-second inactivity gap closed the session.
          </>
        ),
      },
      {
        title: "Bursty users vs steady",
        body: (
          <>
            Users with many clicks in a short span produce dense, dark green
            sessions. A single click → tiny session. This is exactly the
            shape of "active session" you'd report to product analytics.
          </>
        ),
      },
    ],
  },

  // ------------------------------------------------------------------ event-vs-processing-time
  "event-vs-processing-time": {
    steps: [
      {
        title: "Two clocks",
        body: (
          <>
            Top row: event-time per-10s counts from a real Flink tumbling
            window. Bottom row: instantaneous processing-time rate sampled
            every second.
          </>
        ),
      },
      {
        title: "Let's trigger a spike",
        body: (
          <>
            Watch the bottom row jump immediately while the top row only
            reflects the actual <i>density of timestamps</i> within the
            window.
          </>
        ),
        setup: async () => {
          await ctl.spike(8, 5);
        },
        pauseMs: 10000,
      },
      {
        title: "Reproducibility under replay",
        body: (
          <>
            If you replayed the same Kafka offsets from a checkpoint,
            event-time results are <b>identical</b>. Processing-time results
            would shift with whatever the wall clock is doing during replay.
          </>
        ),
        quiz: {
          q: "Why is event time worth the watermark complexity?",
          choices: [
            "It uses less memory",
            "It gives reproducible, replay-safe results",
            "It's faster",
            "It's the only way to use windows",
          ],
          correct: 1,
          explain:
            "Reproducibility is the headline reason. Watermarks are the cost; correctness under replay is the benefit.",
        },
      },
    ],
  },
};
