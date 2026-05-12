import type { ReactNode } from "react";
import { TopKViz } from "./viz/TopKViz";
import { DagViz } from "./viz/DagViz";
import { WatermarkViz } from "./viz/WatermarkViz";
import { CheckpointsViz } from "./viz/CheckpointsViz";
import { WindowsTimelineViz } from "./viz/WindowsViz";
import { JoinViz } from "./viz/JoinViz";
import { FraudViz } from "./viz/FraudViz";
import { LateEventsViz } from "./viz/LateEventsViz";
import { KeyedStateViz } from "./viz/KeyedStateViz";
import { EventTimeViz } from "./viz/EventTimeViz";
import { RecoveryViz } from "./viz/RecoveryViz";
import type {
  TopKMsg,
  DagMsg,
  WatermarkMsg,
  CheckpointsMsg,
  TumblingMsg,
  SlidingMsg,
  SessionMsg,
  JoinMsg,
  FraudMsg,
  GeneratorStats,
} from "./types";

export type ConceptStatus = "live" | "preview" | "soon";

export type WsData = {
  topk?: TopKMsg;
  dag?: DagMsg;
  watermarks?: WatermarkMsg;
  checkpoints?: CheckpointsMsg;
  tumbling?: TumblingMsg;
  sliding?: SlidingMsg;
  session?: SessionMsg;
  join?: JoinMsg;
  fraud?: FraudMsg;
  generator?: GeneratorStats;
};

export type Actions = {
  triggerFraud: () => void;
  setLatePct: (pct: number) => void;
};

export type Concept = {
  id: string;
  title: string;
  oneLiner: string;
  status: ConceptStatus;
  viz: (ctx: WsData, a: Actions) => ReactNode;
  explanation: ReactNode;
  sql?: string;
  java?: string;
  interviewQs: { q: string; a: string }[];
};

export type ConceptGroup = { id: string; title: string; concepts: Concept[] };

export const GROUPS: ConceptGroup[] = [
  {
    id: "basics",
    title: "Streaming basics",
    concepts: [
      {
        id: "dag",
        title: "The dataflow graph",
        oneLiner: "How Flink turns your code into running operators.",
        status: "live",
        viz: (c) => <DagViz data={c.dag} />,
        explanation: (
          <>
            <p>
              A Flink job is a <b>directed acyclic graph</b> of operators: sources read events,
              transforms process them, sinks push results out. Each box below is one operator;
              the arrows are data streams between them. Live numbers show records read and
              written so far.
            </p>
            <p className="mt-2">
              Each operator runs in parallel across <i>subtasks</i> — its
              <code> parallelism</code>. Records are routed between subtasks by key (
              <code>keyBy</code>) or rebalanced.
            </p>
          </>
        ),
        interviewQs: [
          {
            q: "Operator vs subtask?",
            a: "Operator = one logical step ('count by product'). Subtask = one physical instance running on a TaskManager. Parallelism = number of subtasks.",
          },
          {
            q: "Why does Flink chain operators?",
            a: "Chained operators share a thread, so no network or serialization between them. Flink chains when parallelism matches and the upstream uses a forward partitioner.",
          },
        ],
      },
    ],
  },

  {
    id: "time",
    title: "Time & Watermarks",
    concepts: [
      {
        id: "watermarks",
        title: "Watermarks",
        oneLiner: "Flink's notion of 'we're done with everything before time T'.",
        status: "live",
        viz: (c) => <WatermarkViz data={c.watermarks} />,
        explanation: (
          <>
            <p>
              A <b>watermark</b> is a record that flows through the stream saying "no event
              with timestamp ≤ T should arrive after me." It's how Flink decides when to close
              an event-time window.
            </p>
            <p className="mt-2">
              Each subtask emits its own watermark, based on the max timestamp seen minus the
              allowed out-of-orderness. The downstream operator's watermark is the
              <i> minimum</i> across its inputs — so one slow partition holds up the whole job.
            </p>
          </>
        ),
        java: `WatermarkStrategy.<PageView>forBoundedOutOfOrderness(
    Duration.ofSeconds(5))
  .withTimestampAssigner((event, recTs) -> event.ts_ms);`,
        interviewQs: [
          {
            q: "What happens to an event arriving after the watermark passed?",
            a: "It's 'late.' Default = dropped. Options: route to a side output, or set allowedLateness to keep the window state alive.",
          },
          {
            q: "Why is downstream watermark = min(upstream)?",
            a: "Correctness requires 'no event before T' to hold across ALL inputs. The slowest input wins.",
          },
          {
            q: "Trade-off in out-of-orderness?",
            a: "Larger = more late events caught but higher window-close latency. Smaller = lower latency, more drops.",
          },
        ],
      },
      {
        id: "event-vs-processing-time",
        title: "Event time vs processing time",
        oneLiner: "Two clocks. Pick the right one.",
        status: "live",
        viz: (c) => <EventTimeViz tumbling={c.tumbling as { recent?: any[] }} generator={c.generator} />,
        explanation: (
          <>
            <p>
              <b>Event time</b> is the timestamp on the record itself (when the click
              happened). <b>Processing time</b> is the wall clock when Flink saw it. Event time
              is reproducible and correct under replay; processing time is jittery and depends
              on arrival order.
            </p>
            <p className="mt-2">
              Above: green bars = event-time counts per 10s window (from the watermark-driven
              Flink job). Amber bars = processing-time rate sampled every second. Trigger a
              traffic spike from the controls — watch the bottom row jump immediately while
              the top row only changes if event-time densities actually changed.
            </p>
          </>
        ),
        interviewQs: [
          {
            q: "When is processing time OK to use?",
            a: "When events are roughly in order, low replay value, and you don't care about reprocessing correctness — simple alerting, internal metrics.",
          },
        ],
      },
      {
        id: "late-events",
        title: "Late events",
        oneLiner: "Drop, allow, or side-output.",
        status: "live",
        viz: (c, a) => (
          <LateEventsViz data={c.generator} onLatePct={a.setLatePct} />
        ),
        explanation: (
          <>
            <p>
              When an event's timestamp is older than the watermark, it's <i>late</i>. Three
              handling strategies: <b>drop</b> (default), <b>allowedLateness</b> (keep window
              state alive past close, re-fire on each late event), <b>side output</b> (route
              to a separate stream for downstream reconciliation).
            </p>
            <p className="mt-2">
              Pick a non-zero % above — the generator backdates that fraction of events. Each
              red dot below the green watermark line = one event Flink would drop today.
            </p>
          </>
        ),
        java: `OutputTag<PageView> lateTag = new OutputTag<>("late") {};
SingleOutputStreamOperator<TopKResult> main = source
  .keyBy(...)
  .window(TumblingEventTimeWindows.of(...))
  .allowedLateness(Time.seconds(30))
  .sideOutputLateData(lateTag)
  .aggregate(...);
DataStream<PageView> lateStream = main.getSideOutput(lateTag);`,
        interviewQs: [
          {
            q: "How does allowedLateness work under the hood?",
            a: "Window state isn't cleared at watermark pass — Flink keeps it for allowedLateness, re-firing each time a late event arrives.",
          },
        ],
      },
    ],
  },

  {
    id: "windows",
    title: "Windows",
    concepts: [
      {
        id: "tumbling",
        title: "Tumbling windows",
        oneLiner: "Fixed, non-overlapping buckets.",
        status: "live",
        viz: (c) => (
          <WindowsTimelineViz
            data={c.tumbling}
            kind="tumbling"
          />
        ),
        explanation: (
          <>
            <p>
              Each event lives in exactly one window. Easiest to reason about: no
              double-counting. The bars above are recent 10-second tumbling counts.
            </p>
          </>
        ),
        java: `.window(TumblingEventTimeWindows.of(Time.seconds(10)))`,
        interviewQs: [
          {
            q: "Why is tumbling easier than sliding?",
            a: "Each event in exactly one window → counts don't double-count. Sliding overlaps, so the same event contributes to multiple windows.",
          },
        ],
      },
      {
        id: "sliding",
        title: "Sliding windows",
        oneLiner: "Overlapping buckets — smoother counts.",
        status: "live",
        viz: (c) => (
          <WindowsTimelineViz
            data={c.sliding}
            kind="sliding"
          />
        ),
        explanation: (
          <>
            <p>
              A window of size <code>S</code> sliding every <code>L</code> means each event is
              in <code>S/L</code> windows. Used for smoother per-minute counts that update every
              few seconds. Below: 30s windows sliding every 10s — every event sits in 3 windows.
            </p>
          </>
        ),
        java: `.window(SlidingEventTimeWindows.of(
    Time.seconds(30), Time.seconds(10)))`,
        interviewQs: [
          {
            q: "Memory cost vs tumbling?",
            a: "Sliding stores each event in S/L windows. State grows by that multiplier compared to tumbling — at scale this matters.",
          },
        ],
      },
      {
        id: "session",
        title: "Session windows",
        oneLiner: "Buckets keyed on gaps of inactivity.",
        status: "live",
        viz: (c) => (
          <WindowsTimelineViz
            data={c.session}
            kind="session"
          />
        ),
        explanation: (
          <>
            <p>
              Variable-length windows that close after <code>gap</code> seconds of inactivity
              for that key. Used for user sessions, ride trips, support conversations. The
              tracks above are concurrent user sessions (30s inactivity gap).
            </p>
          </>
        ),
        java: `.keyBy(pv -> pv.user_id)
.window(EventTimeSessionWindows.withGap(Time.seconds(30)))`,
        interviewQs: [
          {
            q: "How does Flink handle two session windows that should merge?",
            a: "When an event lands between two open sessions, Flink merges their states via the SessionWindowMergeFunction. The merge happens at fire time.",
          },
        ],
      },
    ],
  },

  {
    id: "state",
    title: "State & Fault Tolerance",
    concepts: [
      {
        id: "checkpoints",
        title: "Checkpoints",
        oneLiner: "Asynchronous snapshots that make exactly-once possible.",
        status: "live",
        viz: (c) => <CheckpointsViz data={c.checkpoints} />,
        explanation: (
          <>
            <p>
              A <b>checkpoint</b> is a consistent snapshot of every operator's state, taken
              without stopping the stream. The JobManager injects a <i>barrier</i> at the
              sources; operators snapshot their state when the barrier arrives, then forward
              it. Once all operators ack, the checkpoint is durable.
            </p>
            <p className="mt-2">
              Below: bars are recent checkpoints; height = duration. Fresh barriers flash
              violet on completion. Failure → restart from the last completed checkpoint →
              exactly-once semantics (with proper sources/sinks).
            </p>
          </>
        ),
        interviewQs: [
          {
            q: "How does Flink keep the stream running during a checkpoint?",
            a: "Chandy-Lamport: barriers flow with the data, operators snapshot async on barrier arrival, no blocking forward progress.",
          },
          {
            q: "Checkpoint vs savepoint?",
            a: "Both are state snapshots. Checkpoint = automatic, for recovery; expires when job goes down. Savepoint = manual, durable, used for upgrades.",
          },
          {
            q: "What breaks exactly-once?",
            a: "A non-replayable source, or non-transactional sink. Need rewindable source (Kafka) + idempotent/transactional sink.",
          },
        ],
      },
      {
        id: "keyed-state",
        title: "Keyed state",
        oneLiner: "Per-key in-memory store, automatically partitioned.",
        status: "live",
        viz: (c) => <KeyedStateViz data={c.topk} />,
        explanation: (
          <>
            <p>
              After <code>keyBy(k)</code>, each operator gets state scoped to <code>k</code>.
              Types: <code>ValueState</code>, <code>ListState</code>, <code>MapState</code>,
              <code> ReducingState</code>, <code>AggregatingState</code>.
            </p>
            <p className="mt-2">
              The heatmap above approximates per-key state distribution by recent event count.
              In a real cluster, skew like this is exactly the kind of pressure point you'd
              fix with key salting, two-stage aggregation, or a state-aware key partitioner.
            </p>
          </>
        ),
        java: `private transient ValueState<Long> counter;

@Override
public void open(Configuration parameters) {
  counter = getRuntimeContext().getState(
    new ValueStateDescriptor<>("counter", Long.class));
}`,
        interviewQs: [
          {
            q: "Where does keyed state live?",
            a: "On the subtask that owns the key (determined by hash partitioning). State stays local — Flink moves work, not data.",
          },
          {
            q: "What state backend should I pick?",
            a: "HashMapStateBackend = fast, fits in RAM. EmbeddedRocksDBStateBackend = bigger than RAM, slightly slower, incremental checkpoints. Default to RocksDB at scale.",
          },
        ],
      },
      {
        id: "savepoints",
        title: "Savepoints & recovery",
        oneLiner: "How you upgrade a Flink job without losing state.",
        status: "live",
        viz: (c) => <RecoveryViz checkpoints={c.checkpoints} />,
        explanation: (
          <>
            <p>
              Trigger a savepoint → stop the job → redeploy the new version → restart from the
              savepoint. State is migrated. Same machinery is what powers automatic recovery
              from a checkpoint after a TaskManager death.
            </p>
          </>
        ),
        interviewQs: [
          {
            q: "Schema evolution for state?",
            a: "Flink supports backward-compatible schema evolution for Avro/POJO state. Add/remove fields, but renames or type changes require a state migration job.",
          },
          {
            q: "What lives in a savepoint?",
            a: "All operator state + Kafka offsets + watermarks. Enough to resume the exact stream position with consistent state.",
          },
        ],
      },
    ],
  },

  {
    id: "patterns",
    title: "Streaming patterns",
    concepts: [
      {
        id: "topk",
        title: "Top-K trending",
        oneLiner: "Heavy hitters per window.",
        status: "live",
        viz: (c) => <TopKViz data={c.topk} />,
        explanation: (
          <>
            <p>
              For each 10-second window we count events per product, then pick the top K
              across all products. Two-stage: per-key aggregate to count, then a global
              <code> windowAll</code> to sort and slice.
            </p>
            <p className="mt-2">
              At scale you'd swap the sort for a fixed-size heap, or use Count-Min Sketch +
              heap for approximate heavy hitters. Today's demo uses an exact sort for clarity.
            </p>
          </>
        ),
        sql: `SELECT product_id, count, row_num FROM (
  SELECT product_id, COUNT(*) AS count,
         ROW_NUMBER() OVER (
           PARTITION BY TUMBLE_END(rowtime, INTERVAL '10' SECOND)
           ORDER BY COUNT(*) DESC
         ) AS row_num
  FROM page_views
  GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), product_id
) WHERE row_num <= 10;`,
        java: `source.keyBy(pv -> pv.product_id)
  .window(TumblingEventTimeWindows.of(Time.seconds(10)))
  .aggregate(new CountAgg(), new TagWindow())
  .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
  .process(new TopKWindow(k));`,
        interviewQs: [
          {
            q: "Why windowAll for the final step?",
            a: "windowAll forces parallelism=1 so we can globally sort within that window. The per-key step before it does heavy aggregation in parallel.",
          },
          {
            q: "Scale to billions of keys?",
            a: "(1) Per-partition top-K heaps, (2) downstream global merge of heaps. Or Count-Min Sketch for approximate counts + heap.",
          },
          {
            q: "What if one product dominates (key skew)?",
            a: "Salt the key: keyBy(product_id + random(0..N)) → partial counts → keyBy(product_id) to combine. Trades latency for balance.",
          },
        ],
      },
      {
        id: "stream-join",
        title: "Stream-stream join",
        oneLiner: "impression × click within a time window.",
        status: "live",
        viz: (c) => <JoinViz data={c.join} />,
        explanation: (
          <>
            <p>
              Two streams keyed on the same key, joined within a time interval. Below: page
              views joined to purchases by <code>user_id</code> within
              <code> [-30s, +5min]</code>. Each row is one (view, purchase) match by the same
              user.
            </p>
            <p className="mt-2">
              State growth is bounded by the join window: each side keeps records only long
              enough for the match window to expire. Tune the interval to balance memory and
              recall.
            </p>
          </>
        ),
        java: `views.keyBy(pv -> pv.user_id)
  .intervalJoin(purchases.keyBy(p -> p.user_id))
  .between(Time.seconds(-30), Time.minutes(5))
  .process(new ProcessJoinFunction<...>() {
    public void processElement(View v, Purchase p, ...) { ... }
  });`,
        interviewQs: [
          {
            q: "Interval join vs window join?",
            a: "Interval join is per-element with a relative time bound — most natural for impression→click. Window join joins two streams within the same fixed window — better for batch-aligned analytics.",
          },
          {
            q: "What happens to state on key skew?",
            a: "Hot keys keep state for the join interval. Skew → uneven memory. Mitigation: shorter interval, partition pre-aggregation, or salt the key.",
          },
        ],
      },
      {
        id: "sessionization-pattern",
        title: "Sessionization",
        oneLiner: "User activity grouped on the fly.",
        status: "live",
        viz: (c) => (
          <WindowsTimelineViz data={c.session} kind="session" />
        ),
        explanation: (
          <>
            <p>
              The Session window operator gives you sessions for free. Each track above is one
              user; rect length = session duration; number = events in that session.
            </p>
          </>
        ),
        interviewQs: [
          {
            q: "Why do session windows need a merge step?",
            a: "An out-of-order event may bridge two open sessions. Flink merges their states with the configured MergingWindowAssigner before firing.",
          },
        ],
      },
      {
        id: "cep-fraud",
        title: "Fraud detection (CEP)",
        oneLiner: "Pattern matching on streams.",
        status: "live",
        viz: (c, a) => <FraudViz data={c.fraud} onTrigger={a.triggerFraud} />,
        explanation: (
          <>
            <p>
              The CEP library lets you describe sequences of events as a pattern and emit when
              matched. This job watches for <code>≥3 purchases</code> from the same user
              within 10 seconds — a textbook account-takeover signature.
            </p>
            <p className="mt-2">
              Click <i>Inject fraud burst</i> — the generator emits a rapid-fire purchase
              sequence and the alert appears in this list within seconds.
            </p>
          </>
        ),
        java: `Pattern<Purchase, ?> p = Pattern.<Purchase>begin("first")
  .followedBy("more")
  .timesOrMore(2)
  .within(Time.seconds(10));

CEP.pattern(purchases.keyBy(p -> p.user_id), p)
   .process(new PatternProcessFunction<>() {
     public void processMatch(Map<String, List<Purchase>> m, ...) { ... }
   });`,
        interviewQs: [
          {
            q: "CEP vs stateful ProcessFunction?",
            a: "CEP is declarative — describe the pattern, get matches. ProcessFunction is imperative — you manage state and timers. CEP is great for sequence patterns; ProcessFunction wins for arbitrary logic.",
          },
        ],
      },
    ],
  },
];

export const FIRST_CONCEPT_ID =
  (typeof window !== "undefined" && window.location.hash.replace(/^#/, "")) ||
  "dag";
