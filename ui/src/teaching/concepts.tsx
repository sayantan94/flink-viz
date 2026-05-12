import type { ReactNode } from "react";
import { TopKViz } from "./viz/TopKViz";
import { DagViz } from "./viz/DagViz";
import { WatermarkViz } from "./viz/WatermarkViz";
import { CheckpointsViz } from "./viz/CheckpointsViz";
import { PlaceholderViz } from "./viz/PlaceholderViz";
import type {
  TopKMsg,
  DagMsg,
  WatermarkMsg,
  CheckpointsMsg,
} from "./types";

export type ConceptStatus = "live" | "preview" | "soon";

export type Concept = {
  id: string;
  title: string;
  oneLiner: string;
  status: ConceptStatus;
  viz: (ctx: WsData) => ReactNode;
  explanation: ReactNode;
  sql?: string;
  java?: string;
  interviewQs: { q: string; a: string }[];
};

export type WsData = {
  topk?: TopKMsg;
  dag?: DagMsg;
  watermarks?: WatermarkMsg;
  checkpoints?: CheckpointsMsg;
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
              A Flink job is a <b>directed acyclic graph</b> of operators:
              sources read events, transforms (map / filter / window / etc.)
              process them, sinks push results out. Each box below is one
              operator. The arrows are the streams between them. The numbers
              are records read and written so far.
            </p>
            <p className="mt-2">
              Each operator runs in parallel across <i>subtasks</i> — the
              number you see as <code>parallelism</code>. Records are routed
              between subtasks by key (<code>keyBy</code>) or rebalanced.
            </p>
          </>
        ),
        interviewQs: [
          {
            q: "What's the difference between an operator and a subtask?",
            a: "An operator is one logical step (e.g. 'count by product'). A subtask is one physical instance of that operator running on a TaskManager. Parallelism = number of subtasks.",
          },
          {
            q: "Why does Flink chain operators together?",
            a: "Chained operators run in the same thread → no network or serialization between them. Flink chains when both sides have the same parallelism and the upstream is a forward partitioner.",
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
              A <b>watermark</b> is a record that flows through the stream
              saying "no event with timestamp ≤ T should arrive after me." It's
              how Flink decides when to close an event-time window.
            </p>
            <p className="mt-2">
              Each subtask emits its own watermark (above), based on the
              max timestamp seen minus the allowed out-of-orderness. The
              downstream operator's watermark is the <i>minimum</i> across all
              its inputs — so one slow partition holds up everyone.
            </p>
            <p className="mt-2">
              Lag colors: <span className="text-emerald-400">green &lt; 5s</span>{" "}
              · <span className="text-amber-400">amber &lt; 30s</span> ·{" "}
              <span className="text-rose-400">red ≥ 30s</span>.
            </p>
          </>
        ),
        java: `WatermarkStrategy.<PageView>forBoundedOutOfOrderness(
    Duration.ofSeconds(5))
  .withTimestampAssigner((event, recTs) -> event.ts_ms);`,
        interviewQs: [
          {
            q: "What happens to an event that arrives after the watermark passed?",
            a: "It's 'late.' By default, Flink drops it. You can route late events to a side output, or allow lateness to keep the window open longer.",
          },
          {
            q: "Why is downstream watermark = min(upstream watermarks)?",
            a: "Because correctness requires that 'no event before T' holds across ALL inputs. The slowest input wins.",
          },
          {
            q: "Trade-off in choosing out-of-orderness?",
            a: "Larger value = more late events caught but higher latency to window close. Smaller = lower latency but more drops.",
          },
        ],
      },
      {
        id: "event-vs-processing-time",
        title: "Event time vs processing time",
        oneLiner: "Two clocks. Pick the right one.",
        status: "preview",
        viz: () => <PlaceholderViz label="Event-time vs wall-clock viz" />,
        explanation: (
          <>
            <p>
              <b>Event time</b> is the timestamp on the record itself (when the
              click happened). <b>Processing time</b> is the wall clock when
              Flink saw it. Almost always you want event time — it's
              reproducible and correct under replay.
            </p>
          </>
        ),
        interviewQs: [
          {
            q: "When is processing time OK to use?",
            a: "When events are roughly in order and you don't care about reprocessing correctness (e.g. simple alerting).",
          },
        ],
      },
      {
        id: "late-events",
        title: "Late events",
        oneLiner: "Drop, allow, or side-output.",
        status: "soon",
        viz: () => <PlaceholderViz label="Late events rain — coming soon" />,
        explanation: (
          <>
            <p>
              When an event's timestamp is older than the watermark, it's late.
              Three handling strategies: <b>drop</b> (default),{" "}
              <b>allowedLateness</b> (keep the window state alive past close),
              <b>side output</b> (route to a separate stream for late
              reconciliation).
            </p>
          </>
        ),
        interviewQs: [
          {
            q: "How does allowedLateness work under the hood?",
            a: "The window state isn't cleared on watermark pass — Flink keeps it for allowedLateness, re-firing the window each time a late event arrives.",
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
        status: "preview",
        viz: () => <PlaceholderViz label="Tumbling window timeline" />,
        explanation: (
          <>
            <p>
              Each event lives in exactly one window. Used by our current Top-K
              job (10-second buckets).
            </p>
          </>
        ),
        java: `.window(TumblingEventTimeWindows.of(Time.seconds(10)))`,
        interviewQs: [
          {
            q: "Why are tumbling windows easier to reason about than sliding?",
            a: "Each event is in exactly one window, so counts don't double-count. Sliding windows overlap — same event contributes to multiple windows.",
          },
        ],
      },
      {
        id: "sliding",
        title: "Sliding windows",
        oneLiner: "Overlapping buckets — smoother counts.",
        status: "soon",
        viz: () => <PlaceholderViz label="Sliding window viz" />,
        explanation: <p>Each event lives in window_size / slide windows.</p>,
        interviewQs: [],
      },
      {
        id: "session",
        title: "Session windows",
        oneLiner: "Buckets keyed on gaps of inactivity.",
        status: "soon",
        viz: () => <PlaceholderViz label="Session window viz" />,
        explanation: (
          <p>
            User activity grouped until a gap (e.g. 30s of no events) closes
            the session. Variable-length windows.
          </p>
        ),
        interviewQs: [],
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
              A <b>checkpoint</b> is a consistent snapshot of every operator's
              state, taken without stopping the stream. Flink injects a{" "}
              <i>barrier</i> at the sources; operators snapshot their state
              when the barrier arrives, then forward it. Once all operators
              ack, the checkpoint is durable.
            </p>
            <p className="mt-2">
              The bars below are recent checkpoints; height = duration. Click
              hover for size. Failure → restart from the last completed
              checkpoint → exactly-once semantics (with proper sources/sinks).
            </p>
          </>
        ),
        interviewQs: [
          {
            q: "How does Flink keep the stream running during a checkpoint?",
            a: "It uses the Chandy-Lamport algorithm: barriers flow with the data, operators snapshot async when they see a barrier, never blocking forward progress.",
          },
          {
            q: "Checkpoint vs savepoint?",
            a: "Both are state snapshots. Checkpoint = automatic, for recovery; expires when the job goes down. Savepoint = manual, durable, used for upgrades and migrations.",
          },
          {
            q: "What breaks exactly-once?",
            a: "A non-replayable source, or a non-transactional sink. Need a source you can rewind to a checkpoint offset (Kafka), and a sink that's idempotent or transactional.",
          },
        ],
      },
      {
        id: "keyed-state",
        title: "Keyed state",
        oneLiner: "Per-key in-memory store, automatically partitioned.",
        status: "soon",
        viz: () => <PlaceholderViz label="State heatmap by key" />,
        explanation: (
          <p>
            After <code>keyBy(k)</code>, each operator gets a state scoped to{" "}
            <code>k</code>. ValueState, ListState, MapState, ReducingState,
            AggregatingState.
          </p>
        ),
        interviewQs: [],
      },
      {
        id: "savepoints",
        title: "Savepoints & recovery",
        oneLiner: "How you upgrade a Flink job without losing state.",
        status: "soon",
        viz: () => <PlaceholderViz label="Kill-TM-then-restore demo" />,
        explanation: (
          <p>
            Trigger a savepoint, stop the job, redeploy the new version,
            restart from the savepoint. State migrated.
          </p>
        ),
        interviewQs: [],
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
              For each 10-second window we count events per product, then pick
              the top K across all products. Two-stage approach: a per-key
              aggregate to count, then a global <code>windowAll</code> to sort
              and slice.
            </p>
            <p className="mt-2">
              At scale you'd swap the sort for a fixed-size heap or use
              Count-Min Sketch + heap for approximate heavy hitters. Today's
              demo uses an exact sort for clarity.
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
            q: "Why use windowAll for the final top-K step?",
            a: "windowAll forces parallelism=1 so we can sort *all* products globally for that window. The per-key step before it does the heavy aggregation in parallel.",
          },
          {
            q: "How would you make this scale to billions of keys?",
            a: "Replace the global sort with: (1) keyed top-K heap on each partition, (2) a downstream global merge of partition-level heaps. Or use Count-Min Sketch for approximate counts and a heap.",
          },
          {
            q: "What if the data is skewed (one product dominates)?",
            a: "Salt the key: keyBy(product_id + random(0..N)), do partial counts, then keyBy(product_id) to combine. Trades latency for balance.",
          },
        ],
      },
      {
        id: "stream-join",
        title: "Stream-stream join",
        oneLiner: "impression × click within a time window.",
        status: "soon",
        viz: () => <PlaceholderViz label="Interval join viz" />,
        explanation: <p>Two streams keyed on the same key, joined within an interval.</p>,
        interviewQs: [],
      },
      {
        id: "sessionization",
        title: "Sessionization",
        oneLiner: "Group activity into user sessions on the fly.",
        status: "soon",
        viz: () => <PlaceholderViz label="Session viz" />,
        explanation: <p>Session windows over user activity; emit on session close.</p>,
        interviewQs: [],
      },
      {
        id: "cep-fraud",
        title: "Fraud detection (CEP)",
        oneLiner: "Pattern matching on streams.",
        status: "soon",
        viz: () => <PlaceholderViz label="CEP pattern viz" />,
        explanation: <p>Define a sequence of events with constraints; emit when matched.</p>,
        interviewQs: [],
      },
    ],
  },
];

export const FIRST_CONCEPT_ID =
  (typeof window !== "undefined" && window.location.hash.replace(/^#/, "")) ||
  "dag";
