import { useEffect, useState } from "react";
import type { Lesson, Quiz } from "./lessons";

export function LessonRunner({
  lesson,
  conceptTitle,
  onExit,
}: {
  lesson: Lesson;
  conceptTitle: string;
  onExit: () => void;
}) {
  const [idx, setIdx] = useState(0);
  const [ready, setReady] = useState(false);
  const [quizPick, setQuizPick] = useState<number | null>(null);
  const step = lesson.steps[idx];
  const isLast = idx === lesson.steps.length - 1;

  useEffect(() => {
    setReady(false);
    setQuizPick(null);
    let cancelled = false;
    (async () => {
      if (step.setup) {
        try {
          await step.setup();
        } catch {
          /* ignore — control endpoints are best-effort */
        }
      }
      if (cancelled) return;
      if (step.pauseMs) {
        setTimeout(() => !cancelled && setReady(true), step.pauseMs);
      } else {
        setReady(true);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [idx, step]);

  const next = () => {
    if (isLast) onExit();
    else setIdx((i) => i + 1);
  };

  return (
    <div className="border-t-2 border-emerald-700 bg-gradient-to-b from-emerald-950/50 to-zinc-950">
      <div className="px-6 py-4 max-w-4xl mx-auto">
        <div className="flex justify-between items-center mb-3">
          <div className="text-[10px] uppercase tracking-widest text-emerald-400">
            Lesson · {conceptTitle} · step {idx + 1}/{lesson.steps.length}
          </div>
          <button
            onClick={onExit}
            className="text-[11px] text-zinc-500 hover:text-zinc-300"
          >
            exit ✕
          </button>
        </div>

        {step.quiz ? (
          <QuizCard
            quiz={step.quiz}
            picked={quizPick}
            onPick={setQuizPick}
            onNext={next}
            isLast={isLast}
          />
        ) : (
          <>
            <h3 className="text-base text-zinc-100 font-semibold mb-2">
              {step.title}
            </h3>
            <div className="text-sm text-zinc-300 leading-relaxed max-w-3xl">
              {step.body}
            </div>
            {step.action && (
              <button
                onClick={step.action.run}
                className="mt-3 text-xs px-3 py-1.5 bg-emerald-900/40 hover:bg-emerald-900/70 border border-emerald-700 rounded text-emerald-200"
              >
                {step.action.label}
              </button>
            )}
            <div className="mt-4 flex items-center gap-3">
              <button
                disabled={!ready}
                onClick={next}
                className={
                  "text-xs px-4 py-2 rounded font-semibold " +
                  (ready
                    ? "bg-emerald-600 hover:bg-emerald-500 text-zinc-950"
                    : "bg-zinc-800 text-zinc-500 cursor-wait")
                }
              >
                {isLast ? "Finish lesson ✓" : "Next →"}
              </button>
              {!ready && step.pauseMs ? (
                <span className="text-[11px] text-zinc-500">
                  watching the cluster react…
                </span>
              ) : null}
              {idx > 0 && (
                <button
                  onClick={() => setIdx((i) => Math.max(0, i - 1))}
                  className="text-[11px] text-zinc-500 hover:text-zinc-300"
                >
                  ← back
                </button>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function QuizCard({
  quiz,
  picked,
  onPick,
  onNext,
  isLast,
}: {
  quiz: Quiz;
  picked: number | null;
  onPick: (i: number) => void;
  onNext: () => void;
  isLast: boolean;
}) {
  const isCorrect = picked === quiz.correct;
  return (
    <div>
      <div className="text-[10px] uppercase tracking-widest text-emerald-400 mb-2">
        Interview-style check
      </div>
      <h3 className="text-base text-zinc-100 font-semibold mb-3">{quiz.q}</h3>
      <div className="space-y-1.5 mb-3">
        {quiz.choices.map((c, i) => {
          const answered = picked !== null;
          const isPick = picked === i;
          const isAns = quiz.correct === i;
          let cls = "border-zinc-800 bg-zinc-900/40 text-zinc-300";
          if (answered && isAns)
            cls = "border-emerald-700 bg-emerald-900/40 text-emerald-200";
          else if (answered && isPick && !isAns)
            cls = "border-rose-700 bg-rose-900/40 text-rose-200";
          return (
            <button
              key={i}
              disabled={answered}
              onClick={() => onPick(i)}
              className={
                "w-full text-left px-3 py-2 rounded border text-sm font-mono " +
                cls
              }
            >
              {String.fromCharCode(65 + i)}. {c}
            </button>
          );
        })}
      </div>
      {picked !== null && (
        <div
          className={
            "text-sm mt-3 p-3 rounded border " +
            (isCorrect
              ? "border-emerald-700 bg-emerald-950/40 text-emerald-200"
              : "border-rose-700 bg-rose-950/40 text-rose-200")
          }
        >
          <div className="font-semibold mb-1">
            {isCorrect ? "✓ Right." : "✗ Not quite."}
          </div>
          <div className="text-zinc-300">{quiz.explain}</div>
        </div>
      )}
      <div className="mt-4">
        <button
          disabled={picked === null}
          onClick={onNext}
          className={
            "text-xs px-4 py-2 rounded font-semibold " +
            (picked !== null
              ? "bg-emerald-600 hover:bg-emerald-500 text-zinc-950"
              : "bg-zinc-800 text-zinc-500 cursor-not-allowed")
          }
        >
          {isLast ? "Finish ✓" : "Next →"}
        </button>
      </div>
    </div>
  );
}
