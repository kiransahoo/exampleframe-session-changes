---
name: jvm-diagnostics
description: Use this skill for JVM deep diagnostics in Kubernetes workloads, including sidecar deployment, thread dumps, heap dumps, GC and process-level analysis.
---

# JVM Diagnostics

## When To Use

- Application is slow, stuck, or timing out with no clear Kubernetes root cause
- Suspected deadlock, high CPU threads, memory pressure, or GC thrashing
- Need thread/heap level evidence for RCA

## Workflow

1. Confirm target pod and namespace
2. Deploy diagnostics sidecar when needed
3. Start with `analyzeJvm` for broad evidence
4. Run low-risk diagnostics next (process list, thread dump, JVM stats, class histogram)
5. Prefer JFR (`captureAndAnalyzeJfr` or `captureJfrProfile`) for production performance profiling before heap dump
6. Use heap dump only when justified and approved
7. Present findings with:
   - **Raw diagnostic output** (thread dumps, histograms, GC stats) - include VERBATIM, never summarize
   - Kubernetes Evidence
   - JVM Evidence
   - Findings (Confirmed/Hypothesis)
   - Evidence Gaps (exact tool errors)
   - Recommendations

## CRITICAL: Output Fidelity

- Your response MUST contain ONLY names and data returned by tool calls.
- Do NOT add namespaces, pods, services, or any resource names from your own knowledge.
- Report ALL items returned by the tool - no more, no less.
- **Thread dumps, heap histograms, and GC stats**: Include the RAW tool output verbatim in your response.
  Do NOT paraphrase, summarize, or rewrite the data. Copy exact thread names, class names,
  lock addresses, line numbers, and stack traces directly from the tool output.
  NEVER substitute real values with examples like "com.example", "0xdeadbeef", or "ClassName.java:10".
  The user needs the exact data to debug their application.

## Safety Rules

- Prefer non-disruptive diagnostics before heap dump
- Treat heap dump as expensive and potentially disruptive
- Keep operations scoped to explicit pod/namespace
- Report uncertainty when JVM process cannot be reliably identified
