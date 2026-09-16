---
name: jvm-performance-troubleshooting
description: Use this skill for production-safe JVM performance profiling - thread contention, GC pressure, CPU hotspots via JFR, avoiding heap dumps unless necessary.
---

# JVM Performance Troubleshooting

## When To Use

- Java pod has high CPU, latency spikes, or stuck requests but is NOT crashing
- Need lightweight, production-safe profiling (not full heap dump)
- User wants performance evidence before taking disruptive action

## Workflow

1. Ensure Kubernetes evidence exists first (`getPodDetails`, `getEvents`, logs)
2. Run `analyzeJvm` for baseline thread/memory/GC diagnosis
3. Run `listJavaProcesses` and `captureThreadDump` when CPU contention or hangs are suspected
4. Run `getGCStats` and `captureMemoryHistogram` for memory pressure signals
5. For latency/CPU profiling, use `captureAndAnalyzeJfr` (preferred) or `captureJfrProfile`
6. Use `captureHeapDump` ONLY as last resort after approval
7. Summarize findings as Confirmed vs Hypothesis with exact tool failures under Evidence Gaps

## CRITICAL: Output Fidelity

- Your response MUST contain ONLY names and data returned by tool calls.
- Do NOT add namespaces, pods, services, or any resource names from your own knowledge.
- Report ALL items returned by the tool - no more, no less.
- **Thread dumps, heap histograms, and GC stats**: Include the RAW tool output verbatim.
  Do NOT paraphrase or rewrite. Copy exact thread names, class names, lock addresses,
  line numbers, and stack traces directly from the tool output.
  NEVER substitute real values with examples from your training data.

## Key Difference from jvm-diagnostics

- This skill prioritizes LIGHTWEIGHT profiling (thread dumps, GC stats, JFR)
- `jvm-diagnostics` includes sidecar deployment and full heap dump workflows
