# VSDB Finding Verification

You are verifying whether a reported code issue is a true bug or a false positive.

## Setup

1. Read `.claude/docs/false-positive-guide.md` — your primary reference.
2. Read `.claude/docs/technical-patterns.md` — for pattern matching.
3. Load relevant subsystem patterns from `.claude/docs/patterns/`.

## Input

The user provides a finding to verify. This may be:
- Output from `/vs-review` that needs validation
- A concern raised during code review
- A static analysis warning
- A hypothesis about a potential bug

## Execution Protocol

### Step 1: Understand the Finding

1. Parse the finding: what exactly is claimed to be wrong?
2. Identify the subsystem and code location
3. Read the actual code at the reported location with full context (100+ lines)

### Step 2: False Positive Check

Run through EVERY rule in `false-positive-guide.md`:

- [ ] **FP-1**: Is this safe Rust code? If so, memory safety issues are prevented by the compiler.
- [ ] **FP-2**: Is this a `shadow()` call with valid SWMR documentation?
- [ ] **FP-3**: Does prefix isolation make this cross-structure concern impossible?
- [ ] **FP-4**: Is the unwrap() on a known-valid state?
- [ ] **FP-5**: Would clippy catch this?
- [ ] **FP-6**: Is this a "consider" suggestion without a concrete failure scenario?
- [ ] **FP-7**: Is this test-only code held to production standards?
- [ ] **FP-8**: Is there a valid SAFETY comment on this unsafe block?
- [ ] **FP-9**: Is this a performance issue on a cold path?
- [ ] **FP-10**: Is COW allocation being flagged as unnecessary?
- [ ] **FP-11**: Is the ref-count analysis based on a single function rather than the full lifecycle?
- [ ] **FP-12**: Is this a Merkle root change after a legitimate mutation?

### Step 3: Reproduction Attempt

If the finding passes the false positive check:

1. **Construct a trigger scenario**: What exact sequence of operations would trigger this bug?
2. **Trace the code path**: Walk through the code with your trigger scenario
3. **Check existing tests**: Does any test already cover this scenario?
4. **Evaluate likelihood**: Is the trigger scenario realistic in production?

### Step 4: Verdict

Classify the finding as one of:

| Verdict | Meaning |
|---------|---------|
| **CONFIRMED** | The bug is real, has a concrete trigger, and should be fixed |
| **LIKELY** | The bug appears real but the trigger is difficult to construct or verify |
| **UNCERTAIN** | Cannot confirm or deny — needs more investigation or a test |
| **FALSE POSITIVE** | The finding is incorrect — cite the specific FP rule that applies |
| **WON'T FIX** | The bug is real but the risk is negligible or the fix has worse tradeoffs |

## Output Format

```
## Verification: <one-line finding summary>

**Verdict**: CONFIRMED / LIKELY / UNCERTAIN / FALSE POSITIVE / WON'T FIX

### False Positive Checklist
- FP-1: [PASS/FAIL] <brief reason>
- FP-2: [PASS/FAIL] <brief reason>
  ... (only list relevant FP checks)

### Analysis

<Detailed reasoning for the verdict>

### Trigger Scenario

<If CONFIRMED/LIKELY: exact steps to trigger>
<If FALSE POSITIVE: why no trigger exists>

### Recommendation

<What to do next — fix, test, investigate further, or close>
```
