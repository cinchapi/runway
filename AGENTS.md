# AGENTS.md

Engineering guidelines for AI agents and human contributors across
Cinchapi Java projects. This file is shared between repositories: it
states the rules that hold everywhere, and the Per-Project Facts
section says how to resolve the details that vary by repo.

## Critical Rules

These are the highest-priority, most-often-violated rules. The rest of
this file elaborates on them.

1. **Tests first.** Write failing tests before production code, for
   features and bug fixes alike. A bug-fix test must demonstrably fail
   against the unfixed code.
2. **Match the project's Java version.** Projects differ (some target
   Java 8, others Java 21). Determine the target before you write code
   (see Per-Project Facts) and never use a language feature or API
   newer than it.
3. **Run `./gradlew spotlessApply` after all code changes.**
   Unformatted code fails CI. Review its diff and confirm changes are
   purely cosmetic.
4. **80-character lines, and use the full width.** Wrapping at column
   55-65 when text fits to 75-80 is as wrong as exceeding 80. Only the
   final line of a paragraph/statement may be short.
5. **`if(` has no space before its paren.** Every other control
   keyword has one: `for (`, `while (`, `switch (`, `catch (`. This is
   asymmetric and intentional.
6. **Explicit branching for distinct paths.** No implicit else: when
   logic has two logically distinct paths, write `if`/`else` for
   both, even when a branch returns or throws. When the `if` has no
   logical alternative, write it alone: a conditional side effect
   takes no `else`, and an `else` whose body is only a comment is
   noise. No guard clauses and no early returns: validate
   preconditions with `Preconditions`/`Verify` calls, and invert the
   condition instead of a bare `return;` that skips the method body.
7. **Javadoc everything** (even private members), except `@Override`
   methods. Imperative mood ("Return...", never "Returns..."). Block
   format always. Class Javadoc requires `@author` = the human user
   you are acting for (from `git config user.name` or context; ask if
   unknown; never fabricate or credit the AI).
8. **Never use Spring or Lombok**, in any form. Never call slf4j
   directly; log through accent4j's `Logger`
   (`com.cinchapi.common.logging.Logger`) or the project's logging
   facade when it has one.
9. **Never guess at an API.** Verify every method, class, and import
   exists in the codebase or its dependencies before referencing it.
   If you cannot verify something exists, say so instead of using it.
10. **Changelog:** only edit `(TBD)` version blocks in `CHANGELOG.md`;
    if multiple exist and the target is ambiguous, ask. Never modify
    released (dated) entries.
11. **Preserve existing behavior.** Signatures, null returns,
    exception types, collection implementation choices (e.g.,
    `LinkedHashMap` ordering), and null-vs-`Optional` /
    `List`-vs-`Set` idioms are part of the contract even when
    undocumented.
12. **Write plainly.** All prose (chat replies, Javadoc, comments,
    changelog entries, documentation) follows the plain-language rules
    in the Writing Style section. Short sentences, active voice, no
    filler, no jargon, skimmable structure.

## Per-Project Facts

This file serves many repositories. Resolve each fact below from the
repo you are in; never assume, and never carry an answer over from
another project.

- **Java version.** Check `sourceCompatibility` /
  `targetCompatibility` (or a toolchain block) in `build.gradle`. If
  absent, check the CI JDK image in `.circleci/config.yml`. If still
  unclear, write Java 8. In a Java 8 project, do not use `var`,
  records, switch expressions, text blocks, `List.of()`,
  `Stream.toList()`, or any other post-8 feature or API. In a Java 21
  project, modern features are welcome where they match the
  surrounding style.
- **Copyright header.** Copy `spotless.java.license` exactly; the
  header differs by repo (Apache 2.0 vs. proprietary). New files use
  the current year; existing files keep their original years.
- **Test base class.** Extend the base class that neighboring tests
  extend, when the project has one.
- **Anything this file leaves open.** Follow the closest existing code
  in the repo.
- **Overrides.** A repo may override any rule here in its own
  repo-local instructions file; when the two conflict, the repo-local
  file wins.

## Writing Style: Plain Language

Write all prose in plain, direct language: replies to the user,
Javadoc, inline comments, commit messages, changelog entries, issue
text, and documentation. The goal is text a skimming human can digest
on the first pass. These rules take inspiration from ASD-STE100
Simplified Technical English but do not restrict vocabulary: use the
best word for the job, just never a fancy word where a plain one
works.

Structure:

- Lead with the point. State the conclusion or answer first, then the
  reasoning or detail.
- Keep sentences short (roughly 20 words or fewer). One idea per
  sentence. If a sentence needs a second clause to work, consider two
  sentences.
- One topic per paragraph, and keep paragraphs short (3-4 sentences).
- Use vertical lists for three or more parallel items instead of
  packing them into a sentence.
- Put warnings and caveats before the instruction or code they apply
  to, not after.

Word and sentence choices:

- Use the active voice. Say who does what.
- Write instructions in the imperative: "Run the tests", not "The
  tests should be run".
- Use the same word for the same thing throughout a document. Do not
  rotate synonyms for variety.
- Prefer the plain word: "use" not "utilize", "start" not "commence",
  "help" not "facilitate", "about" not "approximately".
- Define necessary technical terms briefly on first use; never use
  jargon to sound authoritative when a plain phrase says the same
  thing.
- Cut filler: "in order to", "it is important to note that", "as
  mentioned previously", "essentially", "basically".

Avoid these common LLM habits:

- No preamble or recap. Do not restate the question or announce what
  you are about to do ("Let me...", "I'll now..."); just do it.
- No hedging padding ("It's worth noting", "Generally speaking",
  "There are many factors to consider").
- No sycophancy ("Great question!") and no closing boilerplate ("Hope
  this helps!", "Let me know if...").
- No dense wall-of-text paragraphs. If a reply is longer than a few
  sentences, break it up so it can be skimmed.
- Do not over-qualify. State things directly; add a caveat only when
  it genuinely changes what the reader should do.

Scope: these rules govern word choice and sentence structure. The
Javadoc rules in this file govern content and markup ({@link} usage,
contract focus). Plain language does not apply to code identifiers,
quoted output, or text whose exact wording is fixed (e.g., released
changelog entries).

## Think Before Coding

Don't assume. Don't hide confusion. Surface tradeoffs. Before
implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them; don't pick
  silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

These guidelines bias toward caution over speed. For trivial tasks,
use judgment.

## Simplicity First

Minimum code that solves the problem. Nothing speculative.

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?"
If yes, simplify.

## Build

- `./gradlew build | test | publish | clean` (Gradle via the wrapper)
- Compile checks: `./gradlew compileJava compileTestJava` (plain
  `build` runs the whole test suite; avoid it locally).
- Run tests selectively: `./gradlew test --tests "SomeTest"` (class,
  method, and wildcard patterns). Run only the tests you wrote plus
  known tests that cover the change. Never run the full suite or
  expensive integration tests locally; CI runs those and surfaces
  any regressions.
- `./gradlew spotlessApply` is mandatory after code changes.
- Versioning is semantic. The base version lives in `.version`;
  `version.sh` appends a build counter and branch suffix (`develop` →
  `-SNAPSHOT`, `feature/foo` → `-FOO`, `master`/`release/*` → none).
  Set a new base with `./version.sh 2.0.0`; this also resets the build
  counter and patches the version string in `README.md`.
- CI: CircleCI (`.circleci/config.yml`) with timing-based test
  splitting.

### Dependencies

- Shared/cross-project libraries: add to `gradle/libs.versions.toml`,
  reference as `libs.<alias>` in `build.gradle`.
- Project-specific libraries: declare inline in `build.gradle` with a
  version string.

## Language and Libraries

- Write to the project's Java version (see Per-Project Facts).
- HTTP servers must follow the RESTQL spec and be designed for resql
  compatibility. Before you write or change an HTTP endpoint, read
  `docs/dev/restql.md` (condensed from the private
  https://github.com/cinchapi/restql repo, which is canonical).
- Preferred libraries; search these before writing any new utility:

| Purpose | Use |
|---|---|
| Collections, preconditions | Guava (`Lists.newArrayList()`, `ImmutableMap.of()`, `Preconditions`); Apache Commons also acceptable |
| String/object/map utilities | accent4j: `AnyStrings`, `AnyObjects`, `AnyMaps`, `Sequences`, `Collectives`, `Verify`, `Reflection`, `Types` |
| Checked → unchecked exceptions | accent4j `CheckedExceptions.throwAsRuntimeException()` |
| Logging | accent4j `Logger`, via the project's facade when one exists; never slf4j directly |
| HTTP server / client / config / CLI | `lib-http-server` (RESTQL-compatible), `lib-http-client`, `lib-config`, `lib-cli` |
| Storage / memory | `bucket`, `off-heap-memory` (Cinchapi) |
| JSON | Gson |
| Date/time | Joda-Time (where already used) |
| Nullability / concurrency annotations | `javax.annotation.Nullable`, `javax.annotation.concurrent.*` |

- Functional style: prefer lambdas over anonymous inner classes,
  method references (`String::valueOf`) where clearer, and the Stream
  API for collection transformations. Define custom
  `@FunctionalInterface` types (e.g., `TriFunction`, checked-exception
  variants) only when the standard
  `Function`/`Consumer`/`Supplier`/`Predicate` are insufficient.

## Formatting

Spotless enforces most of this; run it rather than hand-format, but
write code in this style from the start:

- 4-space indent (never tabs); continuation indent 8 spaces.
- Braces open at end of line; `else`, `catch`, `finally`, and do-while
  `while` go on a **new line** after the closing brace. `else if`
  stays compact.
- `if(x)` but `for (`, `while (`, `switch (`, `catch (`; see Critical
  Rule 5.
- Wrap long expressions by breaking **before** operators and
  **before** dots in method chains. Never wrap an assignment operator.
- One blank line between members and around imports; no blank line at
  the start of a method body; at most one consecutive blank line.
- Import groups, blank-line separated, alphabetized, no wildcards:
  `java.*` → `javax.*` → `org.*` → `com.*`.
- Static imports only for test assertions, `Preconditions.*`, and
  frequently used constants; not where the class name gives essential
  context at the call site.
- `@formatter:off/on` only where manual alignment genuinely aids
  readability (lookup tables, DSL-like code).

```java
if(condition) {
    return handleCaseA();
}
else if(other) {
    return handleCaseB();
}
else {
    return handleCaseC();
}
```

## File and Class Structure

- Every Java file starts with the repo's copyright header (see
  Per-Project Facts), then package, blank line, imports, blank line,
  type declaration.
- Member order: static constants → static fields → static factories →
  static utilities → instance fields (each with Javadoc) →
  constructors → public → protected → package-private → private
  instance methods → `@Override` methods
  (`equals`/`hashCode`/`toString`) → inner classes.
- JLS modifier order: `public static final transient volatile ...`.
- Default to the most restrictive visibility that works.

## Naming

- Short and concise; let context carry meaning (`fullName`, not
  `userFullName` inside a user class). No redundant qualifiers, no
  type encoding (`userList`), no abbreviations unless universal (`id`,
  `url`, `config`).
- **No `get` prefix on accessors**: `size()`, `fullName()`, `id()`.
  Verbs for actions: `transfer()`, `validate()`. Booleans read as a
  true/false question: `isActive()`, `hasPermission()`,
  `shouldRetry()`.
- Directional prepositions where the direction is ambiguous:
  `source.transferTo(destination)`.
- No `I` prefix on interfaces; name interfaces for the capability
  (`Byteable`, `Sorter`), implementations for their specific nature
  (`DirectMemory`, `CachedConnectionPool`). Avoid the `Abstract`
  prefix unless the name already contains the concept
  (`AbstractOffHeapMemory`).
- Utility classes: plural noun (`AnyStrings`, `Sequences`), declared
  `public final`, private constructor:
  `private AnyStrings() {/* no-init */}`.
- Packages by functional domain (`io`, `concurrent`, `collect`),
  never by layer (`controller`, `service`).
- Constants: `UPPER_SNAKE_CASE`, named for meaning not value
  (`MAX_RETRY_ATTEMPTS`, not `THREE`).
- Type parameters: single uppercase letters (`T`, `K`, `V`) for
  simple cases; descriptive names for complex generics.
- Underscore prefix allowed for internal framework fields distinct
  from user-visible state (`_realms`, `_audit`).

## Construction and Architecture Patterns

- Prefer static factories (`of`, `from`, `parse`, `wrap`, `create`)
  over public constructors when creation semantics benefit from names,
  caching, or polymorphic returns. Plain constructors are fine for
  simple, unambiguous classes.
- Builder pattern (static inner class, fluent, terminal `build()`) at
  5+ parameters with optionals.
- Convenience overloads delegate to the most-parameterized overload,
  which contains the real implementation; simpler versions pass
  default/sentinel values (e.g., `Time.NONE`).
- Favor composition over inheritance. Keep interfaces focused; static
  factories in interfaces are encouraged; give interfaces `default`
  methods for convenience overloads that delegate to abstract methods.
- Template/hook pattern: abstract base classes define the skeleton and
  call protected hooks (`beforeSave()`, `onLoad()`,
  `collectionSupplier()`). Never design hooks that require subclasses
  to call `super`.
- Forwarding/wrapper classes (e.g., `ForwardingConcourse`) add
  behavior (caching, logging, access control) without modifying the
  original.
- Singletons: constant `INSTANCE` or static `get()`, private
  constructor with `/* no-init */`.
- Immutability: favor it where the design allows. Make fields `final`
  when possible; make immutable classes `final` and annotate them
  `@Immutable`. Do not force immutability on genuinely mutable designs
  (builders, accumulators).
- Inner classes for private implementation details and implementations
  returned by outer-class factories; Javadoc them even when private.
- Constants live as `private static final` fields in their class;
  package-private in the most relevant class (or a dedicated
  `Constants` class) when shared. All constants get Javadoc.

## Documentation

- Document the **contract** (purpose, params, return, exceptions,
  pre/post conditions, thread safety), never the implementation,
  algorithm steps, or collaborators. Docs must survive refactors
  ("evergreen").
- Be DRY and thorough, not verbose: each sentence adds new
  information.
- Write all documentation prose plainly (see Writing Style above).
- `{@link ClassName}` for every class mention; plurals via display
  text: `{@link Node Nodes}` (never "`{@link Node}`s"); possessives:
  `{@link Customer Customer's}`.
- `{@code ...}` for parameter names and literals; `<p>` between
  paragraphs; `<ul>/<li>` for lists; `&mdash;` for em-dashes;
  `<strong>`/`<em>` for emphasis; `<h2>` for sections in long Javadoc;
  `NOTE:` (or `<strong>NOTE:</strong>`) for important caveats.
- Every enum constant gets its own Javadoc.
- Inline comments explain **why**, never **what**; if the "what"
  needs a comment, rename or extract instead. Tag important notes:
  `// NOTE:`, `// TODO:`, `// HACK:`, `// WARNING:`.
- No decorative section-divider comments (dashed lines, banner
  blocks). Structure comes from member order, not comments.
- Ternaries: fine at one level, acceptable at two with line breaks,
  use `if/else` beyond that.

```java
/**
 * Resolve the most appropriate {@link PricingTier} for a
 * {@link Customer} based on account history and current subscription
 * status.
 *
 * @param customer the {@link Customer} to evaluate; must have a
 *        non-null account ID
 * @return the resolved {@link PricingTier}, never {@code null}
 * @throws IllegalStateException if the {@link Customer Customer's}
 *         account is in an unresolvable state
 */
public PricingTier resolve(Customer customer) { ... }
```

## Nulls, Collections, Errors

- Return empty collections (`Collections.emptyMap()`,
  `ImmutableList.of()`), not `null`; return `null` only when absence
  is semantically distinct from empty. Mark with `@Nullable`.
- Do not introduce `Optional` where the codebase uses `null` as the
  signal, and do not change `List` return types to `Set` or vice
  versa.
- Wrap returned internal mutable collections
  (`Collections.unmodifiableSet(...)`) or return Guava immutables. Do
  **not** defensively copy constructor parameters or varargs (except
  mutable collections captured by `@Immutable` classes).
- Validate inputs with Guava `Preconditions` or accent4j `Verify`.
  Convert checked exceptions via accent4j `CheckedExceptions`. Custom
  domain exceptions extend `RuntimeException`. Never swallow
  exceptions or use them for control flow.
- Guava collection factories and immutables throughout; `Multimap`
  for multi-value keys; `LinkedHashMap`/`LinkedHashSet` when order
  matters; `ConcurrentHashMap`/`CopyOnWriteArrayList` for thread
  safety.
- Annotate: `@Nullable`, `@Immutable`, `@ThreadSafe`/`@NotThreadSafe`,
  `@VisibleForTesting`, `@Deprecated` for APIs scheduled for removal.
  `@SuppressWarnings` only with specific values (`"unchecked"`,
  `"rawtypes"`, ...), never blanket.

## Testing

- JUnit 4. Test classes extend the project's base test class when one
  exists (see Per-Project Facts) and use its `beforeEachTest()` /
  `afterEachTest()` hooks. Suites (`*Suite.class`) are excluded from
  normal runs.
- **Workflow (non-negotiable):** write failing tests → minimum code
  to pass → refactor green.
- Names: `test<Action><ExpectedBehavior>[When<Condition>]`, e.g.,
  `testAddReturnsFalseWhenValueAlreadyExists`.
- **Every `@Test` method needs Javadoc with exactly four `<strong>`
  sections** (Goal, Start state, Workflow as a `<ul>` of imperative
  steps, Expected), separated by `<p>`. Tests without this are
  rejected.

```java
/**
 * <strong>Goal:</strong> Verify a timed-out command is always failed,
 * even with exit code 0.
 * <p>
 * <strong>Start state:</strong> No prior state needed.
 * <p>
 * <strong>Workflow:</strong>
 * <ul>
 *   <li>Construct a {@link CommandResult} with exit code 0 and
 *       {@code timedOut = true}.</li>
 *   <li>Call {@code failed()} and {@code succeeded()}.</li>
 * </ul>
 * <p>
 * <strong>Expected:</strong> {@code failed()} returns {@code true};
 * {@code succeeded()} returns {@code false}.
 */
```

- Test behavior, not implementation: edge cases (empty, boundary,
  null, single element, max size), error conditions (right exception,
  right message), and concurrent correctness for shared-state
  components.
- Skip private methods (test via the public API), trivial accessors,
  and framework behavior.
- Use the project's randomized test data generators; hardcode values
  only to reproduce a specific bug (with an issue link).
- Bug-fix tests are named for the issue (`GH123`) and link to it.
- Use abstract base test classes for interface contracts; concrete
  subclasses supply the implementation.
- Concurrency tests: coordinate with `CountDownLatch`, communicate
  via atomics, always `join()` before asserting, set timeouts.

## Changelog (`CHANGELOG.md`)

- Update in the same change as every user-visible feature, behavior
  change, bug fix, or API break. Skip pure refactors, formatting,
  tests, and internal dependency bumps.
- Only under `(TBD)` versions; ask which when multiple and ambiguous.
  Never alter released (dated) entries.
- Write from the user's perspective; lead with the benefit. Past
  tense for fixes ("Fixed a bug where..."), present for features. Use
  existing `#####` section headers; add new ones only for distinct
  major feature areas.

## Git and Pull Requests

- PRs target the repo's integration branch (`develop` in most repos),
  never `master`/`main`. Pass the base explicitly (e.g.,
  `gh pr create --base develop`); if no integration branch exists,
  ask which branch to target instead of assuming master.
- If a branch was cut from `master` by accident, rebase it onto the
  integration branch before the PR.
- Commit subjects: imperative mood, prefixed with the issue when one
  exists (`GH-163: <summary>`).

## Issues (GitHub)

- Every issue: exactly one `type/*` (epic|story|bug|chore) and one
  `area/*` label from the repo's label set; `effort/*` and
  `priority/*` optional.
- Epics include a `## Sub-Issues` table; sub-issues start with
  `Parent: #NNN`. Include an Implementation Plan (numbered) and
  Acceptance Criteria (checklist).
- On close: verify acceptance criteria; if all siblings of an epic
  are closed, close the epic. Follow `docs/dev/issue-tracking.md`
  when the repo has it.

## Agent Conduct

- **Understand before changing:** trace callers and callees of
  anything you modify; read until sure. Don't guess.
- **Search before writing:** check the codebase, accent4j, and Guava
  for existing functionality before adding any method, utility, or
  class. Prefer generalizing existing code over duplicating it with
  tweaks.
- **Design for the caller:** before finalizing a signature, mentally
  write two or three call sites. If callers must cast, wrap,
  null-check unexpectedly, or guess parameter order, redesign the
  API.
- **Complete every ripple:** new enum constant → update every switch
  and dispatch chain; new field → `equals`/`hashCode`/`toString`/
  serialization/copy logic; new interface method → all
  implementations (or `default`); signature change → every caller.
  The compiler won't catch all of these.
- **Stay in scope:** minimal, focused changes. No speculative
  abstractions; extract only after real duplication (2-3
  occurrences). Keep refactoring/formatting separable from behavioral
  changes.
- **Boy Scout Rule, proportionally:** when touching code that
  violates these guidelines (implicit else, inline Javadoc, unclear
  names, missing `{@link}`s, stale comments), improve it, but don't
  restructure a module during a small fix.
- **Deliver complete code:** new files include header, package,
  imports, and full body; no pseudocode or placeholder stubs. Mind
  algorithmic complexity (no O(n) work inside O(n) loops; hoist loop
  invariants; `StringBuilder` in loops; right data structure for the
  access pattern).

## Vue / JavaScript

No semicolons; space before function parens (`function name ()`); no
trailing whitespace; follow the project's linter config and existing
conventions exactly.
