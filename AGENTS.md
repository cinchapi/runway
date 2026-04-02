# Concourse Development Guidelines

This document defines the coding style, architectural principles,
testing expectations, and documentation standards for the
**Concourse** codebase. All AI agents and human contributors must
follow these guidelines.

---

## Important: Do Not Run Tests

**Do not run `./gradlew test` or `./gradlew build`.** The test
suite requires a live Concourse server and takes too long to run
in an interactive session. Write tests, but do not execute them.
You may still run `./gradlew spotlessApply` for formatting.

---

## Project Overview

Concourse is a distributed database warehouse for transactions,
search, and analytics across time. Key capabilities include:

- **Automatic indexing** with constant-time writes
- **Version control** and time-travel queries
- **ACID transactions**
- **Document-graph** data model (schemaless)
- **Full-text search** (InfiNgram)

The codebase is a multi-module Gradle project with these core
subprojects:

| Module | Purpose |
|------------------------------|--------------------------------------|
| `concourse-server` | Core database server and storage |
| | engine |
| `concourse-driver-java` | Official Java client driver |
| `concourse-plugin-core` | Plugin framework |
| `concourse-shell` | CaSH interactive Groovy shell |
| `concourse-cli` | CLI tools for administration |
| `concourse-import` | Data import tools |
| `concourse-export` | Data export tools |
| `concourse-integration-tests`| Integration test suite |
| `concourse-ete-tests` | End-to-end and cross-version tests |
| `concourse-unit-test-core` | Base classes for unit tests |
| `concourse-ete-test-core` | End-to-end test framework |

The Thrift API is defined in `interface/concourse.thrift` and
related `.thrift` files. Client drivers for Python, PHP, and
Ruby also live in the repo.

---

## Reference Documentation

Before making changes, consult these documents to understand
how the system works and how changes should be structured:

| Document | What it covers |
|-------------------------------|--------------------------------------|
| `docs/dev/architecture.md` | Storage engine, data model, |
| | concurrency, transport, compaction, |
| | plugin system, and communication |
| | layer. Read this first for any |
| | server-side work. |
| `docs/dev/developer-guide.md` | Building from source, running the |
| | server locally, IDE setup, running |
| | tests, debugging tips, and common |
| | tasks (adding API methods, creating |
| | plugins, modifying the storage |
| | engine). |
| `docs/dev/testing.md` | Test frameworks |
| | (`ConcourseBaseTest`, |
| | `ConcourseIntegrationTest`, |
| | `ClientServerTest`, |
| | `CrossVersionTest`), conventions, |
| | and how to run tests. |
| `docs/dev/utilities.md` | Scripts in `utils/` for Thrift |
| | compilation, documentation |
| | generation, and dev tools. |
| `docs/dev/contributing.md` | Branch naming, commit messages, |
| | PR process, and code review. |
| `CHANGELOG.md` | Release history and change log. |
| `interface/*.thrift` | Thrift IDL defining the client |
| | &ndash; server API contract. |
| `docs/guide/src/` | User-facing guide covering data |
| | types, queries, search, |
| | transactions, graph features, |
| | and administration. |

When working on a subsystem you are unfamiliar with, **read the
relevant architecture and developer-guide sections** before
writing code. The developer guide includes step-by-step
instructions for common tasks like adding a new API method,
creating a plugin, and modifying the storage engine.

---

## Build System

- **Gradle 8.14.3** with the Gradle wrapper (`./gradlew`).
- Version management uses `version.sh` which reads `.version` for
  the base version and appends a build counter and branch suffix
  automatically.

### Key Commands

```
./gradlew build     # Compile, test, and package
./gradlew test      # Run tests only
./gradlew publish   # Publish to configured Maven repos
./gradlew clean     # Remove build artifacts
```

### Versioning

This project follows **semantic versioning** (`MAJOR.MINOR.PATCH`).
The base version is stored in `.version` (e.g., `1.0.0`).

`version.sh` produces a fully qualified version by appending a
build counter and a branch-derived suffix:

| Branch pattern   | Version produced         |
|------------------|--------------------------|
| `master`         | `1.0.0.42`               |
| `release/*`      | `1.0.0.42`               |
| `develop`        | `1.0.0.42-SNAPSHOT`      |
| `feature/foo`    | `1.0.0.42-FOO`           |

To set a new base version:

```
./version.sh 2.0.0
```

This updates `.version`, resets the build counter, and patches
the version string in `README.md`.

### CI/CD

CircleCI is configured in `.circleci/config.yml` with separate
jobs for setup, compile, test, and publish. Tests use
`circleci tests run` with timing-based splitting for parallelism.

### Dependency Management

Dependencies may be centralized in `gradle/libs.versions.toml`
(Gradle version catalog). Common dependencies available:

| Accessor                        | Library                      |
|---------------------------------|------------------------------|
| `libs.guava`                    | Google Guava                 |
| `libs.accent4j`                 | Cinchapi utility library     |
| `libs.junit`                    | JUnit 4                      |
| `libs.slf4j.api`               | SLF4J logging API            |
| `libs.logback`                  | Logback logging impl         |
| `libs.concourse.driver`        | Concourse database driver    |
| `libs.concourse.ete.test.core` | Concourse test framework     |

**Adding shared/common dependencies** (libraries used across
multiple Cinchapi projects or shared across subprojects in a
multi-project build): add them to the version catalog, then
reference via `libs.<alias>` in `build.gradle`.

```toml
# gradle/libs.versions.toml
[versions]
gson = "2.13.2"

[libraries]
gson = { group = "com.google.code.gson", name = "gson",
         version.ref = "gson" }
```

```groovy
// build.gradle
dependencies {
    implementation libs.gson
}
```

**Adding project-specific dependencies** (libraries only used
by this project with no version-sharing benefit): declare them
directly in `build.gradle` with an inline version.

```groovy
// build.gradle
dependencies {
    implementation 'com.example:some-lib:1.2.3'
}
```

---

## Language and Platform

- **Java 8 preferred.** Java 21 is also supported in some projects.
  Check the project's source compatibility setting.
- **Never use Spring.** No Spring Framework, Spring Boot, or any
  Spring library.
- Prefer Guava, Apache Commons, and Cinchapi internal libraries:
  `accent4j`, `bucket`, `lib-http-server`, `lib-config`,
  `lib-http-client`, `lib-cli`, `off-heap-memory`.
- HTTP servers must follow the RESTQL spec and be designed with
  resql compatibility.

---

## Formatting

### Indentation and Whitespace

- Use **4 spaces** for indentation. Never tabs.
- Continuation indentation is **8 spaces** (2 indentation units).
- Maximum line length is **80 characters** for both code and
  documentation.
- **CRITICAL: Use the full 80-character width.** Do not wrap
  early at 50, 60, or even 65 characters. When text continues
  on the next line, the preceding line must be filled as close
  to column 80 as possible. If a word fits on the current line
  without exceeding 80 characters, it MUST go on the current
  line. Wrapping at column 55-65 when content could reach 75-80
  is a formatting violation as serious as exceeding 80
  characters. The only acceptable short lines are: the final
  line of a paragraph, or a line where the next token is too
  long to fit.
- One blank line between methods, between type declarations, before
  and after imports, and between import groups.
- No blank lines at the beginning of a method body.
- Preserve at most one consecutive blank line.

### Braces

- Opening braces go at the **end of the line** for all constructs:
  classes, methods, constructors, control statements, lambdas,
  enums, annotations, array initializers, and anonymous classes.
- `else` goes on a **new line** after the closing brace of the
  `if` block.
- `catch` goes on a **new line** after the closing brace of the
  `try` block.
- `finally` goes on a **new line** after the closing brace of the
  `catch` block.
- `while` in a do-while goes on a **new line** after the closing
  brace.
- `else if` is compact (on the same line as `else`).

```java
if(condition) {
    return handleCaseA();
}
else if(otherCondition) {
    return handleCaseB();
}
else {
    return handleCaseC();
}

try {
    riskyOperation();
}
catch (SpecificException e) {
    handleSpecific(e);
}
catch (Exception e) {
    handleGeneral(e);
}
finally {
    cleanup();
}

do {
    process();
}
while (hasMore());
```

### Spaces in Control Structures

The `if` keyword is the **only** control keyword with **no space**
before its opening parenthesis. All other control keywords (`for`,
`while`, `switch`, `catch`, `try`, `synchronized`) have a space.

```java
// Correct — note if( vs for ( vs while (
if(condition) {
    for (int i = 0; i < n; i++) {
        while (running) {
            switch (state) {
                // ...
            }
        }
    }
}
```

- **No space before** the opening parenthesis of method invocations
  or method declarations: `method(arg1, arg2)`.
- **No space** inside parentheses for any construct.
- **Space before** opening brace of all blocks: `{`.
- Space after commas in all contexts.
- Space before and after binary operators: `a + b`, `x = y`.
- Space before and after lambda arrow: `(x) -> expr`.
- Space before colon in enhanced for: `for (T item : list)`.
- No space before semicolons.

### Operators and Wrapping

- Spaces around binary operators, assignment operators, and the
  ternary operator.
- When wrapping long expressions, break **before** the operator
  (binary and conditional).
- When wrapping method chains, break **before** the dot.
- Do not wrap assignment operators — keep them on the same line.
- Join wrapped lines where possible for readability.
- Switch case bodies indented relative to case, not to switch.
- Break indented inside case.

```java
boolean result = longConditionA
        || longConditionB
        && longConditionC;

String value = object.methodA()
        .methodB()
        .methodC();
```

### Import Order

Imports are organized into four groups, separated by blank lines,
in this order:

1. `java.*`
2. `javax.*`
3. `org.*`
4. `com.*`

Within each group, imports are sorted alphabetically. Unused imports
must be removed. No wildcard imports.

### Static Imports

Static imports are acceptable for:
- Assertion methods in tests (`Assert.*`)
- Preconditions (`Preconditions.*`)
- Common utility constants used frequently in a file

Avoid static imports for methods where the class name provides
essential context at the call site.

### Formatter Control

Use `@formatter:off` and `@formatter:on` comments sparingly, only
when the formatter would genuinely harm readability (e.g., carefully
aligned lookup tables, DSL-like constructions, or complex
conditionals where manual formatting is clearer).

```java
// @formatter:off
Map<String, Integer> codes = ImmutableMap.of(
    "OK",        200,
    "NOT_FOUND", 404,
    "ERROR",     500
);
// @formatter:on
```

---

## File Structure and Organization

### File Layout

1. License/copyright header
2. Package declaration
3. Blank line
4. Imports (grouped per above)
5. Blank line
6. Class/interface declaration

### Copyright Header

This project is licensed under Apache 2.0. Every Java file must
begin with the copyright header defined in
`spotless.java.license`. The exact header is:

```java
/*
 * Copyright (c) 2013-2026 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
```

### Class Body Organization

Organize class members in this order:

1. **Static constants** (public, then package-private, then private)
2. **Static fields** (mutable static state, if any)
3. **Static factory methods** (`of()`, `from()`, `wrap()`,
   `parse()`, `create()`, etc.)
4. **Static utility methods**
5. **Instance fields** (with Javadoc on each)
6. **Constructors**
7. **Public instance methods**
8. **Protected instance methods** (hook/template methods)
9. **Package-private instance methods**
10. **Private instance methods**
11. **`@Override` methods** (`equals`, `hashCode`, `toString`,
    `compareTo`, etc.) — at the end
12. **Inner classes/interfaces** — at the very bottom

### Access Modifier Ordering

Follow Java Language Specification (JLS) standard modifier order:

```
public/protected/private → static → final →
    transient/volatile → synchronized → native → strictfp
```

```java
public static final String NAME = "value";
private static final long NULL_ID = -1;
protected final transient DatabaseInterface db;
private transient boolean deleted = false;
```

---

## Naming

Names should be as **short and concise as possible** while still
being descriptive. Code should not cause cognitive overload. Every
extra word in a name is a tax on readability, so only include words
that earn their place.

### Let Context Do the Work

A name only needs to carry meaning that is not already obvious from
the surrounding context.

```java
// Redundant — "user" context is already established
public String generateFullName() {
    String userFullName = firstName + " " + lastName;
    return userFullName;
}

// Concise — context makes the meaning clear
public String generateFullName() {
    String fullName = firstName + " " + lastName;
    return fullName;
}
```

### Classes

- PascalCase. Short and clear.
- No prefixes like `I` on interfaces. No `Base` or `Concrete`
  prefixes on implementations.
- Interface names describe the capability or concept: `Byteable`,
  `Sorter`, `Router`, `Handler`, `Serializer`.
- Implementation names reflect their specific purpose:
  `UnsafeMemory`, `DirectMemory`, `DiskMemory`,
  `CachedConnectionPool`, `LinkedHashAssociation`.
- Abstract classes describe the concept. The `Abstract` prefix is
  acceptable only when the name already contains the concept (e.g.,
  `AbstractOffHeapMemory`), but prefer names without it.

### Methods and Functions

- camelCase.
- Methods that perform actions or have side effects should be
  **verbs**.
- Methods that access or derive a value without side effects do
  not need a `get` prefix. Name them for what they return.

```java
// Action methods
void transfer(Account destination) { ... }
void validate() { ... }
void sendNotification() { ... }

// Accessor methods — no "get" prefix
String fullName() { ... }
int size() { ... }
boolean isActive() { ... }
long id() { ... }
Set<String> realms() { ... }
Map<String, Object> computed() { ... }
```

- Factory methods: `of()`, `from()`, `parse()`, `wrap()`, `to()`,
  `ensure()`, `allocate()`, `create()`.
- Boolean accessors read as a true/false question: `isActive()`,
  `hasPermission()`, `shouldRetry()`, `isEmpty()`.

### Semantic Directionality

A method name must make sense on the object it belongs to. When you
read `object.method(argument)`, the directionality should be
unambiguous. Add a preposition (`To`, `From`, `Into`, `With`) when
needed.

```java
source.transferTo(destination);   // Clear
source.transfer(destination);     // Ambiguous
```

### General Conventions

| Element         | Convention                                      |
|-----------------|-------------------------------------------------|
| Classes         | Nouns or noun phrases. Short and specific.      |
| Interfaces      | Name for the capability, not the implementation.|
| Booleans        | Read as a true/false question: `isActive`,      |
|                 | `hasPermission`, `shouldRetry`.                 |
| Constants       | `UPPER_SNAKE_CASE`. Name conveys meaning, not   |
|                 | value. `MAX_RETRY_ATTEMPTS`, not `THREE`.       |
| Type parameters | Single uppercase letters (`T`, `K`, `V`) for    |
|                 | simple cases; descriptive names for complex     |
|                 | generics.                                       |
| Local variables | `camelCase`. No type encoding (`userList`,      |
|                 | `nameString`).                                  |

Avoid abbreviations unless universally understood (`id`, `url`,
`config`). When in doubt, spell it out.

### Utility Classes

- Plural noun of the type they operate on: `AnyStrings`,
  `AnyObjects`, `AnyMaps`, `Characters`, `Sequences`,
  `Collections`, `Types`.
- Declared `public final class`.
- Private constructor with `/* no-init */` or `/* noinit */`
  comment:

```java
private AnyStrings() {/* no-init */}
```

### Packages

- Organized by functional domain: `base`, `collect`, `reflect`,
  `io`, `concurrent`, `function`, `lang`, `data`, `cache`,
  `security`.
- Not by layer (no `controller`, `service`, `repository`
  packages).

### Internal/Framework Fields

Internal/framework state fields may use an underscore prefix when
they are internal metadata distinct from user-visible properties:
`_realms`, `_audit`, `_author`.

---

## Control Flow

### Explicit Branching

Do not use implicit else blocks. When logic has two or more
legitimate branches, make every branch explicit and visually
apparent.

```java
// Wrong — implicit else
if(condition) {
    return handleCaseA();
}
return handleCaseB();

// Correct — explicit else
if(condition) {
    return handleCaseA();
}
else {
    return handleCaseB();
}
```

Guard clauses for precondition validation (null checks, argument
validation at the top of a function) are acceptable because they
protect the function's contract rather than represent branches of
business logic.

### Ternary Operators

Use ternaries for simple conditional expressions. They are fine
for one-level conditionals and even two-level nesting when
formatted with line breaks for clarity. Avoid deeper nesting.

```java
// Simple — good
return raw == TObject.NULL ? null : Convert.thriftToJava(raw);

// Two-level with formatting — acceptable
stored = stored == null
        ? (timestamp == Time.NONE
                ? store.select(record)
                : store.select(record, timestamp))
        : stored;

// Three+ levels — use if/else instead
```

---

## Construction Patterns

### Static Factory Methods

Prefer static factory methods over public constructors for classes
that benefit from descriptive creation semantics, polymorphic
return types, or caching.

```java
public static LockBroker create() {
    return new LockBroker(true);
}

public static LockBroker noOp() {
    return NO_OP;
}
```

### When to Use Constructors

Public constructors are acceptable for simple classes with a small
number of required parameters where a single constructor signature
is unambiguous and descriptive enough. Static factories are
preferred when there are **multiple permutations of 5 or fewer
constructor parameters** and it is better to have named factories
for clarity (e.g., `connect(host, port)` vs `connect(url)`).

### When to Use Builders

Use the Builder pattern when a class has **5 or more parameters**
and some are optional. Builder class is typically a static inner
class with a fluent API and a terminal `build()` method.

```java
Runway runway = Runway.builder()
        .host("localhost")
        .port(1717)
        .username("admin")
        .password("admin")
        .environment("production")
        .build();
```

### Method Overloading

When providing convenience overloads, the simpler version (fewer
parameters) should **delegate to the more complex version** by
passing default/sentinel values. The most-parameterized overload
contains the real implementation.

```java
/**
 * Browse {@code key} in the {@code store}.
 *
 * @param store the {@link Store} to browse
 * @param key the key to browse
 * @return the browsed data
 */
public static Map<TObject, Set<Long>> browse(
        Store store, String key) {
    return browse(store, key, Time.NONE);
}

/**
 * Browse {@code key} in the {@code store} at
 * {@code timestamp}.
 *
 * @param store the {@link Store} to browse
 * @param key the key to browse
 * @param timestamp the historical timestamp
 * @return the browsed data
 */
public static Map<TObject, Set<Long>> browse(
        Store store, String key, long timestamp) {
    // Real implementation here
}
```

---

## Documentation

### Javadoc Scope

- Javadoc **everything**: classes, methods, fields, constructors,
  inner classes, enums, enum constants — even if they are private.
- Do **not** document `@Override` methods.
- Always use block comment format, even for single-line Javadoc.
- Always attribute class documentation with an `@author` tag
  identifying the person whose work the code represents. For
  AI agents, this is the user you are acting on behalf of.
  Determine the name from Git configuration
  (`git config user.name`) or from context provided by the
  user. If you cannot determine the name, ask before
  proceeding. Never fabricate or assume an author name.

### Line Length

Javadoc must not exceed **80 characters** per line under any
circumstance. Equally important: **use the full width
available.** When reflowing Javadoc text, pack as many words
onto each line as will fit without exceeding 80 characters. A
line that ends at column 60 when the next word would only bring
it to column 72 is WRONG — that word belongs on the current
line.

**Self-check**: After writing any Javadoc block, scan each
continuation line. If the line ends well before column 80 and
the first word of the next line could have fit, the text needs
reflowing. Short final lines (end of paragraph or sentence) are
acceptable.

### Document the Essence, Not the Implementation

- Class documentation should answer "what is this thing?" in
  isolation.
- Method documentation defines the **contract**: what it does, what
  it accepts, what it returns, and when it throws.
- Never describe internal algorithms, data structures, helper
  methods, or step-by-step logic. Implementation details change
  and are not important to the consumer.

```java
// Wrong — describes collaborators and implementation
/**
 * A Transport that handles low-level network I/O
 * in a PeerToPeerNetwork, using async NIO channels.
 * Higher-level protocol concerns are managed by the
 * network layer.
 */

// Correct — describes what it IS
/**
 * A {@link Transport} manages communication between
 * {@link Node Nodes}.
 */
```

### Method Documentation Style

- Write method documentation as a **verb in the imperative**, not
  indicative. Say "Return..." not "Returns...".

```java
// Wrong
/**
 * Returns the active user count.
 */

// Correct
/**
 * Return the number of currently active
 * {@link User Users}.
 */
```

### Class References and Referential Integrity

- Use `{@link ClassName}` to refer to all classes, including the
  class being documented.
- For pluralization, use the `{@link}` display text feature:
  `{@link Node Nodes}` — never "`{@link Node}` instances" or
  "`{@link Node}`s".
- For possessives: `{@link Customer Customer's}`.

### What to Include for Methods

- **Purpose and intent** — what the method does and why it exists.
- **Parameters** (`@param`) — what each parameter represents
  semantically.
- **Return value** (`@return`) — what the caller receives and what
  it means.
- **Exceptions** (`@throws`) — when and why it fails.
- **Preconditions** — what must be true before calling.
- **Postconditions** — what the caller can rely on after the call.
- **Thread safety or side effects** — if relevant.

### What to Exclude

- Step-by-step algorithm descriptions.
- References to internal variables or helper methods.
- Lists of everything the function currently does.
- Anything that would need updating when the implementation is
  refactored.

### Javadoc Formatting Details

- **Be DRY.** Do not repeat the same point multiple times. Each
  sentence should add new information.
- **Be evergreen.** Javadoc should allow for future expansion or
  changes without requiring modification. Discuss the essence,
  purpose, and intention. Do not chronicle a specific list of
  functionality that may change.
- **Be thorough but not verbose.** Cover the contract, usage
  patterns, important semantics, and edge cases. Do not pad with
  filler.
- Use `{@code value}` for parameter names, literal values, and
  inline code.
- Use `<p>` for paragraph breaks within Javadoc.
- Use `<strong>` for emphasis, `<em>` for lighter emphasis.
- Use `<ul>/<li>` for bulleted lists.
- Use `&mdash;` for em-dashes.
- Use `<h2>` for section headers in long Javadoc blocks.
- Use `NOTE:` or `<strong>NOTE:</strong>` for important caveats.

### Enum Documentation

Every enum constant must have its own Javadoc block explaining its
purpose:

```java
public enum NavigationKeyFinder {

    /**
     * Automatically select the most efficient traversal
     * strategy based on the characteristics of the data.
     */
    AUTO,

    /**
     * Use forward traversal, starting with records that
     * have outgoing links on the first stop of the
     * navigation path and following links forward to
     * find matches.
     * <p>
     * This strategy is generally more efficient when
     * there are fewer start records than end records.
     * </p>
     */
    FORWARD_TRAVERSAL,

    /**
     * Use reverse traversal.
     */
    REVERSE_TRAVERSAL;
}
```

### Example

```java
/**
 * A {@link PricingTier} represents a level of service and
 * billing that can be assigned to a {@link Customer}.
 *
 * @author <the user you are acting on behalf of>
 */
public class PricingTier { ... }

/**
 * Resolve the most appropriate {@link PricingTier} for a
 * {@link Customer} based on account history and current
 * subscription status.
 *
 * @param customer the {@link Customer} to evaluate; must
 *        have a non-null account ID
 * @return the resolved {@link PricingTier}, never
 *         {@code null}
 * @throws IllegalStateException if the
 *         {@link Customer Customer's} account is in an
 *         unresolvable state (e.g., suspended with no
 *         prior tier)
 */
public PricingTier resolve(Customer customer) { ... }
```

### Inline Comments

Inline comments exist to provide context that is not self-evident
from the code. They explain **why**, never **what**.

```java
// Correct — explains non-obvious reasoning
// Offset by 1 because the API uses 1-based indexing
int page = requestedPage + 1;

// Wrong — narrates the code
// Add 1 to the page number
int page = requestedPage + 1;
```

If you find yourself wanting to write a "what" comment, rename the
variable or extract a well-named function instead.

### Comment Tags

Use uppercase tag prefixes in inline comments to flag important
notes:

- `// NOTE:` — important semantic detail or non-obvious behavior
- `// TODO:` — known technical debt or deferred work
- `// HACK:` — intentional shortcut that should be revisited
- `// WARNING:` — dangerous or fragile code

```java
// NOTE: The use of #cache instead of #delegate is
// intentional because we want the stale value if the
// delegate is unavailable
return cache.getOrDefault(key, fallback);
```

---

## Annotations

Use annotations to communicate important properties of code:

| Annotation           | Usage                                         |
|----------------------|-----------------------------------------------|
| `@Nullable`          | Parameters or return values that may be null. |
| `@Immutable`         | Classes that are immutable.                   |
| `@ThreadSafe`        | Classes designed for concurrent access.       |
| `@NotThreadSafe`     | Classes not designed for concurrent access.   |
| `@VisibleForTesting` | Access widened solely for test reachability.  |
| `@Deprecated`        | APIs scheduled for removal.                   |
| `@Override`          | All overriding methods (no Javadoc needed).   |

### @SuppressWarnings

Use `@SuppressWarnings` with specific, narrow values. Never
suppress all warnings. Common values:

- `"unchecked"` — generic type casts
- `"rawtypes"` — raw generic types
- `"serial"` — missing `serialVersionUID`
- `"unused"` — deliberately unused parameters
- `"deprecation"` — intentional use of deprecated API
- `"restriction"` — use of restricted APIs (e.g., `sun.misc.*`)

When suppressing multiple warnings, use array syntax:

```java
@SuppressWarnings({ "unchecked", "rawtypes" })
```

---

## Architecture

### Design Principles

Before writing code, think about design:

- Does this need to exist, or does something already handle it?
- Where does this logically belong in the codebase's structure?
- What are the likely axes of future change, and am I making those
  changes easy?
- Am I introducing coupling that doesn't need to exist?

Favor composition over inheritance. Favor explicit contracts over
implicit conventions. Favor small, focused units of work over
large, monolithic ones. Design interfaces and abstractions around
**purpose and intent**, not around the current implementation.

### DRY and Reuse

Do not repeat yourself. If logic exists elsewhere, reuse it. If
two pieces of code are doing nearly the same thing, extract the
commonality and parameterize the differences.

When adding new functionality, actively look for existing
components, utilities, or patterns that can be leveraged. If
existing code almost fits but needs slight modification, prefer
refactoring it to be more general over duplicating it with tweaks.

### Code Organization

Organize code by **domain and purpose**, not by technical layer
alone. Group things that change together. Separate things that
change for different reasons.

When adding to an existing codebase, respect and follow its
established patterns and conventions. If those patterns conflict
with these principles, make a judgment call: small deviations can
be noted with a TODO; large structural issues can be incrementally
improved.

### Interface Design

- Interfaces should be focused and cohesive.
- Provide **default methods** for convenience overloads that
  delegate to abstract methods. This reduces boilerplate in
  implementations.
- Static factory methods in interfaces are encouraged.
- Use composition of interfaces over deep inheritance hierarchies.
- Do not prefix interface names with `I`.

### Template Method / Hook Pattern

- Abstract base classes define skeleton behavior.
- Subclasses override protected hook methods to customize:
  `beforeSave()`, `onLoad()`, `collectionSupplier()`,
  `mapSupplier()`.
- Do not force subclasses to call `super` — design the template
  so the framework calls the hooks.

### Wrapper/Forwarding Pattern

- Use forwarding classes (e.g., `ForwardingConcourse`) to wrap
  and delegate to another implementation.
- Allows adding behavior (caching, logging, access control)
  without modifying the original.

### Inner Classes

- Use for private implementation details that should not be
  visible outside the enclosing class.
- Use for concrete implementations that are returned by factory
  methods of the outer class.
- Always Javadoc inner classes, even private ones.

### Singleton Pattern

- Private constructor with `/* no-init */` comment.
- Static `get()` or constant `INSTANCE` for access.

```java
public static Anonymous get() {
    return INSTANCE;
}

private Anonymous() {/* no-init */}
```

### Immutability

- Favor immutable classes where the design allows it.
- Use `@Immutable` from `javax.annotation.concurrent` to annotate
  immutable classes.
- Make fields `final` when possible.
- Make immutable classes `final` to prevent subclassing that could
  violate immutability.
- Allow mutable designs when the domain requires it (e.g., builder
  state, accumulation patterns) — do not force immutability where
  it creates unnecessary complexity.

### Visibility

Default to the **most restrictive visibility** that works:
- Classes that are only used within a package should be
  package-private, not public.
- Fields should be private unless there is a specific reason to
  widen access.

### Constants

- Constants that are private to a class stay as `private static
  final` fields in that class.
- Constants shared across multiple classes in the same package
  use package-private access in the most relevant class, or a
  dedicated `Constants` class if they are truly cross-cutting.
- All constants must have Javadoc.

---

## Agent Behavioral Rules

These rules address judgment calls that experienced engineers make
instinctively but that must be made explicit for AI agents.

### Understand Before Changing

Before modifying any code, **trace the full call path**. Read the
callers of the method you are changing. Read the methods it calls.
Understand how data flows into and out of the code you are about
to touch.

Before writing any new method, utility, or class, **search the
codebase and its dependencies** for existing functionality that
already does what you need. Check accent4j, Guava, and the
project's own utility classes. Never add a method that already
exists under a different name. Never add a utility that an
existing library already provides.

If you are unsure how something works, read the code until you
are sure. Do not guess. Do not assume.

### Preserve Existing Behavior

When modifying existing code, the default posture is
**conservative**. Do not change method signatures, return types,
parameter names, or observable behavior unless explicitly asked
to. Side effects of existing code are part of its contract even
if they are undocumented.

Do not "improve" code in ways that subtly break callers. If a
method currently returns `null` in certain cases, callers may
depend on that. If a method throws an exception under certain
conditions, callers may catch it. These behaviors are part of
the API whether or not they appear in the Javadoc.

### Think About Callers

Always read code from the **caller's perspective**. A method's
signature should make the call site read naturally. Before
finalizing a method signature, mentally write out two or three
call sites and verify they are clean and intuitive.

If the caller has to do ceremony — casting the return type,
wrapping arguments, null-checking the result, remembering which
parameter is which because several share the same type — the API
is wrong. Redesign it.

```java
// Bad — caller must remember parameter order
void copy(String source, String destination, boolean overwrite)

// Better — directionality is unambiguous
void copyTo(Path destination, CopyOption... options)
```

### Don't Silently Change Semantics

**Collection implementation types are part of the contract.**
`LinkedHashMap` preserves insertion order — switching to `HashMap`
may break callers who depend on order even if the declared type
is `Map`. `TreeSet` maintains sort order. `ArrayList` allows
random access. Do not swap implementations without understanding
the downstream impact.

**Exception propagation is part of the contract.** Do not add
try/catch blocks that swallow or wrap exceptions unless the
design requires it. Do not convert checked exceptions to
unchecked if the caller expects to catch them.

Do not wrap values in `Optional` when the codebase uses `null`.
Do not add default values where `null` was the intended signal.
Do not change `List` return types to `Set` or vice versa.

### Completeness

When you add something, **trace every usage site** and update
all affected code:

- Adding an **enum constant**? Update every `switch` statement
  and every conditional chain that dispatches on that enum.
- Adding a **field** to a class? Update `equals()`,
  `hashCode()`, `toString()`, serialization logic, and any
  copy/clone methods.
- Adding a **method to an interface**? Update every
  implementation class. Consider whether it should be a
  `default` method.
- Adding a **constructor parameter**? Update all factory methods,
  builders, and call sites.
- Changing a **method signature**? Update every caller.

The compiler catches some of these; not all. You are responsible
for the ones it does not.

### Performance Awareness

Do not optimize prematurely, but do not be needlessly wasteful.

- Be aware of **algorithmic complexity**. Do not do O(n) work
  inside an O(n) loop, creating O(n²) behavior.
- **Hoist loop-invariant computations** out of loops. If a value
  does not change across iterations, compute it once before the
  loop.
- Use `StringBuilder` for string concatenation in loops, not
  the `+` operator.
- **Choose the right data structure** for the access pattern.
  `HashSet` for membership checks, `LinkedHashMap` for ordered
  key-value access, `ArrayList` for indexed access.
- Be mindful of unnecessary object creation in hot paths.

### Don't Fabricate

**Never guess at an API.** Every method call, class reference,
and import in your code must correspond to something that
actually exists in the codebase or its declared dependencies.

If you are not sure whether a method exists, **search for it
before using it**. Do not assume that `AnyStrings.format()`
exists because it sounds reasonable — look it up. Do not assume
a class is in a particular package because it seems logical —
verify it.

If you cannot verify that something exists, **say so
explicitly** rather than writing code that references it.
Fabricated API calls cause compilation failures and erode trust.

### Scope Discipline

Every change should be **minimal and focused**. Solve the problem
at hand. Do not introduce abstractions, interfaces, or
generalization until the second or third time a pattern repeats —
an abstraction should be extracted from observed duplication, not
predicted from imagined future needs.

Do not mix unrelated modifications in the same change. If you
notice an opportunistic improvement while working on a feature,
it should be clearly separable from the primary work. Formatting
fixes, renaming, and refactoring should not be tangled with
behavioral changes.

When asked to add a feature, resist the urge to build a
framework. A single concrete class is almost always the right
starting point.

---

## Null Handling

- Prefer returning **empty collections** (`Collections.emptyMap()`,
  `ImmutableList.of()`) over `null` when the result is a
  collection type.
- Only return `null` when the absence of a value is **semantically
  distinct** from an empty result.
- Use `@Nullable` from `javax.annotation` on parameters and return
  types that legitimately accept or produce null.
- Validate non-null preconditions at method boundaries using
  `Preconditions.checkNotNull()` or `AnyObjects.checkNotNull()`.

---

## Defensive Copying and Return Values

- **Return values**: Wrap mutable internal state with
  `Collections.unmodifiableSet()`, `Collections.unmodifiableMap()`,
  etc., or return Guava `Immutable*` collections. Do not expose
  raw internal collections that callers could mutate.
- **Constructor parameters**: Generally trust the caller. Do not
  defensively copy parameters unless the class is `@Immutable` and
  the parameter is a mutable collection that must be snapshot.
- **Varargs**: Process varargs directly — do not defensively copy
  the array.

```java
public Set<String> realms() {
    return Collections.unmodifiableSet(_realms);
}

public Map<String, Object> derived() {
    return Collections.emptyMap();
}
```

---

## Error Handling

- Use **Guava Preconditions** or **accent4j Verify** for input
  validation:

```java
Preconditions.checkArgument(
        files.length > 0,
        "Must include at least one file");
Verify.thatArgument(x > 0, "Expected positive value");
```

- Convert checked exceptions to unchecked using
  `CheckedExceptions.throwAsRuntimeException()` or
  `CheckedExceptions.wrapAsRuntimeException()` from accent4j.
- Define custom exception classes extending `RuntimeException` for
  domain-specific errors.
- Do not swallow exceptions silently. At minimum, log or rethrow.
- Do not use exceptions for control flow.

---

## Collections and Data Structures

- Use **Guava factories** for collection creation:
  `Lists.newArrayList()`, `Maps.newLinkedHashMap()`,
  `Sets.newHashSet()`, `Maps.newHashMap()`,
  `Lists.newArrayListWithCapacity(n)`.
- Use **Guava immutable collections** for constants and defensive
  returns: `ImmutableSet.of()`, `ImmutableMap.copyOf()`,
  `ImmutableList.of()`.
- Use `Multimap` from Guava when a key maps to multiple values.
- For thread-safe collections, use `ConcurrentHashMap`,
  `CopyOnWriteArrayList`, or Guava's concurrent utilities.
- Prefer `LinkedHashMap` and `LinkedHashSet` when insertion order
  matters.

---

## Functional Patterns

- Use **lambdas** where a functional interface is expected. Prefer
  lambdas over anonymous inner classes.
- Use **method references** when they improve clarity:
  `String::valueOf`, `Entry::getKey`, `ArrayList::new`.
- Use the **Stream API** for collection transformations: `map()`,
  `filter()`, `collect()`, `flatMap()`.
- Prefer `Collectors.toList()`, `Collectors.toSet()`,
  `Collectors.toMap()`.
- Define custom `@FunctionalInterface` types when the standard
  `Function`, `BiFunction`, `Consumer`, `Supplier`, `Predicate`
  are insufficient (e.g., `TriFunction`, checked-exception
  variants like `ExceptionalBiFunction`).
- Use `Optional` sparingly. The codebase historically prefers
  `@Nullable` annotations with explicit null checks over
  `Optional`.

---

## Unit Testing

### Tests Come First

**Always write tests before writing the implementation.** This is
non-negotiable. For every new behavior, feature, or bug fix, the
workflow is:

1. Write one or more test methods that define the expected behavior.
   The tests should fail (or not compile) because the production
   code does not yet exist or does not yet handle the case.
2. Write the minimum production code to make the tests pass.
3. Refactor if needed, keeping the tests green.

This applies to all work — new features, new methods, bug fixes,
and behavioral changes. Do not write production code first and
backfill tests afterward. The tests are the specification; they
drive the design.

For **bug fixes**, the reproduction test must demonstrably fail
against the current code before the fix is applied. This proves
the test actually catches the bug.

### Framework

Tests use JUnit 4 with the following conventions:

- All test classes extend the project's base test class, which
  provides lifecycle hooks (`beforeEachTest()`,
  `afterEachTest()`).
- Use `@Test` on every test method.
- Test suites (`*Suite.class`) are excluded from normal test runs.

### Test Class Structure

```java
/**
 * Unit tests for {@link MyComponent}.
 *
 * @author <the user you are acting on behalf of>
 */
public class MyComponentTest extends ConcourseBaseTest {

    private MyComponent component;

    @Override
    protected void beforeEachTest() {
        component = MyComponent.create();
    }

    @Override
    protected void afterEachTest() {
        component.shutdown();
    }

    @Test
    public void testAddReturnsTrueForNewEntry() {
        // ...
    }
}
```

### Test Method Javadoc

Every `@Test` method must have a block Javadoc comment with
exactly four sections, each introduced by a `<strong>` label.
This is **non-negotiable** — tests without this documentation
will be rejected.

The four required sections are:

1. **Goal** — one sentence stating what the test verifies.
2. **Start state** — what must be true before the test runs
   (e.g., "A freshly created sandbox" or "No prior state
   needed").
3. **Workflow** — a bulleted list (`<ul>/<li>`) of the
   discrete steps the test performs. Each bullet should be a
   short, imperative statement.
4. **Expected** — the assertion(s) the test makes and what
   constitutes a pass.

Use `<p>` to separate sections. Use `{@link}` and `{@code}`
for class and value references, as in all other Javadoc.

```java
/**
 * <strong>Goal:</strong> Verify that a timed-out command is
 * always considered failed, even if the exit code is 0.
 * <p>
 * <strong>Start state:</strong> No prior state needed.
 * <p>
 * <strong>Workflow:</strong>
 * <ul>
 *   <li>Construct a {@link CommandResult} with exit code 0
 *       and {@code timedOut = true}.</li>
 *   <li>Call {@code failed()}, {@code succeeded()}, and
 *       {@code timedOut()}.</li>
 * </ul>
 * <p>
 * <strong>Expected:</strong> {@code failed()} and
 * {@code timedOut()} return {@code true};
 * {@code succeeded()} returns {@code false}.
 */
@Test
public void testFailedReturnsTrueWhenTimedOut() {
    // ...
}
```

### Naming

Test method names should be descriptive sentences that explain
what is being tested and the expected outcome. Use the pattern
`test<Action><ExpectedBehavior>[When<Condition>]`.

```java
// Good
public void testAddReturnsFalseWhenValueAlreadyExists() {}
public void testFindReturnsEmptySetWhenNoCriteriaMatch() {}
public void testWriteIsRangeBlockedIfReadingAllValues() {}

// Bad
public void testAdd() {}
public void test1() {}
```

### Randomized Test Data

Use the project's test data generators to create random inputs.
This broadens coverage beyond hardcoded cases and catches edge
cases over many runs. When a specific value is needed to reproduce
a bug, use a hardcoded value with a comment linking to the issue.

### Abstract Base Test Classes

Use abstract base test classes to define reusable test contracts
for interface or abstract class hierarchies. Concrete test classes
supply the implementation under test while inheriting the full
suite of behavioral tests.

```java
public abstract class StoreTest
        extends AbstractStoreTest {

    // Tests that apply to ALL Store implementations
}

public class EngineTest extends StoreTest {

    @Override
    protected Store getStore() {
        return new Engine(...);
    }
}
```

### Concurrency Testing

For concurrent tests:

- Use `CountDownLatch` to coordinate thread start and finish.
- Use `AtomicBoolean` or `AtomicReference` to communicate results
  between threads.
- Always `join()` threads before asserting results.
- Set reasonable timeouts to detect deadlocks.

```java
@Test
public void testConcurrentAccess()
        throws InterruptedException {
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(1);
    AtomicBoolean success = new AtomicBoolean(false);
    Thread t = new Thread(() -> {
        startLatch.countDown();
        try {
            finishLatch.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        success.set(true);
    });
    t.start();
    startLatch.await();
    // ... exercise the system while thread is active ...
    finishLatch.countDown();
    t.join();
    Assert.assertTrue(success.get());
}
```

### Bug Reproduction Tests

When fixing a bug, write a reproduction test that fails before
the fix and passes after. Name it after the issue tracker
identifier (e.g., `GH123`, `CON456`) and link to the issue in
the Javadoc.

### What to Test

- **Behavior, not implementation.** Test what a component does,
  not how it does it internally.
- **Edge cases.** Empty inputs, boundary values, null arguments,
  single elements, maximum sizes.
- **Error conditions.** Verify that the code throws the right
  exceptions with the right messages under the right
  circumstances.
- **Concurrent correctness.** For shared-state components, test
  that concurrent access does not cause corruption, deadlocks,
  or exceptions.

### What NOT to Test

- Private methods directly (test them through the public API).
- Trivial getters/setters with no logic.
- Framework behavior (e.g., testing that JUnit runs tests).

---

## Libraries and Dependencies

### Preferred Libraries

| Purpose                | Library                               |
|------------------------|---------------------------------------|
| Collections/Utilities  | Guava (`com.google.common.*`)         |
| Preconditions          | `Preconditions` (Guava)               |
| Verification           | `Verify` (accent4j)                   |
| String utilities       | `AnyStrings` (accent4j)              |
| Object utilities       | `AnyObjects` (accent4j)              |
| Checked exceptions     | `CheckedExceptions` (accent4j)       |
| Reflection             | `Reflection` (accent4j)              |
| Type utilities         | `Types` (accent4j)                   |
| Map utilities          | `AnyMaps` (accent4j)                 |
| Collection utilities   | `Sequences`, `Collectives` (accent4j)|
| Nullability            | `javax.annotation.Nullable`          |
| Concurrency annot.     | `javax.annotation.concurrent.*`      |
| HTTP server            | `lib-http-server`                    |
| Configuration          | `lib-config`                         |
| HTTP client            | `lib-http-client`                    |
| CLI framework          | `lib-cli`                            |
| Off-heap memory        | `off-heap-memory`                    |
| JSON                   | Gson                                  |
| Date/time              | Joda-Time (where already used)       |

### Banned Libraries

- Spring Framework (all modules)
- Lombok

---

## Vue/JavaScript Rules

When writing Vue or JavaScript code:

- **No semicolons.**
- **Space before function parentheses:** `function name ()` and
  `method ()`.
- **No trailing whitespace.**
- Follow the linter configuration of the project exactly.
- Follow the conventions of the existing codebase precisely.

---

## Code Delivery Rules

- For **new files**: provide the complete file with no omissions.
  Include the copyright header, package, imports, full class body,
  and all methods.
- For **modifications to existing files**: provide only the changed
  or added code, with clear indication of where it should go.
- All code must be **copy-paste ready**. No pseudocode, no
  placeholders, no `// TODO` stubs (unless explicitly representing
  a real deferred task).
- Always examine the entire codebase context to understand
  available APIs, methods, and existing patterns before writing
  code. Do not reinvent what already exists.
- Write associated unit tests where appropriate.
- Code must be **performant and scalable**. Algorithms should be
  well thought out.
- **Run `./gradlew spotlessApply` after finishing all code
  changes.** This applies the project's Spotless formatter
  (Eclipse JDT with the project's configuration) to ensure every
  file conforms to the 80-character line limit, import ordering,
  brace placement, and all other formatting rules. Do not skip
  this step — unformatted code will fail CI. If `spotlessApply`
  modifies any files, review the changes to confirm they are
  purely cosmetic before proceeding.

---

## Opportunistic Improvement

When you touch existing code — whether modifying, extending, or
working adjacent to it — and you notice it violates these
principles, improve it. This includes:

- Extracting duplicated logic into shared utilities.
- Adding explicit else blocks where implicit ones exist.
- Replacing unclear variable names with intention-revealing ones.
- Trimming names that carry redundant context.
- Adding missing contract documentation to public interfaces.
- Removing stale or misleading comments.
- Converting inline Javadoc to block format.
- Adding `{@link}` references where classes are mentioned in
  documentation without them.

Apply the Boy Scout Rule: leave the code better than you found it.
But use good judgment about scope — a small bug fix is not the time
to restructure an entire module. Make improvements proportional to
the task at hand.

---

## Attribution

- All class-level Javadoc must include an `@author` tag
  identifying the person whose work the code represents.
  For AI agents, this is the user you are acting on behalf
  of &mdash; **not** the AI itself or a hardcoded name.
  Determine the actual author name from Git configuration
  (`git config user.name`), from context provided by the
  user, or by asking the user directly. Never guess.
- All new files must include the project's copyright header
  (see `spotless.java.license`).

---

## Summary of Non-Negotiables

1. **4-space indentation, spaces only.** No tabs, ever.
2. **80-character line limit** for code and documentation. Use
   the full width available — do not wrap early. Wrapping at
   55-65 characters when content fits to 75-80 is a violation.
3. **`else`/`catch`/`finally` on a new line** after the closing
   brace.
4. **`if(` has no space; `for (`/`while (`/`switch (` have a
   space.** This is asymmetric and intentional.
5. **Explicit branching.** No implicit else. All logic branches
   are visually and structurally clear.
6. **Import order:** `java` > `javax` > `org` > `com`, with blank
   lines between groups. No unused imports.
7. **Javadoc everything** except `@Override` methods. Always block
   format. Always `@author` on classes, using the actual name of
   the user you are acting on behalf of (from `git config` or
   context).
8. **Document contracts, not implementations.** Evergreen
   documentation that survives refactors.
9. **Inline comments explain why, not what.** If the "what" isn't
   obvious, fix the code.
10. **Naming:** concise, descriptive, context-aware. Verbs for
    actions. No `get` prefix on accessors. Semantic directionality
    on methods. No redundant qualifiers.
11. **DRY.** One thing, one place. Extract and reuse relentlessly.
12. **Return empty collections, not null.**
13. **Protect return values** with unmodifiable wrappers.
14. **Simpler overloads delegate to complex ones.**
15. **Tests come first.** Write tests before implementation for
    all new behaviors and bug fixes. No exceptions.
16. **Test names describe behavior.** Every test method name
    explains what it verifies.
17. **Test method Javadoc is mandatory.** Every `@Test` method
    must document Goal, Start state, Workflow (as bullet
    points), and Expected state.
18. **Understand before changing.** Trace call paths and search
    for existing code before writing anything new.
19. **Do not fabricate.** Every API reference must be verified.
    Never guess at method names or class names.
20. **Run `./gradlew spotlessApply` after all code changes.**
    Never skip the formatter. Unformatted code fails CI.
21. **Opportunistic improvement.** Leave things better than you
    found them.
