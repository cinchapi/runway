# Project instructions

Instructions specific to the runway repo. The shared instructions in
`.agents/instructions/AGENTS.md` apply first. Rules here add to them and win on
conflict.

Keep this file to what an agent cannot learn from the shared guides or from the
code: how to build and test here, facts that differ by repo, where the project's
own docs live, and any shared rule this repo overrides.

## Build and quality gates

- Build with the Gradle wrapper: `./gradlew build | test | publish | clean`.
- `./gradlew build` and `./gradlew test` run the whole suite, and every test
  class boots a Concourse server. Do not run them locally.
- Compile check: `./gradlew compileJava compileTestJava`.
- Run one class: `./gradlew test --tests "SomeTest"`.
- Run `./gradlew spotlessApply` after every code change. CI does not run the
  format check, so nothing else catches unformatted code.
- The base version lives in `.version`. `version.sh` appends a build counter and
  a branch suffix: `develop` gets `-SNAPSHOT`, `release/*` gets `-rc`, `master`
  gets none, and any other branch gets its upper-cased name.
- Set a new base version with `./version.sh X.Y.Z`. It also patches the version
  in `README.md`.
- CI is CircleCI with timing-based test splits on one Java 8 image.
- There is no version catalog. Declare every dependency inline in
  `build.gradle`. The Concourse artifacts share `ext.concourseVersion`.

## Repo layout

- One module. Main code lives under `src/main/java/com/cinchapi/runway`, plus
  one I/O helper under `src/main/java/com/cinchapi/concourse`.
- Tests live under `src/test/java/com/cinchapi/runway`.
- `docs/dev/`: the project's developer docs.

## Facts that differ by repo

- Java 8. No `sourceCompatibility` is set; CI runs a Java 8 image.
- Integration branch: `develop`.
- Copyright header: Apache 2.0, from `spotless.java.license`.
- Tests extend `RunwayBaseClientServerTest`, which extends `ClientServerTest`
  from `concourse-ete-test-core` and shares one Concourse server per class.
  `AbstractRunwayTest` adds the shared record types. A test that runs against
  several Concourse versions extends `CrossVersionTest`.
- The tests boot the Concourse version in `Testing.CONCOURSE_VERSION`.
- Random test data: `com.cinchapi.concourse.util.Random`.
- Main code does no logging, and there is no facade.
- Changelog: `####` for a version, `#####` for a section under it.
- Labels: the GitHub repo defines no `type/*` or `area/*` labels. Ask before you
  label an issue.

## Where to look

- `README.md`, "Concurrent Writers": before code saves a `Record` another writer
  can change, or reads a value and then writes a decision that rests on it.
  Classify the operation and take the first rung that meets the goal:
  `save(true)`, `verifyOnSave`, the single-key atomics (`exchange`,
  `getAndUpdate`, `updateAndGet`), `db.save(a, b, c)`, or a transaction. Do not
  pick a rung by instinct.
- The `Record` and `Runway` Javadoc: the contract of each save, verify,
  `intern`, and transaction method. Verify a signature there before use.
- `docs/dev/restql.md`: before you write or change an HTTP endpoint. The private
  repo `https://github.com/cinchapi/restql` is canonical.

## Overrides

- None.
