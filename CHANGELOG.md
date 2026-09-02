# Changelog

#### Version 2.4.0 (TBD)
* **Added `@ExcludeFromSaveGraph` to stop a save from recursing into a linked `Record`.** By default, a save recursively saves every linked `Record` it can reach, so saving one record visits, and may write, the whole object graph behind it. Annotate a field with `@ExcludeFromSaveGraph` and a save writes that field's links but does not recurse into the records they point at. ([GH-210](https://github.com/cinchapi/runway/issues/210))
    * Those linked records are neither read nor written, so they are not checked for staleness and do not widen the save's conflict footprint. A `save(true)` no longer fails because another writer changed one of them, and a save no longer fails because another save was concurrently writing one of them.
    * A save no longer persists a `Record` it reaches only through an annotated field. The caller must save that `Record` itself; otherwise the stored link becomes a stale reference, which a later load handles under the governing `ReferenceNotFoundPolicy`.
    * The annotation applies to the field, not to the records it points at. A save that reaches the same `Record` through an unannotated field still saves it there.
    * This annotation has no effect on how loading, `@CascadeDelete`, `@JoinDelete`, `@CaptureDelete`, reference repair, and the binding that scopes a `Record` to a `Runway` or `Transaction` behave.

#### Version 2.3.0 (September 1, 2026)
Runway 2.3.0 makes concurrent work safe. A new Transaction API scopes any combination of reads and writes to one ACID transaction. Saves now write only what changed, and a save can verify the values a decision rests on. Atomic operations resolve records by their unique identity, and every `Audience`, including the anonymous one, is now a full database participant.

##### Transaction API
Runway previously offered no way to guarantee atomicity or full ACID compliance across an ad hoc combination of reads and writes. Each save committed atomically, but a decision made on loaded data could not be guaranteed to still hold when it was written. The Transaction API provides that guarantee. It opens a window to the full power of Concourse transactions, including serializable isolation and atomic multi-operation commits, without a raw Concourse connection.

* **`Runway#startTransaction` starts a `Transaction`.** `startTransaction()` returns a `DatabaseInterface` view that scopes every read and write to a single ACID transaction.
    * Reads observe the transaction's isolated snapshot, including its own uncommitted writes; no reader outside the transaction can observe a staged write before the commit.
    * Reads join the transaction's conflict footprint, so a commit fails instead of persisting a decision that was made on data a concurrent writer changed.
* **A transaction owns its records exclusively.** Every `Record` loaded or created through the view is bound to the transaction, along with the records linked from it. `save()` stages within the transaction, and the writes become durable only when `commit()` succeeds.
    * While the transaction is open, a bound record can only be read, written or deleted through it. Any other path to the record (a direct save, a save or deletion that reaches it through another record, or `Record#assign`) is refused with an `IllegalStateException`.
    * The single-key atomic operations (`exchange`, `getAndUpdate` and `updateAndGet`) on a bound record resolve within the transaction. The value the operation reads joins the conflict footprint, the write stages, and both become durable only when the commit succeeds. After the transaction ends, the operations resume against the enclosing `Runway`.
        * `getAndUpdate` and `updateAndGet` re-read the current value through the transaction and apply the update once, without a retry loop.
        * `exchange` answers against the snapshot, so a `true` holds only if the transaction commits.
        * A conflict with a concurrent writer surfaces as a `TransactionException` at the operation or fails the commit.
        * After an abort, a record that the database holds keeps the replacement in memory without treating it as an unsaved change. `getAndUpdate` and `updateAndGet` self-correct on their next call, and a later save writes only the caller's own edits.
    * Access-controlled operations and lazy `DeferredReference` loads on a bound record resolve within the transaction, so they observe the snapshot and join the conflict footprint.
    * A record that is not bound to the transaction operates against its own scope, even while the transaction is open.
* **`create` constructs a `Record` bound to the creating scope.** The new `DatabaseInterface#create` returns a `Record` whose direct save persists within the scope that created it: a `Runway`, a `Transaction` or an `Audience`. The created `Record`, and every `Record` reachable from its constructor arguments, is bound to that scope. Creation is an optional operation; an implementation that cannot support it refuses with an `UnsupportedOperationException`.
* **`Transaction` is `AutoCloseable` and thread-confined.** `close()` aborts whatever was not committed, so a try-with-resources block guarantees a clean end. Only the thread that starts a transaction may use it.
    * An abort discards every staged write; a record's in-memory edits remain, so the caller can retry.
    * Save and delete notifications fire only after a successful commit.
    * After a transaction ends, the view and its bound records operate against the enclosing `Runway` again; only another `commit()` and new hook registrations are refused.
* **A failed save poisons the transaction.** A save that throws after it begins writing can never commit. Every subsequent operation is refused except `abort()` (or `close()`) and `afterAbort` registration, so a partial save never becomes durable.
    * A save that refuses data throws `SuppressedRunwayException`, which carries the refusal. It cannot report the refusal by returning `false`, the way a `Runway`-bound save does, because what it staged cannot be selectively undone. Every other failure propagates as itself.
    * A save that is refused before it begins (an invalid argument) leaves the transaction usable.
* **A deletion is final within a transaction.** Once a save deletes a record, no later save in the same transaction can bring it back, references to it are removed at commit, and its delete notification fires once.
* **`Runway#transact` and `Runway#transactAndSupply` execute work within a managed transaction.** The work receives a `TransactionInterface`: `transact` for work with no result, and `transactAndSupply` for work that returns one. The commit happens after the work completes.
    * The view withholds `commit`, `abort` and `close`, so the work cannot end the transaction it joins.
    * Conflicts retry within the bounds of the governing `AtomicRetryPolicy`, so the work must be free of side effects outside of the transaction; `RetryExhaustedException` carries the final conflict as its cause.
* **`Record#transact` and `Record#transactAndSupply` execute work in the record's scope.** These methods let a `Record` perform an atomic combination of reads and writes. Within an open transaction the work joins it; otherwise, the work runs in its own managed transaction, the same as `Runway#transact` and `Runway#transactAndSupply`.
* **An `Audience` starts and scopes transactions.** `Audience#startTransaction` returns an open `Transaction` that behaves the same as the audience, just within the confines of the transaction: reads observe the audience's visibility and writes require its permissions.
    * The caller owns the transaction's lifecycle; after the transaction ends, the audience operates against the enclosing `Runway` again.
    * If the audience later joins a different `Transaction`, then a database operation on a view from the earlier transaction is refused with an `IllegalStateException` instead of following the audience into the new scope.
    * `Audience#transact` and `Audience#transactAndSupply` execute work in the audience's transactional scope, the same as `Record#transact` and `Record#transactAndSupply`; the work receives a view with the same audience behavior.
* **The `Transactional` interface names a construct that can start and scope transactions.** `Runway`, `Audience`, and every transaction view implement it.
    * A `Transaction` is itself `Transactional`: `transact` and `transactAndSupply` on an open `Transaction` join it, and `startTransaction` within an open `Transaction` is refused because transactions do not nest. After the transaction ends, both fall through to the enclosing `Runway`.
* **`Transaction#afterCommit` and `Transaction#afterAbort` schedule outcome-dependent side effects.** An `afterCommit` hook runs once, only after the transaction successfully commits, and never for an attempt that a conflict retry discards. An `afterAbort` hook runs when the transaction ends without a successful commit. Hooks run synchronously in registration order, and a hook that throws does not change the transaction's outcome.

##### Saves and Concurrent Writers
Three changes govern how a save interacts with concurrent writers. A save now writes only what changed, the stale-write check covers exactly what a save writes, and a save can verify the values a decision rests on. A `Transaction` remains the tool for a decision that spans records.

* **Breaking change: by default, a save now writes only what changed.** Every `Record` tracks its changes granularly. A save writes precisely the values the instance added, changed, or removed since it last loaded or saved. Previously, a save wrote the record's entire state, so a save from an instance with a stale view erased changes that other writers committed after the instance loaded. Now those changes survive. Declare the new `@MergeStrategy(OVERWRITE)` annotation on a field to opt that field into the legacy behavior: whenever the record saves, the field writes its full current state and overwrites concurrent changes. ([GH-163](https://github.com/cinchapi/runway/issues/163))
    * This primarily changes the semantics of collections. A save merges the instance's added and removed elements into the stored collection instead of replacing it. When other writers change the stored collection concurrently, storage is not guaranteed to exactly match the in-memory collection after a save. A mutation with no serialized effect, such as reordering a `List` (the database stores an unordered set of values), is no longer an unsaved change: a save of it writes nothing and fires no save notification.
* **Breaking change: `preventStaleWrites` now checks only the data a save could
  overwrite.** Changes to records or values that the save does not touch no
  longer cause a conflict. Previously, any change in the saved object graph
  caused a conflict. This prevented two callers from using the flag while
  updating different fields on the same record.
  ([GH-179](https://github.com/cinchapi/runway/issues/179))
    * A collection is checked element by element. A concurrent change to an
      element that the save neither adds nor removes does not cause a
      conflict, so two callers can add different elements to one collection.
    * Deletion remains stricter. Deleting a record removes all of its data, so
      any concurrent change to that record causes a conflict.
    * Changing a value and then restoring its loaded value does not cause a
      conflict because the save would not overwrite anything.
    * The flag checks writes, not reads. If a write depends on a value read
      earlier, declare that value with `verifyOnSave`, or use a `Transaction`
      when the value belongs to another record.
* **A `Record` can declare the values a save must verify.** Call `verifyOnSave` with the names of the fields a decision rests on. The next save of that `Record` then fails with a `StaleDataException` when the database no longer holds what the `Record` last saw for one of them. A decision that rests on another `Record` still calls for a `Transaction`, whose reads join its conflict footprint.
    * A value stored alongside the ones the `Record` saw is a difference, so an element another writer added to a declared collection fails the save.
    * The verification applies whether or not the save prevents stale writes.
    * The declaration lasts until a save commits, so each decision declares its own. A save that does not commit leaves the declaration in place.
    * A name that does not identify a stored field of the `Record` is refused.
    * Within a `Transaction`, a `Record` that the `Transaction` loaded needs no declaration, because the `Transaction` fails its own commit when a writer changes anything it read. Any other `Record` carries its declaration into the save, and a writer that moves a declared value fails that save or the commit. A save is refused when it carries a declaration on a `Record` that was last loaded or saved outside the `Transaction` after it began; load that `Record` through the `Transaction` instead.

##### Atomic Operations
A record's identity is its data under its `@Unique` constraints. This release lets a constraint scope that identity across the class hierarchy, and it adds `intern`, which resolves a record by its identity, atomically.

* **A `@Unique` constraint can scope its identity across the class hierarchy.** Declare `@Unique(any = true)`, and the declaring class and every descendant share one identity space, in the same sense that `findAnyUnique` matches across them. A save that duplicates the identity anywhere in that subtree fails enforcement. The scope lives on the declaration, so no caller passes a class that can drift. The default is unchanged: a constraint without `any = true` applies among records of the same concrete class. ([GH-171](https://github.com/cinchapi/runway/issues/171))
    * A named compound constraint declares one `any` for all of its members. A hierarchy-scoped group also declares all of its members in one class. A group that violates either rule is rejected as a misdeclaration.
* **`intern` atomically returns the one record that shares a record's unique identity, or saves the given record when none exists.** A caller can construct a record and canonicalize it in one call, with no hand-built criteria that can drift from the constraints. A separate find-then-create sequence can lose a race with a concurrent caller; the single call cannot.
    * On `Runway`, the lookup and the save commit as one transaction, independent of any transaction the caller holds open. Concurrent callers for the same identity converge on a single record, and conflicts retry within the bounds of the governing `AtomicRetryPolicy`. On a `TransactionInterface`, the lookup and the save stage within that transaction.
    * `Record#intern()` interns the record in its own transactional scope: if the record is bound to an open transaction, then the lookup and the save stage within it; otherwise, they commit as one transaction against the record's `Runway`.
    * `Record#intern()` is an identity operation, not an audience-mediated action. It canonicalizes the record itself, so no access checks apply, even when the record is an `Audience`. `Audience#intern` is the audience-mediated form. It enforces the audience's permissions and visibility on whatever record it interns, including the audience itself.
    * A record shares the identity only if it agrees with every `@Unique` constraint; a `null` value does not participate. An existing record that shares some but not all of the identity is not a match. The save then fails `@Unique` enforcement rather than adopt that record silently. Within a caller-owned transaction, that failed save poisons the transaction, so the staged writes can never commit.
    * Each `@Unique` constraint is matched within its declared scope, so a constraint with `any = true` looks across the declaring class's hierarchy. `intern` adopts only a record that shares the given record's concrete class. An identity that a record of another class claims is never adopted; the save fails `@Unique` enforcement, which surfaces the conflict.
    * A record with no non-null value under any `@Unique` constraint is rejected with `IllegalArgumentException`.
    * `DuplicateEntryException` propagates when more than one record matches a single constraint within its scope, consistent with `findUnique`.
* **An `Audience` performs `intern` and the atomic `find*AndUpdate` methods under its access rules.**
    * `intern` requires permission to create the record, even when an existing record already claims the identity. It refuses an existing match that is not visible to the `Audience`. The refusal of a hidden match still confirms that a record with the identity exists.
    * The rules apply to whatever record the `Audience` interns, including itself; `Record#intern()` remains the unmediated identity operation.
    * The `Audience` is recorded as the author of a record that `intern` saves.
    * A `find*AndUpdate` call matches among the records that are visible to the `Audience`, so "first" and "unique" are evaluated over the visible matches.
    * The call updates the match only if the key is writable by the `Audience`; otherwise, it returns `null` and changes nothing.
    * The field eligibility and replacement rules are the same as on `Runway`.
    * The lookup, the access checks and the update run in the `Audience`'s transactional scope. Within an open transaction, they stage and commit with it. Otherwise, they commit together in their own transaction, and conflicts retry within the bounds of the governing `AtomicRetryPolicy`.

##### Stale Reference Handling
In cases where Runway encountered a stale reference (e.g., a record with no stored data because it was previously deleted or because there were destructive modifications outside of Runway), it previously behaved inconsistently. For example, a stale reference in a collection was dropped and quietly deleted from the database, while a scalar field with a stale reference either did the same thing or caused an entire load operation to fail, depending on the server.

This release makes stale reference handling consistent and lets users configure the desired behavior globally or per field.

* **By default, a stale reference is skipped** (`ReferenceNotFoundPolicy.SKIP`) so that a housing scalar field receives a `null` value and a housing collection does not consider the reference at all. The stale reference still exists in the database, but Runway will not encounter load errors because of it. The consequences of this default are that users must account for `NullPointerException`s in cases where a `null` value is hydrated unexpectedly, and that a load no longer cleans up stale references on its own.
* **Declare `@ReferenceNotFound` on a field to choose a different policy.** `ReferenceNotFoundPolicy.REPAIR` skips the stale reference and also deletes it from the database, so the housing record stops carrying it. `ReferenceNotFoundPolicy.ERROR` fails the load of the housing record and throws a `ReferenceNotFoundException` naming that record, the field, and the stale reference. Use `ERROR` for a reference the record cannot be correct without.
* **A policy applies when a reference loads.** For a `DeferredReference` that is `get()` rather than the load of the record that holds it, so `get()` returns `null` under `SKIP`, deletes the stale reference and returns `null` under `REPAIR`, and throws `ReferenceNotFoundException` under `ERROR`. Previously, `get()` on a stale reference threw a `NullPointerException` from inside Runway.
* **`Runway.builder().referenceNotFoundPolicy` sets the policy for every field that declares none.** Set it to `REPAIR` to keep the cleanup that a load previously performed on its own.

##### Audiences
* **Every `Audience` performs database operations.** Previously, only an `Audience` that is a `Record` could; every database operation on the anonymous `Audience` failed. Now an anonymous audience holds the database it operates against and mediates the same operations, with the same visibility and permission rules as a `Record`-based audience, including the transactional operations.
    * `Audience.anonymous()` resolves against the single open `Runway`, and is also available as `Runway#anonymous()`. The new `Audience.anonymous(DatabaseInterface)` names the database explicitly, including a `Transaction`.
    * An anonymous `Audience` that resolves against zero or multiple open `Runway` instances names no database. It still answers access policy questions, such as `$checkIfVisible`, but refuses every database operation with an `IllegalStateException`.
    * Anonymous audiences are equal to one another regardless of the database each holds. Compare with `equals` or the new `Audience#isAnonymous()` instead of identity.
* **`Audience#create` binds the created `Record` to the audience's database context.** The `Record`, and every `Record` reachable from its constructor arguments, saves within that context. Previously, `Audience#create` checked permission and attributed authorship, but bound the record to nothing.
    * A mediated `create` or `intern` leaves the caller's records bound as they were unless it saves the record. A `create` that throws restores the binding of every record reachable from the constructor arguments. An `intern` that does not save the record restores the record and every record reachable from it. If a save failure poisons a `Transaction` that stays open, then the transaction's failed-save contract governs instead.

##### API Breaks and Deprecations
* **Breaking change: a delete listener receives a deleted record's identity
  and stored state instead of a `Record`.** `Runway.Builder#onDelete` and
  `Runway.Properties#onDelete` now take a
  `TriConsumer<Long, Class<? extends Record>, Map<String, Set<Object>>>`,
  which receives the deleted record's id, its type, and the values it held
  when the save deleted it. A deleted record no longer exists, so a live
  object invited callers to act on something that could not be read, saved
  or linked.
    * Replace a listener that reads fields off the record with one that reads
      them out of the data map, keyed by field name.
      ```java
      runway = Runway.builder()
              .onDelete(Order.class, (id, clazz, data) -> {
                  Set<Object> status = data.get("status");
                  audit.record(id, status);
              })
              .build();
      ```
    * The data is the state the database stored, so a record that the save
      itself changed reports its stored values, not the unsaved edits the
      caller's instance held.
    * A save reads a deleted record's stored state only when a delete
      listener is registered for the record's class or a superclass, so a
      deletion that no listener receives pays nothing for the data a
      notification would carry.
    * When the read happens, it shares a server round trip that the save
      already makes, so it never adds one of its own.
    * A listener registered after a save already staged its deletions
      receives an empty data map for that save's deletions, never `null`.
    * A listener registered for a type still receives only records of that
      type or a subclass, and a listener that throws still does not block the
      remaining listeners.
* **Removed unused members from the `com.cinchapi.runway.db` interfaces.**
  `Saver#find(Criteria, Consumer)`,
  `Saver#reconcile(String, long, Collection)`, `Reader#select(String, long)`,
  `Reader#get(String, long)` and `Reader#concourse()` are removed, along with
  their implementations. No Runway operation called them.
    * `AbstractReader#concourse()` remains for subclasses, but it is no longer
      public.
    * The varargs `Saver#reconcile(String, long, Object...)` and the
      criteria-based reads are unchanged.

##### Bug Fixes
* Fixed a bug where a unique query through an `Audience` whose visibility `Scope` denies all access failed with a `ClassCastException` instead of returning `null`. `findUnique` and `findAnyUnique` now return `null` under such a `Scope`, consistent with their contract when no record matches.
* Fixed a bug where a save of a record that another writer deleted restored that record instead of failing. The record reappeared in queries over its class, and a later load of it could fail because the values it requires were gone. A save that would write anything into a record that holds no data now throws the new `DeletedRecordException`, which names the record, and writes nothing.
    * The refusal reaches the caller ahead of a stale-write failure when both apply to the same record.
    * A realm change and a stored author attribution are writes, so a save whose only change is realm membership, or attribution through an `Audience` that a save stores, is refused the same way.
    * A record that has never been saved is unaffected. A save with nothing to write still succeeds. Deleting a record that another writer already deleted still succeeds.
    * Inside a transaction, the refusal poisons the transaction, so none of its staged writes can commit.
    * Against a server that supports bulk commands, the existence check adds one server round trip per save operation, and none when the save has nothing to write or already performs a uniqueness or stale-data read.
* Fixed a bug where a `write` performed through an `Audience` could modify a record that a visibility `Scope` hid from that `Audience`. The write succeeded when the caller held the record and its field rules permitted the keys. A write through an `Audience` now also requires the record to be visible.
* Fixed a bug where a save that processed multiple in-memory copies of the same record could deliver the save notification to a copy that changed nothing and treat that copy as current. The combined saves of one transaction could do the same. A later `preventStaleWrites` save of the stale copy then silently overwrote the changes that actually persisted. Now the notification goes to the copy whose changes persisted, and the stale copy fails the stale-write check.
* Fixed a bug where a save of a record with no unsaved changes traversed its transient fields and saved modified records that were referenced only through them. A transient field is outside a record's persistent data, so a record referenced only through one no longer saves with its holder.
* Fixed a bug where an exception thrown by an `overrideSave` accessor, or a `null` record argument, was recorded as an ordinary save failure and returned `false`. That disguised a programming error as a data rejection. Now both propagate from the save as exceptions.
* Fixed a bug where `AccessControl#authorize` refused an anonymous `Audience` that was permitted to create the record, so `authorize` could disagree with creation through an `Audience`. `authorize` now applies the same creation rules that govern creation through an `Audience`.
* Fixed a bug where a save that enforced a `@Unique` constraint conflicted with concurrent writes to unrelated records of the same class, including the creation of a record that holds none of the constrained values. The conflict failed the commit of a `Transaction` that performed the save, and it made a managed save retry. Such a save now conflicts only with a writer of the constrained values. ([GH-176](https://github.com/cinchapi/runway/issues/176))
* Fixed a bug where a save attributed a revision to an `AdHocRecord`. An `AdHocRecord` is never stored, so the attribution named a record that does not exist and no reader could resolve it. Such a save now records no author.
* Fixed a bug where an `Audience#frame` that filtered any data caused a later, fully permitted `read` on the same thread to fail with a `RestrictedAccessException`. A `frame` now has no effect on later calls, as its contract promises. In a server that pools threads, the stale failure could cross requests.
* Fixed a bug where the single-key `Audience#read` (and `readAs`) returned `null` for a key the `Audience` may not read, instead of the `RestrictedAccessException` that its contract documents. The single-key form now behaves the same as the collection-based `read`. A `read` of a record that the `Audience` cannot discover at all is now also refused with a `RestrictedAccessException`; previously the outcome depended on the earlier calls on the thread.
* Fixed a bug where an `Audience#frame` that threw partway through its walk poisoned every later `frame` on the same thread. The records in flight at the failure rendered as `(recursive link)` placeholders instead of nested data, and the result still looked successful. In a server that pools threads, one failure degraded every later response the thread rendered. A `frame` that throws now leaves no residue, and the placeholder appears only for a genuine cycle within a single `frame`. ([GH-206](https://github.com/cinchapi/runway/issues/206))
* Fixed a bug where a save that already committed reported failure. The save returned `false`, and every record it processed reverted to its pre-save state in memory, so the caller could not learn that the database held the changes. The bug occurred when a save deleted a record that another record in the same save referenced through a `@CaptureDelete` collection field whose declared type cannot be created without constructor arguments.
    * Within a `Transaction`, the failure still reaches the caller from `commit()`, and `committed()` reports `true`.
    * A failure in one record's dispatch does not block the notifications and checkpoints of the records that follow it.

#### Version 2.2.0 (August 4, 2026)
* **Added `DynamicWritePolicy` to govern which fields `Record#set` can write.** By default, a dynamic write can reach any field, including final, private, package-private and protected ones, which preserves the historical behavior. Configure a policy per `Runway` instance with `Runway.builder().dynamicWritePolicy(...)`. ([GH-147](https://github.com/cinchapi/runway/issues/147))
    * `DynamicWritePolicy.javaDefaults()` returns a policy that respects Java modifiers, so only public non-final fields accept dynamic writes. `DynamicWritePolicy.builder()` composes a policy that selectively allows final, private, package-private or protected fields.
    * When the governing policy refuses a write, `Record#set` throws `NonWritableFieldException` instead of writing the field or storing the value as a dynamic attribute. The policy governs every field a key can name, including the internal framework state that tracks a record's identity and metadata.
    * The bulk `Record#set(Map)` overload applies entries individually, so a mid-map refusal leaves the earlier entries applied. The caller must catch the exception and discard the record instead of saving it.
    * A field annotated with the new `@Writable` annotation always accepts dynamic writes, regardless of the policy.
    * Records that are not assigned to a `Runway` instance follow the permissive default, and loading a record from the database always populates every field regardless of policy.
    * The governing policy is readable through `properties().dynamicWritePolicy()`.
* **Added `findFirst` and `findAnyFirst`** to read the single first record that matches a `Criteria` under a caller-supplied `Order`, or `null` when no record matches. The database evaluates the order and returns at most one record, so the full match set is never loaded. ([GH-139](https://github.com/cinchapi/runway/issues/139))
    * Unlike `findUnique`, the methods perform no duplicate detection and never throw when more than one record matches; the result is the first record under the order.
    * `findAnyFirst` applies the same contract across the class hierarchy, as `findAny` does for `find`.
    * Overloads accept `Realms` scoping and a client-side `Predicate` filter; with a filter, the result is the first record that both matches the `Criteria` and passes the filter.
    * The `Selection` fluent API expresses the same read: the `first()` terminal becomes available once a sort order is set, and the resulting selection participates in batched `select` calls.
* **Added an `onDelete` listener API** to `Runway.Builder` and `Runway.Properties` for reacting to record deletions. A delete listener is called after a save deletes a record, whether the record was explicitly marked with `deleteOnSave()` or was pulled into the deletion through annotations like `@CascadeDelete` and `@JoinDelete`. Delete listeners mirror save listeners: they are typed, compositional, asynchronous, and exception-suppressing. ([GH-65](https://github.com/cinchapi/runway/issues/65))
* **Behavior change:** a save that deletes a record now fires delete listeners instead of save listeners. Previously, save listeners reported the deleted record as if it had been written. ([GH-65](https://github.com/cinchapi/runway/issues/65))
* **Behavior change:** a record that is updated during `@CaptureDelete` cleanup (because a record it references was deleted) now fires save listeners. Previously, that update was not reported to any listener. ([GH-65](https://github.com/cinchapi/runway/issues/65))
* Fixed a bug where a save that deleted a record reverted the unsaved changes of another record in the same save call when that record referenced the deleted record through a `@CaptureDelete` field.
* Fixed a bug where the argument order of a bulk save determined the outcome when one call both deleted a record and saved a modified record connected to that deletion. A deletion is now final within a save: a deleted record cannot be re-created by staged changes elsewhere in the same call, and a reference removed by `@CaptureDelete` cleanup cannot be restored by the same call. ([GH-157](https://github.com/cinchapi/runway/issues/157))
* Fixed a bug where a record passed to a bulk save fired no save listener when the same save had already written it as the linked reference of an earlier record.
* Fixed a bug where a failed save could silently discard a record's unsaved changes. After the failure, the record reported no unsaved changes even though nothing committed, so a later save skipped its fields. The bug occurred when the same save call deleted a record that the affected record referenced through a `@CaptureDelete` field.
* Fixed a bug where a failed save left a deletion mark on every record that annotations like `@CascadeDelete` and `@JoinDelete` pulled into the attempted deletion. A later save that reached such a record deleted it instead of persisting its data.
* Fixed a bug where a delete failed when the deleted record was the only element of another record's `@CaptureDelete` array field.
* Fixed a bug where a deletion hook annotation on an array field was ignored. A record that linked to the deleted record only through an array `@JoinDelete` field did not join the deletion. A record that linked only through an array `@CaptureDelete` field kept its stored reference unless it was part of the same save.
* Fixed a bug where a record failed to load when one of its array fields held no stored values.
* Fixed a bug where a listener that threw an `Error` stopped all later save and delete notifications. A listener failure of any kind is now suppressed, and the remaining listeners and later notifications still fire.
* Fixed a bug where a save that deleted a record did not remove a surviving record's in-memory reference to it. A later save of the survivor re-created the stored `@CaptureDelete` link to the deleted record. After a committed save, a surviving record's in-memory references and its unsaved-changes status now match its stored data.
* Fixed a bug where a later save of a record that a deletion pulled in through annotations like `@CascadeDelete` and `@JoinDelete` re-created the deleted record. When an instance of the record was part of the save that performed the deletion, a later save of that instance now repeats the deletion instead of persisting its data.
* Fixed a bug where a save that failed because a record's overridden save refused it left the other records in the call reporting no unsaved changes, so a later save skipped their fields.
##### Atomic Operations
This release adds atomic read-modify-write operations that conditionally replace one field's value in a single atomic step, without a raw Concourse connection. They come in two forms: single-key operations on `Record` for a record the caller already holds (`exchange`, `getAndUpdate`, and `updateAndGet`), and find-and-update methods on `Runway` that atomically select the record and update it (`findUniqueAndUpdate` and `findFirstAndUpdate`, with `findAnyUniqueAndUpdate` and `findAnyFirstAndUpdate` hierarchy variants).

* `Record#exchange(String, Object)` is the conditional counterpart to `Record#set(String, Object)`: `set` stages a value that takes effect on the next save, whereas `exchange` writes through to the database immediately, and only if the database still holds the record's in-memory value for the key. On success, the in-memory field updates to match; on failure, nothing changes anywhere. A record that is staged for deletion is refused, so an exchange never persists a value that the pending save immediately deletes.
* `Record#getAndUpdate(String, UnaryOperator)` and `Record#updateAndGet(String, UnaryOperator)` apply a function to the current value and persist the result with the same conditional semantics. When a concurrent writer wins the race, the current stored value for the key is re-read and the function re-applies to it, so the function may run more than once and must be safe to do so. `getAndUpdate` returns the replaced value; `updateAndGet` returns the produced one.
* When the update function returns its input unchanged, nothing is written; the read is verified as current instead, so a no-op decision is never made against stale data.
* `getAndUpdate` and `updateAndGet` refuse a record that has unsaved changes, including a record that was never saved, and a record that is staged for deletion. A retry overwrites the record's in-memory value for the key, and the refusal prevents a silent discard of a staged value.
* A retry touches only the key under update. Staged realm changes and loaded linked records survive a retry untouched, and if the stored value is concurrently removed, the retry re-applies the function to `null`. After a retry, an `audit` read on the record includes the concurrent revisions the retry observed.
* `Runway#findUniqueAndUpdate(Class, Criteria, String, UnaryOperator)` finds the one record that matches a `Criteria` and applies the operator to the value of the key; the find and the write commit as one transaction, so concurrent callers contending for the same record are mutually excluded. It returns the updated record, or `null` when nothing matches (the operator never runs), and throws `DuplicateEntryException` when more than one record matches, without updating or committing. ([GH-140](https://github.com/cinchapi/runway/issues/140))
* `Runway#findFirstAndUpdate(Class, Criteria, Order, String, UnaryOperator)` applies the same one-transaction contract to the first match under a required `Order`, which makes it the primitive for atomically claiming the next available record. The operator receives `null` when the field has no value, so an unset lock field can be claimed. The `findAnyUniqueAndUpdate` and `findAnyFirstAndUpdate` variants apply the same contracts across the class hierarchy. ([GH-140](https://github.com/cinchapi/runway/issues/140))
* On a write conflict, a find-and-update retries the whole cycle (re-find, re-apply, re-write) against fresh state, so the operator may run more than once and must be safe to do so. Any `Criteria` is supported, including one that references derived or computed data, and records supplied by an attached `AdHocDataSource` are never matched.
* Only an intrinsic single-value field whose type is a Java primitive or its boxed form, a `String`, a `Timestamp`, an enum, or a `Tag` is eligible, and the replacement must be a non-null instance of the field's type. A field with no current value is eligible: the update function receives `null`, and a conditional write that expects absence succeeds only if the record exists in the database and the field still has no value when it commits. `@Unique` fields are refused because uniqueness cannot be enforced within a single-key operation. Link-typed fields (a `Record` or `DeferredReference` value) and every other type, including serialized objects, are refused because a single-key operation cannot cover their state faithfully. `@Required` and `@ValidatedBy` constraints are enforced on the replacement before anything is written. The `DynamicWritePolicy` that governs `Record#set` also governs these operations, so a field the policy refuses throws `NonWritableFieldException`.
* These operations are targeted writes, not saves: the `beforeSave` hook and save listeners do not run, and a successful write counts as a modification for stale-write detection.
* **`AtomicRetryPolicy`** governs how the atomic operations respond to contention. Configure the retry limit and the jittered backoff pacing per `Runway` instance with `Runway.builder().atomicRetryPolicy(AtomicRetryPolicy.create(limit, backoffMillis))`. The configured policy is readable via `runway.properties().atomicRetryPolicy()`. When the retries are exhausted, the operation throws the new `RetryExhaustedException` instead of returning a non-committed result.

#### Version 2.1.1 (July 30, 2026)
* Fixed a bug where loading a record by id through an abstract class with no scanned descendants (most importantly, the `Record` base class itself via `load(Record.class, id)`) threw `InstantiationException` instead of resolving the record's concrete class from its stored section. ([GH-145](https://github.com/cinchapi/runway/issues/145))
* Loading a record by id through an abstract class now returns `null` when the id does not name a Runway record (its data has no class section), consistent with how invalid records are indistinguishable from invisible ones. ([GH-145](https://github.com/cinchapi/runway/issues/145))

#### Version 2.1.0 (May 22, 2026)
* Fixed a bug where loading an access-controlled `Record` through an `Audience` (e.g., `Audience#load(Class, long)`) threw `UnsupportedOperationException` when the class's registered visibility `Scope` used a scoped navigation criteria (`Criteria.where().scope(prefix, inner)`). ([GH-125](https://github.com/cinchapi/runway/issues/125))
* **Breaking change:** `@Computed` properties are no longer materialized by `Record#map()`, `Record#json()`, or `Audience#frame()` unless the caller positively names them. The `@Computed` annotation has always promised that "computed data is generally expensive to generate and should only be calculated when explicitly requested" &mdash; but the historical default materialized every `@Computed` property on any bare serialization call, eagerly invoking suppliers the caller never asked for. That behavior contradicted the annotation's contract and made `@Computed` operationally indistinguishable from `@Derived`. This release corrects the longstanding bug by aligning the default with the documented contract. ([GH-128](https://github.com/cinchapi/runway/issues/128))
    * **Mitigations for callers depending on the legacy behavior:**
        * If the property should always be eagerly materialized, change the annotation from `@Computed` to `@Derived` &mdash; this is the correct annotation for properties that are part of the default serialized representation.
        * If the property must remain `@Computed` but the call site needs the legacy materialization, pass `SerializationOptions.builder().includeComputedValuesByDefault(true).build()` to `map(opts)`, `json(opts)`, or `frame(opts, keys, subject)`.
        * Positively naming the `@Computed` key in the keys list (e.g., `record.map("propertyName")`) always fires its supplier regardless of the flag.
    * `Audience#frame` has a new overload, `frame(SerializationOptions, Collection<String>, Record)`, that threads options through every level of the framing pipeline, including recursive frames of linked `AccessControl` records. The existing `frame(Collection<String>, Record)` and `frame(Record)` overloads delegate with `SerializationOptions.defaults()` &mdash; which under the new default now exclude `@Computed` properties.
* Fixed a bug where `Record#refresh()` left memoized `@Computed` values cached from before the refresh, so subsequent reads returned stale results instead of recomputing against the refreshed state. ([GH-93](https://github.com/cinchapi/runway/issues/93))
* **Added an additive `+` key prefix to `Record#map`, `Record#json`, and `Audience#frame`** for layering a single key onto the default payload &mdash; the escape hatch for naming one `@Computed` property without enumerating every other key (e.g., `record.map("+computedProperty")` returns the defaults plus the computed value). The moment any bare positive key appears in the call, defaults are dropped and only the listed keys (bare or `+`-prefixed) appear in the result; `-` continues to exclude. Legacy call shapes are unchanged. ([GH-133](https://github.com/cinchapi/runway/issues/133))
* Fixed a bug where `Audience#frame` silently dropped a `-`-prefixed key against any restricted (non-`ALL_KEYS`) readable set, collapsing the call to an empty map instead of returning the readable defaults minus the excluded key. ([GH-133](https://github.com/cinchapi/runway/issues/133))
* Fixed a bug where `Audience#frame` serialized a `Record`-valued key (or a collection of `Record` values) differently depending on how it was requested. An explicitly named key returned a bare reference to the record; the same key, when included implicitly by default, returned the fully nested object. Both now return the nested object.

#### Version 2.0.0 (May 20, 2026)

##### Command API
This release adopts the Concourse Command API (`prepare()`/`submit()`), introduced in Concourse 1.0.0, to batch Runway's hottest read and write paths into the fewest possible server round trips. When the connected server is older than 1.0.0, Runway transparently falls back to the legacy per-call path.

* **Batched multi-selection reads**: `Runway.select(Selection...)` now collapses an N-selection call into a single `prepare()`/`submit()` round trip, regardless of whether the selections target the same or different classes. Replaces the prior combinable/isolated dispatch that issued one round trip per isolated selection plus one for the OR-merged combinable batch. ([GH-103](https://github.com/cinchapi/runway/issues/103))
* **Batched saves**: `Runway.save(Record...)` and `Runway.save(boolean, Record...)` now drive every save &mdash; stage, stale-data audit, uniqueness validation, field writes, cascade-delete reads, and commit &mdash; through as few `prepare()`/`submit()` round trips as the save permits: `1` round trip when no validation reads are needed (no `@Unique` fields and `preventStaleWrites=false`); `2` round trips otherwise. For a record with `f` fields, `u` `@Unique` fields, and `preventStaleWrites=true`, the save cost drops from `~3 + u + f + 2` round trips to `2`. ([GH-104](https://github.com/cinchapi/runway/issues/104), [GH-105](https://github.com/cinchapi/runway/issues/105))

##### Transitive Navigation
Every `load`, `find`, and `findAny` operation now resolves linked `Record` data through a single, unified pre-fetch path built around Concourse's `navigate()` API plus the new `*` transitive modifier. The unified path covers every reachable destination in a single `navigate()` per class group (typed or untyped) and a follow-up bulk-`select()` cleanup pass that closes any gaps the navigate paths cannot reach &mdash; mutual-reference cycles whose field names alternate, dangling links, links into records reached through single-`Record` edges, etc. ([GH-80](https://github.com/cinchapi/runway/issues/80), [GH-98](https://github.com/cinchapi/runway/issues/98))

* Self-referential `Record` graphs of arbitrary depth pre-fetch in a single round trip via the `*` transitive modifier; previously, self-referential `Collection<Record>` fields (e.g., `Exchange.children: Set<Exchange>`) and self-referential single-`Record` fields (e.g., `Exchange.parent: Exchange`) bounded pre-fetching at one level.
* `Collection<Record>` fields reached through single-`Record` edges (e.g., `Document.metadata.tags`) and non-cyclic `Collection<Record>` fields nested under self-referential ones are now fully pre-fetched.
* Untyped loads through `loadAny` / `findAny` now also benefit from `navigate()` pre-fetch &mdash; the load pipeline groups discovered records by their section key and dispatches a class-aware `navigate()` per group. An untyped load that touches `K` classes therefore costs one batched round trip per group, so the table's `1` applies to typed loads and to untyped loads whose results all belong to a single class.
* The single-record `load(Class, id)` codepath is unified with the bulk pre-fetch path, so every load surface shares one mechanism.
* Select-side path computation is unchanged.

Transitive navigation also cuts the number of server round trips a load costs, and the saving grows with how deeply records are linked. Consider a record type whose instances form a tree. It has a self-referential collection field, `children: Set<Exchange>`, and a `parent` reference back up the tree. Each node also links to a few other record types, some of them subtypes of a shared polymorphic hierarchy. The depth of such a tree &mdash; the number of levels of children that exist at run time &mdash; is set by the data, not by the schema.

On the `1.14.x` line, running against Concourse `0.12.x`, pre-fetching could descend a self-referential link only one level per round trip. Loading the root of the tree above cost roughly one round trip for each level of depth: the first round trip fetched the root's direct children, the second fetched their children, and so on down to the leaves. Because that depth is a property of the data, the cost had no ceiling. The deeper the tree, the more round trips every load took.

On `2.0.0`, running against Concourse `1.0.x`, the new `*` transitive modifier removes the per-level cost. A navigate path written `children*` tells a single `navigate()` call to follow the `children` link repeatedly, to whatever depth the data reaches. That one call pre-fetches the whole tree, every level at once, along with the other record types each node links to. A graph that used to cost one round trip per level now costs one round trip in total, however deep it runs. The table below states the upper bound on round trips for two representative graph shapes.

| Linked-record prefetch | Non-cyclic graph (depth `L`) | Self-referential tree (depth `D`) |
| --- | --- | --- |
| `1.14.x` / `0.12.x` &mdash; `NONE`, no prefetch (the default) | `1 + N` | `1 + N` |
| `1.14.x` / `0.12.x` &mdash; `BULK_SELECT`, batched per level | `1 + L` | `1 + D` |
| `2.0.0` / `1.0.x` &mdash; Command API and `*` modifier | `1` | `1` |
| `2.0.0` / `0.12.x` &mdash; legacy fallback (no Command API or `*`) | `2` | `2 + D` |

Each cell is the largest number of server round trips needed to fully resolve a single `load` or `find`. The formulas use three quantities:

* `N` &mdash; the number of linked records the load reaches.
* `L` &mdash; the length of the longest chain of non-cyclic `Record`-to-`Record` links. This is fixed by the schema, so it stays small and constant.
* `D` &mdash; the depth of a self-referential tree. This is set by the data and has no fixed upper bound.

No pre-fetch mechanism in the `1.14.x` line could resolve a self-referential tree in a number of round trips that did not grow with `D`. The `2.0.0` mechanism is the first that can.

A `2.0.0` load has two independent optimization opportunities, both introduced in Concourse `1.0.0`:

1. **The Command API** (`prepare()`/`submit()`) lets Runway pack the per-load `select` for the record itself and the `navigate` for the records it links to into a single round trip rather than two.

2. **The `*` transitive modifier** to `navigate()` lets a single `navigate()` call follow a self-referential link to any depth. Runway emits `*` paths (e.g., `children*`) for every self-referential edge in the source graph.

Concourse `1.0.x` provides both features; older Concourse servers provide neither. The third row of the table is the both-on case; the fourth row is the both-off case.

Whichever paths a single `navigate()` cannot reach are gathered by a follow-up bulk-`select()` cleanup pass that walks the missing graph one round trip at a time. Two classes of edge always require the cleanup, regardless of server version: a cycle that alternates between two different field names (for example, a type `A` that links to `B` through a field named `bs` while `B` links back through a field named `as` &mdash; the `*` modifier can only repeat one field name), and a link whose target has been deleted. Against an older server, every self-referential edge also needs the cleanup, because the navigate-path set has had its `*` paths stripped.

The third row's `1` falls out of both optimizations being in effect: the Command API combines the select and the navigate into one round trip, and the `*` modifier makes that single navigate cover an arbitrarily deep self-referential tree, so the cleanup pass has nothing to walk for a pure tree. A graph with alternating-field cycles still costs one cleanup round trip per level of the part `*` could not express.

The fourth row's `2 + D` is the same calculation with both optimizations absent: two round trips for the separate select and navigate, plus one cleanup round trip per level of every self-referential edge (`+ D` for a tree of depth `D`). Non-cyclic graphs do not depend on `*` paths, so they pay only the Command-API cost: `2` round trips.

##### API Breaks and Deprecations
* Removed the `CachingConcourse` infrastructure and the related `Runway.Builder` cache configuration. `Runway.Builder.cache(Cache<Long, Record>)` and `Runway.Builder.withCache(Cache<Long, Map<String, Set<Object>>>)` have been deleted along with the `com.cinchapi.runway.cache` package (`CachingConcourse`, `CachingConnectionPool`, `LeasingCache`, `NoOpLeasingCache`, `NoOpCache`). Connection-level data caching is removed in this release; the planned `prepare()`/`submit()` write transport made the per-method invalidation model untenable, and the implementation had latent invalidation bugs (e.g., missing overrides for `consolidate`, `link`, `unlink`, and several `clear` / `insert` variants). Callers that relied on `withCache(...)` should remove the configuration; the thread-local reservation API (`Runway#reserve()` / `Runway#unreserve()`) is unaffected. ([GH-81](https://github.com/cinchapi/runway/issues/81))
* Removed the `ReadStrategy` enum and the `Runway.Builder.readStrategy(...)`, `Runway.Builder.streamingReadBufferSize(...)`, and `Runway.Builder.recordsPerSelectBufferSize(...)` configuration. Every read now fetches the matching records in bulk. The former streaming read strategy deferred each record's read until it was consumed, but with linked-`Record` pre-fetching now unconditional it produced the same fully-loaded `Record` graph as a bulk read, so it offered no behavior worth configuring. Callers that set a `ReadStrategy` or buffer size must remove those calls.
* A `@Unique` constraint violation detected during `Runway.save()` now surfaces as a `Record.ConstraintViolationException` (a subtype of `RunwayException`) rather than a `java.lang.IllegalStateException`. The violation still makes `save()` return `false` with the exception recorded on the offending `Record`, but callers that catch or type-check `IllegalStateException` to detect uniqueness failures must switch to catching `RunwayException`. The change affects every connected server version &mdash; both the new bulk-command save path and the legacy per-call path. ([GH-104](https://github.com/cinchapi/runway/issues/104))
* Removed the `CollectionPreSelectStrategy` enum and the `Runway.Builder.collectionPreSelectStrategy(...)`, `Runway.Builder.disablePreSelectLinkedRecords()`, and `Runway.Properties.collectionPreSelectStrategy()` configuration. Pre-fetching linked `Record` data is now unconditional &mdash; every load resolves linked records through the navigate-based path described above. The former `BULK_SELECT` and `NONE` strategies produced the same fully-loaded `Record` graph as `NAVIGATE`, only with more database round trips, so they offered no behavioral choice worth configuring. Callers that selected a strategy or called `disablePreSelectLinkedRecords()` must remove those calls; to make an individual linked field load only when accessed, wrap it in a `DeferredReference`.

##### Dependencies
* Upgraded the `concourse-driver-java` dependency to `1.0.1`, a major-version upgrade from the `0.12.x` line. Applications that pin a transitive Concourse version must update it to match.

#### Version 1.14.6 (May 1, 2026)
* Fixed a bug where loading a `Record` graph that contained a nested `Record` with a dangling `Link` (one whose target had been cleared) inside a `Collection<Record>` field would throw `InvalidArgumentException`, making the graph unloadable until the dangling `Link` was removed manually. ([GH-94](https://github.com/cinchapi/runway/issues/94))
* Fixed a bug where loading a `Record` under a non-default `CollectionPreSelectStrategy` would throw `NullPointerException` and abort the entire load whenever a `Link` target was missing from the pre-fetched destination data. ([GH-95](https://github.com/cinchapi/runway/issues/95))

#### Version 1.14.5 (April 14, 2026)
* Fixed a bug where an anonymous audience could not discover access-controlled records that were readable or writable by anonymous unless discoverability was also explicitly granted, unlike non-anonymous audiences who could implicitly discover any record they were permitted to read or write

#### Version 1.14.4 (April 4, 2026)
* Fixed a bug where `Selection` objects passed to the `Runway.select()` method did not track state or results. The results were correctly available on the returned `Selections` container, but the input `Selection` objects should have also tracked this data. ([GH-90](https://github.com/cinchapi/runway/issues/90))
* Fixed a bug that allowed filtered `Selection` reads to poison the reservation cache and cause subsequent reads with the same parameters but a different or absent filter to return incorrect results. For example, a read through an `Audience` could cause subsequent `Runway`-wide reads to return results that were still narrowed by that audience's visibility rules. ([GH-89](https://github.com/cinchapi/runway/issues/89))
* Fixed a bug where multiple `Selection` objects with the same query parameters but different filters passed to a single `Runway.select()` call shared results instead of executing independently, causing the second selection to receive the first selection's filtered results. ([GH-92](https://github.com/cinchapi/runway/issues/92))
* Fixed a bug where injecting a `null` or no-op filter via `Selection.withInjectedFilter()` into a `Selection` that already had a client-side filter would silently discard the original filter, causing the resulting `Selection` to return unfiltered results.

#### Version 1.14.3 (April 3, 2026)
* Fixed a bug where `AdHocDataSource` records were invisible to `Runway.select()` when executing multiple selections simultaneously, causing count and data queries to return empty results ([GH-86](https://github.com/cinchapi/runway/issues/86))
* Fixed a bug where `Runway#close()` could leave dangling instances in the static registry if closing the connection pool threw an exception, which could interfere with subsequent implicit saves ([GH-87](https://github.com/cinchapi/runway/issues/87))

#### Version 1.14.2 (April 3, 2026)
* Fixed a bug where `Record#matches(Criteria)` returned incorrect results for navigation keys that traverse two or more consecutive collection-valued fields (e.g., `tenants.seats.user.userId` where both `tenants` and `seats` are `Set` fields). Only single-hop collection navigation worked correctly; paths with multiple collection hops always failed to match. This caused `Scope`-based visibility rules that use multi-hop navigation to incorrectly filter out records that should have been visible.
* Fixed a bug where `Record#matches(Criteria)` always returned `false` for `LINKS_TO` queries that use navigation keys terminating at a `Record`-valued field (e.g., `orgs.seats.member LINKS_TO 12345`). These queries now correctly match when the navigated record's id satisfies the `LINKS_TO` condition.
* **Behavioral change:** `Record#get(String)` with multi-hop navigation keys through consecutive collection-valued fields (e.g., `friends.friends.label`) now returns a flat collection of leaf values instead of nested collections-of-collections. The previous nesting was erroneous &mdash; the flat result is consistent with how Concourse resolves the same navigation key server-side.

#### Version 1.14.1 (April 2, 2026)
* Fixed a bug where the Selection API (`Selection.of` and `Selection.ofAny`) did not support unique-result queries, forcing callers to use the legacy `findUnique`/`findAnyUnique` methods instead. Added `Selection.ofUnique(Class)`, `Selection.ofAnyUnique(Class)`, and a chainable `.unique()` method on `InitialBuilder` and `QueryBuilder` that produce a `UniqueSelection` &mdash; returning the single matching record (or `null`) and throwing `DuplicateEntryException` when more than one match exists.
* Fixed a bug where passing duplicate `Selection` objects to `Runway#select(Selection...)` caused redundant database queries instead of reusing the result from the first occurrence.

#### Version 1.14.0 (April 2, 2026)
* **Static Visibility Scopes**: Added `Scope` and static scope registration to the `AccessControl` framework as a class-level alternative to instance-based visibility checks. When a `Scope` is registered for an `AccessControl` type, it is applied during `Audience.select()` in place of the per-instance `$isDiscoverableBy` check:
  * `Scope.of(Criteria)` pushes visibility filtering to the database as a query constraint, ensuring only matching records are returned rather than loading all records and filtering post-load. This is significantly more performant when only a small fraction of records for a class are visible to a given audience.
  * `Scope.unrestricted()` short-circuits to return all records without any filtering.
  * `Scope.none()` short-circuits to return no records without any database query.
  * `Scope.unsupported()` signals that scope-based visibility is not applicable; instance-based checks are used instead.
  * `AccessControl.registerVisibilityScope(Class, Function<Audience, Scope>)` registers a scope provider for a single class.
  * `AccessControl.registerVisibilityScopeHierarchy(Class, Function<Audience, Scope>)` registers a scope provider for a class and all known subclasses discovered at runtime, without overwriting any explicit per-class registrations already made.
  * Instance-based permissions remain the default and are recommended for most use cases. Static scopes are best suited when access rules can be expressed as a well-defined `Criteria` (to push filtering to the database) or when access is uniformly all-or-nothing across an entire class.
* **Selection API**: Added `Selection`, `Selections`, and `Runway#select(Selection...)` for declaring and executing multiple data retrieval operations together. The `select()` API possibly executes multiple reads in as little as a single database round trips, reducing overhead regardless of any other configuration.
* **Read Reservations**: Added `Runway#reserve()` and `Runway#unreserve()` to activate and deactivate a thread-local reserve that works with both the Selection API and direct read methods. When the reserve is active, `select()` caches its results so that subsequent calls to `select()`, `find()`, `count()`, `load()` &mdash; including reads through the `Audience` framework &mdash; return the cached data instead of re-querying the database. This is designed for the middleware/handler pattern: middleware calls `reserve()` and `select()` to pre-fetch data, route handlers read through `find()`/`count()`/`load()` or `Audience` methods and transparently benefit from the cache, and `unreserve()` clears everything at the end of the request.
* Added `Runway#getKnownRecordTypes()` to return all known `Record` subclasses discovered on the classpath at runtime.
* Fixed a bug where `Pagination.applyFilterAndPage` would throw a `NullPointerException` when invoked with a `null` filter or `null` page.
* Fixed a bug where local `Criteria` evaluation via `ConcourseCompiler` did not account for non-readable fields, producing results that diverged from how Concourse would resolve the same `Criteria` server-side. Non-readable (e.g., private) fields are stored in the database and indexed like any other field, so server-side resolution always considers them. Local evaluation now includes all fields regardless of visibility, matching server-side behavior.
* Added `Record#matches(Criteria)` to test whether a `Record` satisfies a `Criteria` locally. Navigation keys are fully supported, including traversal through private fields and collections of linked `Records`.
* Upgraded the `concourse-driver-java` dependency to `0.12.4` to fix a bug that caused local `Criteria` evaluation via `ConcourseCompiler` to provide inconsistent and unexpected results for records that did not contain a value stored under one or more keys in the input `Criteria`.

#### Version 1.13.1 (April 14, 2026)
* Fixed a bug where an anonymous audience could not discover access-controlled records that were readable or writable by anonymous unless discoverability was also explicitly granted, unlike non-anonymous audiences who could implicitly discover any record they were permitted to read or write

#### Version 1.13.0 (March 12, 2026)
* **Configurable `CollectionPreSelectStrategy`**: Added `CollectionPreSelectStrategy`, a configurable enum that controls how `Runway` pre-selects data for `Collection<Record>` fields (e.g., `List<Dock>`, `Set<Node>`). Previously, loading a Record with a collection of N linked Records issued N individual `select()` calls — one per element — inside `convert()`. Three strategies are now available:
  * `NAVIGATE` — uses Concourse's `navigate()` API to batch-prefetch all destination Record data in a single call with snapshot atomicity. Requires `StaticAnalysis` class-aware path computation.
  * `BULK_SELECT` — scans loaded data for `Link` values and batch-fetches all discovered targets via `concourse.select(Set<Long>)`, repeating per depth level until all reachable Records are collected. Schema-agnostic — works for untyped loads without class-specific path computation.
  * `NONE` — the legacy N+1 behavior where each linked Record is fetched individually.
  * Configure via `Runway.builder().collectionPreSelectStrategy(CollectionPreSelectStrategy.BULK_SELECT)`. Default is `NONE`.
  * Works across all query pipelines: `load()`, `find()`, and bulk `load(Class)`.
  * Self-referential collections (e.g., `List<Node>` on `Node`) are handled with cycle detection to prevent infinite path expansion.
  * Mixed field types (single `Record` + `Collection<Record>`) work correctly — collection pre-select covers the collection while the existing pre-select path mechanism covers single fields.
* **`computeOnce()` Memoization for `@Computed` Methods**: Added `Record#computeOnce(String, Supplier)`, a protected method that provides opt-in, per-instance memoization for expensive `@Computed` properties. During serialization, a `@Computed` method can be invoked through multiple independent paths — directly from a `@Derived` method, via `get(key)`, and through the serialization supplier — each triggering redundant work. Wrapping the method body with `computeOnce()` ensures all invocation paths share a single cached result, eliminating duplicate computations (e.g., database queries) within a serialization cycle.
  * `Record#clearComputeOnceCache()` invalidates all cached results, allowing fresh recomputation when the underlying data may have changed.
  * Opt-in only: existing `@Computed` methods that do not use `computeOnce()` retain their current behavior of recomputing on every access.
* **`Runway.Properties` Post-Build Configuration**: `Runway.Properties` (accessed via `runway.properties()`) is the centralized handle for post-build configuration and inspection of a `Runway` instance. It exposes getters and setters for `collectionPreSelectStrategy` and `onSave` listener registration. The direct `Runway#onSave` methods are deprecated in favor of the `Properties` equivalents.

#### Version 1.12.0 (March 7, 2026)
* **Spurious Save Failure Retry**: Added a `SpuriousSaveFailureStrategy` configuration that controls how `Runway` handles `TransactionException` during save operations. When set to `RETRY`, `Runway` automatically retries a failed save if none of the root records have stale data, indicating the failure was caused by a spurious MVCC conflict (e.g., overlapping `@Unique` constraint reads in concurrent transactions) rather than a genuine data conflict. The default strategy is `FAIL_FAST`, which preserves the existing behavior.
  * Configure via `Runway.builder().spuriousSaveFailureStrategy(SpuriousSaveFailureStrategy.RETRY)`.
  * Stale data detection uses Concourse's `review` audit to check whether any external writes occurred after the record was last loaded or saved.
* **Type-Specific Save Listeners**: The `onSave` method on `Runway.Builder` now supports type-specific listeners via a new `onSave(Class<T>, Consumer<T>)` overload. A listener registered for a type only fires for records that are instances of that type (including subclasses), eliminating the need for `instanceof` checks. The existing `onSave(Consumer<Record>)` method is now equivalent to `onSave(Record.class, listener)` and matches all records.
  * **Compositional**: Multiple `onSave` calls add listeners rather than replacing previous ones. All matching listeners fire in registration order.
  * **Error Isolation**: If a listener throws an exception, it is caught and suppressed, and subsequent matching listeners still fire.
* **Post-Build Save Listeners**: The `Runway` instance now exposes `onSave(Class<T>, Consumer<T>)` and `onSave(Consumer<Record>)` methods that allow registering save listeners after the instance has been built. New listeners are chained with any previously registered listeners. The notification infrastructure (queue and worker thread) is lazily initialized on the first post-build registration if no listeners were configured at build time.
* **Prevent Stale Writes**: Added a `Runway#save(boolean preventStaleWrites, Record...)` and `Record#save(boolean preventStaleWrote)`overloads that can be configured to reject a save when any `Record` in the object graph has been externally modified since it was last loaded or saved. When `preventStaleWrites` is `true`, every `Record` encountered during the save &mdash; including linked records &mdash; is checked for staleness before its data is written. If stale data is detected, a `StaleDataException` is thrown and the transaction is rolled back, guaranteeing that externally modified data is never silently overwritten. When `preventStaleWrites` is `false` (the default), saves behave as before. This check adds latency proportional to the size of the object graph, so it is best suited for environments where concurrent writes are common and data integrity is paramount.
  * The existing `save(Record...)` method delegates to `save(false, records)`, preserving full backward compatibility.
  * New `StaleDataException` (extends `RunwayException`) carries the primary key of the stale `Record` via `StaleDataException.id()`.
* **Record Refresh**: Added a `Record.refresh()` method that reloads a `Record`'s in-memory state from the database. After a refresh, the `Record` reflects the latest persisted data and is no longer considered stale, allowing subsequent `save(true, ...)` calls to succeed.
* Fixed a bug where save notifications were only fired for top-level records and not for linked records that were recursively saved within the same transaction. All records that actually persist changes during a save operation now receive notifications. Also fixed an issue where `Runway#save` with a single record would fire the save listener twice for the same record.

#### Version 1.11.0 (February 14, 2026)

##### Access Control Framework
Runway now provides a comprehensive access control framework that enables fine-grained, role-based access management for Record operations. This framework allows developers to define granular access rules that are automatically enforced across all database operations, ensuring data security and privacy at the application level.

* **`AccessControl` Interface**: Records can implement the `AccessControl` interface to define granular access rules for creation, discovery, reading, writing, and deletion. The interface provides methods to specify field-level permissions based on the requesting audience:
  * **Discovery Rules**: `$isDiscoverableBy(Audience)` and `$isDiscoverableByAnonymous()` control whether records can be found or seen at all
  * **Field Access Rules**: `$readableBy(Audience)` and `$writableBy(Audience)` define which fields can be accessed for read and write operations
  * **Lifecycle Rules**: `$isCreatableBy(Audience)` and `$isDeletableBy(Audience)` control record creation and deletion permissions
  * **Rule Types**: Support for allowlists (`Set.of("field1", "field2")`), denylists (`Set.of("-field1", "-field2")`), combined rules, and special rule sets (`AccessControl.ALL_KEYS`, `AccessControl.NO_KEYS`)

* **`Audience` Interface**: Represents the entity performing database operations and extends `DatabaseInterface` to provide access-controlled CRUD operations. Records implementing `Audience` can perform operations on behalf of themselves:
  * **`read(keys, record)`**: Enforces access control and throws `RestrictedAccessException` if any requested field is denied
  * **`frame(keys, record)`**: Filters out inaccessible data instead of throwing exceptions, returning only what the audience can access
  * **`create(record)`**, **`write(keys, record)`**, **`delete(record)`**: Access-controlled CRUD operations that respect the target record's access rules
  * **Navigation Support**: Automatic access control enforcement for dot-notation field access (e.g., `job.title`, `application.candidate.email`) where each navigation hop respects the target record's access rules

* **`Anonymous` Audience**: Provides a singleton audience implementation for unauthenticated users, accessible via `Audience.anonymous()`. This enables differentiated access rules between authenticated and anonymous users.

* **`RestrictedAccessException`**: A runtime exception thrown when an audience attempts unauthorized operations on access-controlled records, providing clear security boundary enforcement.

This access control framework enables developers to build secure, multi-tenant applications with role-based access patterns while maintaining the simplicity and performance characteristics of Runway's existing Record operations.

##### Auditing
Runway now provides comprehensive auditing capabilities that hook into Concourse's ability to automatically track all changes to Record instances over time. The new `audit()` method returns a chronological history of modifications, including what changed, when it changed, and, possibly, who made the changes:

* **Complete Change History**: Each timestamp in the audit trail represents a save operation, with associated revisions showing specific field changes
* **Author Attribution**: Changes made through the `Audience` framework are automatically attributed to the responsible Audience, providing clear accountability for all modifications
* **Unattributed Changes**: Changes made outside the Audience framework are still tracked but marked as "unknown author", ensuring complete visibility into all record modifications
* **Flexible Filtering**: Support for both positive and negative key filtering (e.g., `audit("name", "-internal")`) allows focusing on specific fields while excluding unwanted data
* **Change Type Detection**: The audit system automatically identifies SET operations (new values), CLEARED operations (removed values), and CHANGED operations (modified values)
* **Intrinsic Properties Only**: Change tracking is limited to intrinsic properties; computed, derived, and dynamic properties are not included in the audit trail as they are calculated on-demand rather than stored persistently

##### Interface Default Method Support for Annotations
Runway now recognizes `@Derived` and `@Computed` annotations on interface default methods, allowing implementing Records to inherit these property definitions without requiring explicit method overrides.

* **Reusable Property Definitions**: This enables creating interfaces that define common derived or computed properties across multiple Record types

##### Pluggable ID Support
Runway now allows Records to provide a custom "id" dynamic property that will be returned instead of the database ID for calls to `get("id")`, `map()`, and other data access methods.

* **Database ID Preservation**: The actual database ID remains unchanged and is always accessible using the `id()` method
* **Fallback Behavior**: If no dynamic "id" property is provided, the database ID is still returned for that key

##### Gateway Database Access Layer
Runway now provides a `Gateway` class that offers intelligent routing between database operations, simplifying database access by automatically choosing the most appropriate underlying operation based on the parameters provided.

* **Intelligent Operation Routing**: The `Gateway` automatically routes to the appropriate database operation (`find`/`load` or `findAny`/`loadAny`) based on the parameters provided. When any parameters are null, they are ignored in the retrieval process, allowing for flexible and concise database queries.

* **Unified Access Interface**: The `Gateway` provides a unified entry point for some database access without requiring manual accounting for optional arguments or conditional logic. This design allows client code to remain concise and expressive while ensuring that optional input such as `Criteria`, `Order`, or `Page` are honored consistently and efficiently.

* **Lazy Initialization**: The `Gateway` is lazily initialized when first accessed through the `DatabaseInterface.gateway()` method, providing efficient resource management.

* **Method Variants**: The `Gateway` provides both `retrieve` and `retrieveAny` methods that correspond to the underlying `find`/`load` and `findAny`/`loadAny` operations respectively, with full support for filtering, sorting, pagination, and realm-based access control.

##### Load Behavior Change
The `load(Class, long)` method in `DatabaseInterface` now returns `null` instead of throwing an `IllegalStateException` when attempting to load a Record with a non-existing ID.

* **New Behavior**: `load` returns `null` when the Record does not exist, allowing callers to handle missing records gracefully without exception handling
* **Backwards Compatibility**: The new `loadNullSafe(Class, long)` method preserves the previous fail-fast behavior by throwing an `IllegalStateException` when the Record does not exist

##### Data Priority Consistency Fix
Fixed a bug where there was inconsistent priorities in the order of data returned from `get()` vs `map()` operations.

* **Priority Order Standardization**: The priority order for data resolution has been standardized across both `get()` and `map()` operations:
  * **Dynamic data** (highest priority)
  * **Intrinsic data**
  * **Computed data**
  * **Derived data** (lowest priority)
* **Previous Inconsistency**: Previously, `get()` used the order: dynamic → intrinsic → derived → computed, while `map()` used: dynamic → intrinsic → computed → derived
* **Impact**: This fix resolves issues that occurred when the same key was used in both computed and derived data, ensuring consistent behavior across all data access methods

##### Ad-Hoc Records
Runway now provides infrastructure for serving non-persistent, in-memory data through the standard `DatabaseInterface` API. This enables seamless integration of programmatic data sources with persistent database records, with thread-local scoping for request-aware data attachment.

* **`AdHocRecord`**: A read-only `Record` base class for temporary, non-persistent data structures. Subclasses define their schema through fields like regular Records, but attempts to persist or modify an `AdHocRecord` will throw an `UnsupportedOperationException`. This is useful for generating report-like structures, aggregated data views, or other read-only data representations that need to be compatible with the application's data access patterns.

* **`AdHocDataSource`**: A `DatabaseInterface` implementation that serves a single `AdHocRecord` type from an in-memory data source. Data is supplied via a `Supplier` that is evaluated on each query, allowing for dynamic or computed data. The `AdHocDataSource` supports full query capabilities including `Criteria` filtering, `Order` sorting, and `Page` pagination—all resolved in-memory against the supplied collection.

* **`Runway.attach(AdHocDataSource...)`**: Attaches one or more ad-hoc data sources to the Runway instance for the current thread. When attached, queries for the source's `AdHocRecord` type are automatically routed to the attached source instead of the underlying database. Returns an `AttachmentScope` that implements `DatabaseInterface` and `AutoCloseable` for convenient try-with-resources usage:
  ```java
  AdHocDataSource<ReportRecord> reports = new AdHocDataSource<>(
      ReportRecord.class, () -> generateReports());

  // Using try-with-resources for automatic cleanup
  try (AttachmentScope scope = runway.attach(reports)) {
      // Both handles serve attached data
      scope.load(ReportRecord.class);   // Returns reports
      runway.load(ReportRecord.class);  // Also returns reports
      runway.load(User.class);          // Routes to database
  }
  // Sources automatically detached on close
  ```

* **`Runway.detach(AdHocDataSource)`** / **`Runway.detach(Class)`**: Explicitly detaches an ad-hoc data source from the current thread, restoring normal database routing for that Record type.

* **Thread Isolation**: All attached sources are thread-local, enabling request-scoped or context-scoped attachment. Sources attached in one thread do not affect queries in other threads, making this feature safe for use in multi-threaded web applications.

* **Search Not Supported**: Full-text search operations (`search` and `searchAny`) are not supported for attached `AdHocDataSource` instances. These methods always query the underlying database. Use `find` with appropriate `Criteria` for filtering ad-hoc data.

##### Other Improvements
* **Record Reference Replacement**: Added a new `replace(Record find, Record replace)` method to the `Record` class that recursively replaces all references to a specific record instance with another record throughout the object graph, maintaining referential integrity while handling nested records, deferred references, and sequences.
* **Metadata Interface**: Added a new `Metadata` interface that provides implementing Record types with computed properties to obtain the Record's timestamps for creation and most recent update (including the ability to filter for most recent update to specific keys).
* **Runway Exception Hierarchy**: Introduced a dedicated exception hierarchy for Runway-specific errors to enable more nuanced error handling. The `Record#throwSuppressedExceptions()` method now throws a `SuppressedRunwayException` (which extends the new `RunwayException` base class) instead of a generic `RuntimeException`. Additionally, `ZombieException` now extends `RunwayException`. This change is fully backward compatible since both `RunwayException` and `SuppressedRunwayException` extend `RuntimeException`, meaning existing code that catches `RuntimeException` will continue to work as expected. This enhancement allows applications to distinguish between Runway-specific exceptions and other runtime exceptions, facilitating more precise exception handling strategies.

##### Bug Fixes
* Fixed an issue where static analysis failed to detect `Record` subtypes on Java 9+, causing runtime errors where Record subclasses were not properly recognized. This was due to classloader changes introduced by the Java Platform Module System (JPMS) that prevented the Reflections library from discovering classpath entries using its default configuration.
* [GH-68](https://github.com/cinchapi/runway/issues/68): Fixed a bug that caused deletion to fail when the record subject to deletion was referenced under a `@CaptureDelete` field in a parent that also had another `@CaptureDelete` field containing a `null` value
* [GH-67](https://github.com/cinchapi/runway/issues/67): Fixed a bug that caused loading to fail when a `Record` contained a collection of linked `Records` and one of those links had been deleted or cleared (e.g., had no data in the database). Now, in this scenario, Runway will remove the stale reference.

#### Version 1.10.1 (October 1, 2025)
* Fixed a bug that caused the `countAny(Class, Criteria, Realms)` method to incorrectly count records within only the specified class instead of across the entire class hierarchy.

#### Version 1.10.0 (May 11, 2025)

##### Deletion Hooks
New deletion hooks are available to ensure automatic referential integrity when records are deleted. These annotations streamline data management by automatically handling dependencies between records.

* **`@CascadeDelete`**: Simplifies deletion of dependent records within the framework. Fields annotated with `@CascadeDelete` automatically delete their linked records when the containing record is removed. This functionality ensures that related records do not persist after their parent records are deleted, preserving consistency. Deletions occur in a single, atomic transaction, allowing for more efficient data cleanup.

* **`@JoinDelete`**: Automates the deletion of containing records when a linked record is removed. Fields annotated with `@JoinDelete` trigger the deletion of the containing record if the linked record is deleted. This is the reverse of `@CascadeDelete`, as it removes all parent or container records that depend on the existence of linked records, thereby ensuring referential integrity. The operation is performed atomically.

* **`@CaptureDelete`**: Facilitates automatic reference removal for cases where a linked record is deleted but the containing record should remain intact. When a record is deleted, fields annotated with `@CaptureDelete` are automatically set to `null` or removed from the containing record's collection. This allows for more flexible data management, maintaining integrity without deleting the containing record.

##### Save Lifecycle Hooks
Runway now provides comprehensive options for injecting logic into the save routine and responding to save events, enabling more flexible and reactive data management patterns.

* **Breaking Change**: The `Record#save` method has been made `final` to ensure consistent behavior across all save operations, including bulk saves. Previously, overridden `save` methods were not called during Runway's bulk save operations, leading to inconsistent behavior. Applications should migrate any custom save logic to either the `beforeSave` hook or save listeners.

* **`beforeSave` Hook**: Added a protected `beforeSave` method to the `Record` class that is automatically called before a record is saved to the database. This hook allows records to update their state or perform validation immediately before persistence:
  * Executes within the same transaction as the save operation, ensuring atomicity
  * Can modify record fields, with changes included in the save operation
  * Allows for custom validation beyond what annotations provide
  * Exceptions thrown from `beforeSave` abort the save operation and roll back the transaction
  * Works consistently with both individual and bulk save operations

* **Save Listeners**: Added support for registering save listeners that allow applications to be notified when records are successfully saved. This feature enables reactive workflows and improved integration with external systems:
  * **Save Notification**: Using the `onSave` method of the `Runway.builder()`, applications can register a listener that will be called whenever a record is successfully saved. The listener is called asynchronously after the save transaction is committed, ensuring that notifications only occur for successful operations.
  * **Efficient Processing**: Save notifications are processed in a dedicated background thread, allowing the main application to continue without waiting for notification processing to complete.
  * **Error Tolerance**: Exceptions thrown by save listeners are silently swallowed, ensuring that listener errors don't affect the application's core functionality.

* **`overrideSave` Hook**: Added a protected `overrideSave` method to the `Record` class that allows completely bypassing the standard save routine:
  * Returns a `Supplier<Boolean>` that determines the result of the save operation without database interaction
  * When non-null, the normal persistence mechanism is skipped entirely
  * Useful for creating in-memory only records or implementing custom persistence logic
  * Works consistently with both individual and bulk save operations

##### New Functionality and Enhancements
* Added `@Computed` and `@Derived` annotations that can be applied to methods to mark them as returning `computed` and `derived` properties, respectively. These annotations are meant to be used in lieu of the `#computed()` and `#derived()` methods, which are now deprecated
* Introduced a new `Record.set(Map<String, Object> data)` method that allows for bulk updating of fields within a record.
* added `Runway.ping()` to provide an interface to Concourse's new ping healthcheck, introduced in `0.11.10`.

##### Improvements
* Improved Runway's bulk loading functionality to ensure that the same object reference is used for a linked Record that exists as a value in multiple records. Previously, in a single bulk load operation, Runway would create a new Java object for EVERY loaded reference, regardless of whether that referenced object was already encountered earlier in the load, which created unnecessary heap bloat. This optimization reduces memory usage and ensures object identity is maintained across references to the same record within a single load operation.
* Optimized computed value generation to ensure values are only computed once per map operation. Previously, when filtering null values during serialization, computed values were unnecessarily generated twice - once during the null check and again when adding to the result map. This improvement caches computed values within each map operation while still ensuring fresh values are generated for each new operation.
* Enhanced the save functionality to detect when an existing Record has been modified and only attempt to write to the database if its required to reflect new state. Previously, whenever a Record was saved, Runway always attempted to write to the database, even if doing so was a no-op. This optimization eliminates unnecessary database operations and improves performance. This is especially true for Records that link to other Records since Runway's save functionality cascades and automatically save's any referenced Records to maintain referential integrity. Now, when a save is cascaded, those referenced Records will only perform database writes if they have indeed been modified.

##### Bug Fixes
* Fixed a regression that casued a `NullPointerException` to be thrown when a `null` intrinsic, `derived` or `computed` value was encountered while performing local `condition` evaluation.
* Fixed a few bugs that caused `@Required`, `@Unique` and `@ValidatedBy` constraints to behave unexpectedly in certain scenarios:
  * For a field containing a Sequence value, `@ValidatedBy` was applied to the entire Sequence as a whole, instead of to each item in the Sequence indivudally.
  * For a field containing a Sequence value, `@Unique` was checked for the entire Sequence as a whole, instead of for each item in the Sequence indivudally.
  * For a field containing a Sequence value, `@Required` was not properly enforced in cases when the Sequence was empty.
* Fixed a bug that made it possible for a field containing a Sequence of `DeferredReference` objects, to have items in that sequence erroneously removed if those items were not loaded using `DeferredReference.get()` before the housing Record was saved.
* Fixed a bug that caused a `NoSuchElementException` to be thrown instead of an `IllegalStateException` when attempting to `load` an non-existing `Record`.
* Fixed a bug that caused record deletion via `deleteOnSave` to not persist if the deleted Record was saved using `Runway.save(Record...)` bulk save functionality.

#### Version 1.9.4 (July 22, 2022)
* Fixed a bug that occurred when using *pre-select* to load a Record containing a reference field whose **declared** type is the parent class of a descendant class with additionally defined fields and the stored value for that field is an instance of that descendant class. In this case, the pre-select logic did not load data for the descendant defined fields, which resulted in unexpected `NullPointerException` regressions or an overall inability to load those Records if the descendant defined field was annotated as `Required`.
* Improved the efficiency of local `condition` evaluation by removing unnecessary data copying.
* Addressed performance regressions that have been observed when performing pagination alongside a locally resolvable `filter` or `condition` whose matches are sparsely distributed among the unfiltered results. The pagination logic still incrementally loads possible matches (instead of all-at-once), but uses additional logic to dynamically adjust the number of possible matches loaded based on whether the previous batch contained any matches.

#### Version 1.9.3 (July 4, 2022)
* For instances of Concourse Server at version [`0.11.3`](https://github.com/cinchapi/concourse/releases/tag/v0.11.3)) or greater, we improved overall read performance by pre-selecting data for linked Records, whenever possible. Previously, if a `Record` contained an attribute whose type was another `Record`, Runway would eagerly load the data for that reference in a separate database call. So, if Runway needed to process a read of many Records with references to other Records, performance was poor because there were too many database round trips required. Now, Runway will detect when a `Record` has references to other Records and will  pre-select the data for those references while selecting the data for the parent `Record` if it is possible to do so. This greatly reduces the number of database round trips which drastically improves performance by up to `89.7%`.
  * This improvement is automatically enabled whenever `Runway` is connected to a Concourse deployment at version [`0.11.3+`]. If necessary, it is possible to disable the functionality when building a `Runway` instance by invoking the `disablePreSelectLinkedRecords()` method.
* Added a new `Runway.properties()` method that exposes an interface to get metadata and other information about a `Runway` instance. This interface can be used to query whether a `Runway` is capable and configured to take advantage of pre-selection.
* Improved the performance of `Runway` commands that perform pagination when a `filter` or a `condition` that must be resolved locally (e.g., because it references derived or computed keys not in the database) is provided. Previously, in these cases, `Runway` would load all possible records before applying the `filter` or `condition` and lastly performing pagination. Now, `Runway` incrementally loads possible matching records and applies the `filter` or `condition` on the fly until the requested `Page` has been filled.
* Removed the `com.cinchapi.runway.util.Paging` class that was copied from the `concourse-server` project since it is no longer used for internal pagination logic.
* Removed unnecessary random result set access when lazily instantiating the Set of records that match a `Runway` operation.
* Optimized load performance by
  * using more intelligent logic to scaffold a `Record` instance and
  * performing static analysis and caching immutable metadata for `Record` types that was previously computed during each load.

#### Version 1.9.2 (March 18, 2022)
* Upgraded the underlying `Concourse` client dependency to version [`0.11.2`](https://github.com/cinchapi/concourse/releases/tag/v0.11.2), which means that Runway now supports specifying a CCL function statement as an operation key or an operation value if it is connected to a Concourse Server that is version `0.11.0+`.

#### Version 1.9.1 (February 20, 2022)
* Fixed a bug that randomly causes a spurious error to be thrown indicating that a Record attribute doesn't exist in the database when an attempt is made to access it.

#### Version 1.9.0 (August 14, 2021)
* Added support for multi-field `Unique` value constraints. When applying the `Unique` constraint to a `Record` field, you can now provide a `name` parameter (e.g. `@Unique(name = "identity"))`. If multiple `Unique` annotated fields have the same `name`,  Runway will enforce uniqueness among the combination of values for all those fields across all `Records` in the same class. If a `Unique` annotated field is a `Sequence`, Runway will consider uniqueness to be violated if and only if any items in the sequence are shared and all the other fields in the same uniqueness group are also considered shared.
* Added `Realms` to virtually segregate records within the same environment into distinct groups. A `Record` can be dynamically added to or removed from a `realm` (use `Record#addRealm` and `Record#removeRealm` to manage). Runway provides overloaded read methods that accept a `Realms` parameter to specify the realms from which data can be read. If a Record exists in at least one of the specified `Realms`, it will be read.
  * By default, all Records exist in ALL realms, so this feature is backwards compatible.
  * By default, read methods consider data from ANY realm, so this feature is backwards compatible.
* Fixed a bug where the `Required` annotation was not enforced when loading data from the database. If a record was modified outside of Runway such that a required field was nullified, Runway would previously load the record without enforcing the constraint. This caused applications to encounter some unexpected `NullPointerException`s.

#### Version 1.8.1 (April 20, 2020)
* Fixed a bug that allowed for dynamically `set`ing an intrinsic attribute of a `Record` with a value of an invalid type. In this scenario, Runway should have thrown an error, but it didn't. While the value with the invalid type was not persisted when saving the Record, it was return on intermediate reads of the Record.

#### Version 1.8.0 (February 12, 2020)
* Improved validation exception messages by including the class name of the Record that fails to validate.
* Added a `onLoadFailure` hook to the `Runway.builder` that can be used to get insight and perform processing on errors that occur when loading records from the database. Depending on the error, load failures can be fatal (e.g. the entire load operation fails). The `onLoadFailure` hook does not change this, but it does ensure that fatal errors can be caught and inspected. By default, Runway uses a non-operational `onLoadFailure` hook. The hook can be customized by providing a `TriConsumer` accepting three inputs: the record's `Class` and `id` and the `Throwable` that represents the error.
* Fixed an issue that occurred when setting a value to `null` and that value not being removed from the database.

#### Version 1.7.0 (January 1, 2020)
* Fixed a bug that caused `Runway` to exhibit poor performance when using the `withCache` option.
* Fixed bugs that caused Runway's data caching to exhibit inconsistent behaviour where stale data could be added to the cache.
* Added a `Runway#builder` option to specify a `readStrategy`. Runway's **read strategy** determines how Runway reads data from Concourse.
  * The `BULK` strategy uses Concourse's `select` method to pull in all the data for all the records that match a read at the same time.
  * The `STREAM` option uses Concourse's `find` method to find the ids of all the records that match a read in order to stream the data for those records on-the-fly when needed.
  * The `AUTO` option contextually uses the `BULK` or `STREAM` option on a read-by-read basis (usually depending on which option will return results faster).
By default, Runway uses the `AUTO` strategy unless a `cache` is provided, in which case, the `STREAM` option is used by default since data streaming is more cache-friendly and is consistent with the way record caching previously worked in previous versions of Runway.
* Deprecated the `recordsPerSelectBufferSize` option in the `Runway#builder` in favor of the `streamingReadBufferSize` option which has the same effect.

#### Version 1.6.0 (November 23, 2019)
* Fixed a bug that caused `Runway` operations to occassionally trigger an `out of sequence response` error in the underlying Concourse connections.
* Added support **data caching**. This feature can be enabled by passing a `Cache` to the `Runway#builder#withCache` method. Data caching is an improvement over record caching. With this new feature, caching is managed closer to the level of database interaction to ensure greater performance, timely invalidation and scalability.
* Improved internal logic that determines whether `Runway` serves a request by bulk selecting data or incrementally streaming.
* Added initial support for `find`ing and `count`ing `Criteria` conditions that touch `computed` and `derived` data. There is currently no support for querying on non-intrinsic data of linked Records (e.g. no navigation).

#### Version 1.5.0 (November 17, 2019)
* Fixed a bug that caused the `countAny` methods to return the wrong data.
* Added methods to the `Runway` driver that support filtering data. Unlike a `Criteria` or `Condition` a `filter` is a `Predicate` that receives the loaded `Record` as input and executes business logic to determine whether the `Record` should be included in the result set. For example, filtering can be used to seamlessly enforce permissions in a `Runway` method call by passing in a predicate that checks whether the caller has access to the `Record`.
* Remove support for record caching. The `Runway#builder#cache` method has been deprecated. Providing a record cache to Runway no longer has any effect.
* Added an `onLoad` hook to the `Record` class that can be used to provide a routine that is executed whenever an existing Record is loaded from the database.
* Fixed a bug that caused the linked objects included in the `map` or `json` functions to not respect the provided `SerializationOptions`.

#### Version 1.4.1 (October 2, 2019)
* Fixed a regression bug where the `Runway#findAnyUnique` failed because an attempt was made to instantiate an object of the provided class instead of the record's stored class.

#### Version 1.4.0 (August 24, 2019)
* Added a `DeferredReference` type that can be used to wrap a `Record` link within another `Record`. When a `DeferredReference` is used, the linked `Record` isn't loaded from the database until it is actually referenced for usage.
* Added support for native database sorting and pagination that is introduced in Concourse version `0.10.0`. The `Runway` driver now contains read methods that accept `Order` and `Page` parameters. If the connected server does not support native sorting and pagination, Runway will fallback to local sorting and pagination that was used prior to Concourse `0.10.0`.
* Added support for link navigation in the `#get` and `map` methods. If a `navigation key` is provided, `Runway` will traverse linked `Record` references to return the destination value(s). In the case of `map`, those destination values will be associated with a nested structure that encapsulates the Record hiearchy. For example, mapping `users.name` where `users` is a collection of Records and `name` is a String will return a mapping from `users` to a collection of maps containing the `name` key mapped to the respective value for each linked Record in the original Record's `users` collection.
* Added `#count` and `#countAny` methods to the `Runway` interface to count the number of records in a Class or across a Class hiearchy that possibly match a Criteria.
* Improved the intelligence of data streaming so that it is only activated when necessary.

#### Version 1.3.2 (August 19, 2019)
* Introduced data streaming to fix an issue that caused some requests to time out when loading large amounts of data.

#### Version 1.3.1 (July 15, 2019)
* Improved `load` performance by removing extraneous data loading.

#### Version 1.3.0 (June 29, 2019)
* Fixed a bug that cause the `Record#map` method to throw a `NullPointerException` when explictly requesting a key whose value was `null`.
* Added the `Record#intrinsic` methods, which behave similiary to the analogous `map` methods with the only difference being the `intrinsic` only considers properties that are not derived or computed (e.g. intrinsic to the Record and therefore stored in Concourse).
* Fixed a bug that caused a `NullPointerException` when attempting to retrieve a `Record`'s id by providing the `id` key to the `get`, `map`, or `json` methods. It is still preferable to retrive the id using the `#id()` method but fetching it indirectly is now supported.
* Added the `SerializationOptions` container to encapsulate various preferences when serializing `Record`s as either `json` or a `map`. Right now, the supported options are `flattenSingleElementCollections` to return Concourse-style JSON and `serializeNullValues` to return JSON containing key/value pairs where the value is null. `SerializationOptions` provide much more fluency and flexibility. As a result, the `json` methods that took a boolean parameter to `flattenSingleElementCollections` are now deprecated.
* Added *Just-In-Time Loading* for results returned from the `#find` and `#load` methods. Now, the work of loading the data for a Record in the result set is deferred until that Record is actually used. This makes stream operations more efficient because unnecessary data is no longer loaded during intermediate operations.
* Improved the performance of loading Records by loading all the record's data in memory at once instead of dispatching separate `get` or `select` requests on a field by field basis.
* Added `#search` and `#searchAny` methods to the `Runway` controller. Both of these methods provide an interface for Concourse's fulltext search functionality.
* Runway now supports result set sorting. We've added `find`, `findAny`, `load` and `loadAny` methods that take an `order` parameter in the form of a `List` or a space separated `String` sequence of `sort keys`. A `sort key` is a record attribute that is prepended with a `>` or `<` to respectively imply ascending (default) or descending sorting on the attribute. It is now possible to sort a result set on any number of keys.
* `Record`s are now `Comparable`, in support of the aformentioned result set sorting functionality.
  * **NOTE:** Sorting on computed or derived keys is **NOT** supported.
* Improved the error messages that are thrown from `Record#throwSuppressedExceptions` so that only the  messages from the suppressed exceptions are included in the thrown Exception's message as opposed to the entire stacktrace. The full stacktrace can be accessed using `Exception#getStackTrace`.
* Added a `builder()` factory to `Runway`. This builder can be used to construct a `Runway` instance. As a result, the `connect` methods that take parameters have been deprecated.
* Added support for *optional* caching to improve load performance. The `Runway` `builder` container a `cache` option that allows for specifying a Guava `Cache` that is used by Runway to cache references to loaded objects. Usage of the cache can improve the load performance of a dependent application, but should only be used if the underlying database is only changed by the Runway-dependent application.
* Made an improvement such that saving a Record that contains fields whose types are Record types (but not a collection of Records) no longer create unnecessary database revisions. Previously, the save routing would always `#set` the value of the field, which removed and added the value even if the value didn't change. Now, the `#verifyOrSet` method is used to store the value, so revisions are only created in the database if the value has actually changed.
* Improved the cylce detection algorithim in the `Recod#json` generation functionality by adding more granular cycle detection so that linked Record objects are expanded and printed unless doing so would definitely create an immediate cycle.
* Added Just-in-Time Opportunistic Bulk Loading(JITOBL) to make `#find*`, `#load*` and `#search*` methods more efficient. With JITOBL, Runway will select data for multiple records in as few database calls as possible.

#### Version 1.2.0 (March 4, 2019)
* In the `Record` class, we added a `db` attribute, containing a reference to the `Runway` instance to which the `Record` is assigned. The `db` can be used to create getter methods or computed properties that query the database to return dynamic values. For example, if a `Record` class is the destination link from a field in many other `Record` classes (e.g. a one-to-many relationship), you can query the `db` to return all the related source records.
  * **NOTE:** Runway assignment happens automatically whenever a Record is 1) loaded, 2) saved and 3) created in a JVM where only a single Runway instance is available. If a Record is created when multiple Runway instances are available, the desired one can be assigned using the `Record#assign` method.

#### Version 1.1.2 (February 13, 2019)
* Fixed a regression introduced in Version `1.1.1`. This regression caused fields of `Records` to be stored in Concourse improperly. When saving, the items in those collections would overwrite each other so that only one value was stored at a time. This release fixes the bug and restores the correct functionality.

#### Version 1.1.1 (February 3, 2019)
* Fixed a bug that cause an issue when updating non-collection `Record` fields (which are stored as `Links` in Concourse). Previously, updating a Record reference would cause the new value to be appended to the old value in Concourse so that multiple values were stored, simulating a collection. This behaviour has now been corrected so that the new value will overwrite the previously stored value.

#### Version 1.1.0 (January 26, 2019)
* Added a `map(String...keys)` method with different semantics than that of the `get(String...keys)` method. In the `map` method, all the `Record`'s readable data is returned if no keys are provided. In the `get` implementation, an empty `Map` is returned instead.
* Deprecated the `get(String...keys)` method since it is redundant in light of the introduction of the `map(String...keys)` method.
* Added support for **negative filtering** in the `map(String...keys)` method. With negative filtering, you can provide a key that is prefixed with the minus sign (e.g. `-`) to indicate that the key should not be included in the data that is returned.
* Added the `compute` hook to support calculating dervided properties that are "expensive" to compute, on-demand.
* Fixed a bug that cause the `Runway#connect()` factory to return `null`.
* Deprecated the `Runway#findOne` methods in favor of ones named `Runway#findUnique` for better semantics and readability. The new methods have the same functionality as the old ones.
* Added methods that query across application defined class hiearchies. These methods allow you to find and load records across a class hiearchy using a parent/base class.
  * `Runway#findAny` finds any Records whose type matches the provided class or one of its descendant classes.
  * `Runway#findAnyUnique` loads a unique Record whose type matches the provided class or one of its descenadant classes. If multiple Records within the hiearchy match the criteria, a `DuplicateEntryException` is thrown.
  * `Runway#loadAny` loads any Records whose type matches the provided class or one of its descendant classes.

#### Version 1.0.0 (October 13, 2018)
* Refactor and major version release!
