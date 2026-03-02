# Changelog

#### Version 1.12.0 (TBD)
* **Spurious Save Failure Retry**: Added a `SpuriousSaveFailureStrategy` configuration that controls how `Runway` handles `TransactionException` during save operations. When set to `RETRY`, `Runway` automatically retries a failed save if none of the root records have stale data, indicating the failure was caused by a spurious MVCC conflict (e.g., overlapping `@Unique` constraint reads in concurrent transactions) rather than a genuine data conflict. The default strategy is `FAIL_FAST`, which preserves the existing behavior.
  * Configure via `Runway.builder().spuriousSaveFailureStrategy(SpuriousSaveFailureStrategy.RETRY)`.
  * Stale data detection uses Concourse's `review` audit to check whether any external writes occurred after the record was last loaded or saved.
* **Centralized Save Logic**: All save paths (`Record#save()` and `Runway#save(Record...)`) now flow through a single implementation in `Runway#save`, eliminating duplicated transaction management and ensuring consistent behavior for single-record and multi-record saves.
* **Type-Specific Save Listeners**: The `onSave` method on `Runway.Builder` now supports type-specific listeners via a new `onSave(Class<T>, Consumer<T>)` overload. A listener registered for a type only fires for records that are instances of that type (including subclasses), eliminating the need for `instanceof` checks. The existing `onSave(Consumer<Record>)` method is now equivalent to `onSave(Record.class, listener)` and matches all records.
  * **Compositional**: Multiple `onSave` calls add listeners rather than replacing previous ones. All matching listeners fire in registration order.
  * **Error Isolation**: If a listener throws an exception, it is caught and suppressed, and subsequent matching listeners still fire.
* **Post-Build Save Listeners**: The `Runway` instance now exposes `onSave(Class<T>, Consumer<T>)` and `onSave(Consumer<Record>)` methods that allow registering save listeners after the instance has been built. New listeners are chained with any previously registered listeners. The notification infrastructure (queue and worker thread) is lazily initialized on the first post-build registration if no listeners were configured at build time.
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