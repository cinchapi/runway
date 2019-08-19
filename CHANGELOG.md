# Changelog

#### Version 1.4.0 (TBD)
* Added a `DeferredReference` type that can be used to wrap a `Record` link within another `Record`. When a `DeferredReference` is used, the linked `Record` isn't loaded from the database until it is actually referenced for usage.
* Added support for native database sorting and pagination that is introduced in Concourse version `0.10.0`. The `Runway` driver now contains read methods that accept `Order` and `Page` parameters. If the connected server does not support native sorting and pagination, Runway will fallback to local sorting and pagination that was used prior to Concourse `0.10.0`.
* Added support for link navigation in the `#get` and `map` methods. If a `navigation key` is provided, `Runway` will traverse linked `Record` references to return the destination value(s). In the case of `map`, those destination values will be associated with a nested structure that encapsulates the Record hiearchy. For example, mapping `users.name` where `users` is a collection of Records and `name` is a String will return a mapping from `users` to a collection of maps containing the `name` key mapped to the respective value for each linked Record in the original Record's `users` collection.
* Added `#count` and `#countAny` methods to the `Runway` interface to count the number of records in a Class or across a Class hiearchy that possibly match a Criteria.

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