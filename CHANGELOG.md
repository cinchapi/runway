# Changelog

#### Version 1.3.0 (TBD)
* Fixed a bug that cause the `Record#map` method to throw a `NullPointerException` when explictly requesting a key whose value was `null`.
* Added the `Record#intrinsic` methods, which behave similiary to the analogous `map` methods with the only difference being the `intrinsic` only considers properties that are not derived or computed (e.g. intrinsic to the Record and therefore stored in Concourse).
* Fixed a bug that caused a `NullPointerException` when attempting to retrieve a `Record`'s id by providing the `id` key to the `get`, `map`, or `json` methods. It is still preferable to retrive the id using the `#id()` method but fetching it indirectly is now supported.
* Added the `SerializationOptions` container to encapsulate various preferences when serializing `Record`s as either `json` or a `map`. Right now, the supported options are `flattenSingleElementCollections` to return Concourse-style JSON and `serializeNullValues` to return JSON containing key/value pairs where the value is null. `SerializationOptions` provide much more fluency and flexibility. As a result, the `json` methods that took a boolean parameter to `flattenSingleElementCollections` are now deprecated.

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