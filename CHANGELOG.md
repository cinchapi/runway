# Changelog

#### Version 1.1.1 (TBD)
* Fixed a bug that cause an issue when updating non-collection `Record` fields (which are stored as `Links` in Concourse). Previously, updating a Record reference would cause the new value to be appened to the old value in Concourse so that multiple values were stored, simulating a collection. This behaviour has now been corrected so that the new value will overwrite the previously stored value.

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