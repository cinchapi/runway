# Changelog

#### Version 1.1.0 (TBD)
* Changed the semantics of the `get(String...keys)` method so that all the `Record`'s readable data is returned if no keys are provided. In the previous implementation, an empty `Map` would have been returned instead. This means that calling `get()` has the same affect as calling `map()`.
* Deprecated the `map()` method since its now redundant in light of the changes to the `get()` method.
* Added support for **negative filtering** in the `get(String...keys)` method. With negative filtering, you can provide a key that is prefixed with the minus sign (e.g. `-`) to indicate that the key should not be included in the data that is returned.

#### Version 1.0.0 (October 13, 2018)
* Refactor and major version release!