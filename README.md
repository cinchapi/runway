Runway
======

Runway is the official ORM (Object-Record Mapping) framework for [Concourse](https://concoursedb.com). It provides a framework for persisting simple POJO-like objects to Concourse while automatically preserving transactional security, enforcing constraints, and managing record relationships.

## Getting Started

### Connecting to Concourse

Use the `Runway` controller to connect to a Concourse database. The simplest approach uses default connection parameters (localhost:1717, admin/admin):

```java
Runway db = Runway.connect();
```

For more control, use the builder:

```java
Runway db = Runway.builder()
        .host("db.example.com")
        .port(1717)
        .username("admin")
        .password("secret")
        .environment("production")
        .build();
```

`Runway` implements `AutoCloseable`, so it can be used in a try-with-resources block:

```java
try (Runway db = Runway.builder().build()) {
    // work with db
}
```

### Auto-Pinning

When only a single `Runway` instance exists, it is automatically "pinned" so that Records can call `save()` on themselves without an explicit reference. If multiple instances exist, you must save Records through the `Runway` controller directly using `db.save(record)`.

## Defining Record Types

Persistable types extend the `Record` class. Like Concourse itself, Runway does not require an explicit schema. The class definition and optional annotations are all that is needed.

```java
public class Player extends Record {

    public String name;

    @Unique
    @Required
    protected String email;

    private int score = 0;
}
```

### Schema Rules

- **Non-transient fields** are persisted to the database. Mark fields `transient` to exclude them.
- **Field visibility** controls serialization output. Public and protected fields appear in `json()` and `map()` output. Private fields are stored but excluded from serialized output unless annotated with `@Readable`.
- **Java inheritance** is fully supported. Fields from superclasses are inherited by subclasses.

### Identifiers

Every Record has a unique `id` assigned automatically when it is first created. Access it via `record.id()`.

### Type Mapping

Runway intelligently maps Java types to Concourse storage:

| Java Type | Concourse Storage |
|---------------------|------------------------------------------|
| Primitives, Strings | Stored directly as the corresponding type |
| `Record` subclasses | Stored as Links between records |
| Collections, Arrays | Each element stored individually for the key |
| `Serializable` | Stored in serialized (binary) form |
| `Enum` | Stored as the enum constant's `Tag` representation |

## Constraints

Annotate fields to declare database constraints that Runway enforces on save:

| Annotation | Effect |
|----------------|-------------------------------------------------------|
| `@Required` | Rejects save if the value is `null` or an empty collection/array |
| `@Unique` | Rejects save if another record of the same class has the same value |
| `@ValidatedBy` | Rejects save if the value fails the specified `Validator` |

### Compound Uniqueness

Apply the same `@Unique` constraint across multiple fields by giving them the same `name`. This enforces that the *combination* of values is unique, rather than each field independently:

```java
@Unique(name = "location")
public String city;

@Unique(name = "location")
public String state;
```

## CRUD Operations

### Creating Records

Create a Record by calling its constructor. No database interaction occurs until the record is saved.

```java
Player player = new Player();
player.name = "Serena Williams";
player.email = "serena@example.com";
```

### Saving Records

Save a Record to persist its current state to Concourse. Runway calculates diffs and stores only the changes within an ACID transaction.

```java
boolean success = player.save();
```

If constraints are violated, `save()` returns `false`. Call `throwSuppressedExceptions()` to get a detailed stack trace of the failure.

Save multiple records in a single ACID transaction via the `Runway` controller. This is essential when records reference each other:

```java
Player player1 = new Player();
Player player2 = new Player();
player1.rival = player2;
db.save(player1, player2);
```

### Loading Records

Load a single Record by its class and id:

```java
Player player = db.load(Player.class, 42);
```

Load all Records of a type:

```java
Set<Player> allPlayers = db.load(Player.class);
```

### Finding Records

Use Concourse `Criteria` to query for matching Records:

```java
Set<Player> found = db.find(Player.class,
        Criteria.where().key("score")
                .operator(Operator.GREATER_THAN)
                .value(100).build());
```

### Finding a Unique Record

When you expect exactly one result:

```java
Player player = db.findUnique(Player.class,
        Criteria.where().key("email")
                .operator(Operator.EQUALS)
                .value("serena@example.com").build());
```

### Polymorphic Queries

Methods ending in `Any` search across a type hierarchy. For example, if `ProPlayer` and `AmateurPlayer` both extend `Player`:

```java
// Only loads exact Player instances
Set<Player> exact = db.load(Player.class);

// Loads Player, ProPlayer, AmateurPlayer, etc.
Set<Player> all = db.loadAny(Player.class);
```

The same pattern applies to `find`/`findAny`, `findUnique`/`findAnyUnique`, `count`/`countAny`, and `search`/`searchAny`.

### Sorting and Pagination

Pass `Order` and `Page` to control result ordering and pagination:

```java
Set<Player> topTen = db.load(Player.class,
        Order.by("score").descending(),
        Page.sized(10).go(1));
```

### Counting

Count records matching criteria without loading them:

```java
int total = db.count(Player.class);
int highScorers = db.count(Player.class,
        Criteria.where().key("score")
                .operator(Operator.GREATER_THAN)
                .value(100).build());
```

### Deleting Records

Mark a record for deletion, then save to commit:

```java
player.deleteOnSave();
player.save();
```

### Reading and Writing Fields Programmatically

Use `get` to read fields by name and `set` to write them:

```java
Map<String, Object> data = player.get("name", "score");
player.set("score", 99);
player.set(Map.of("name", "New Name", "score", 100));
```

## Concurrency and Atomicity

Runway offers four mechanisms that make concurrent writes safe. Use the narrowest mechanism that covers the situation. A narrower mechanism asks less of the caller, holds less open, and fails in fewer ways.

| Situation | Mechanism |
|---|---|
| You change one field on one record, and a concurrent change to that field must not be lost | `exchange`, `getAndUpdate`, `updateAndGet` |
| Several records must persist together, or not at all | `save(...)`, which is already one ACID transaction |
| You loaded records, edited them in memory, and a concurrent edit must not be silently overwritten | `save(true)` |
| You want the one record that holds a unique identity, and a new record when none holds it | `intern` |
| You want to update whichever record matches a criteria | `findUniqueAndUpdate` and its siblings |
| You read, especially with a `find`, and then write a decision that depends on what you read | `Transaction` |
| You handle a request, a route, or a service call | None of these. Atomicity belongs to the operation, not to the request |

### Single-Key Atomic Operations

Use these when one field on one record changes, and a concurrent change to that same field must not be lost. A counter, a status flag, and a claim on a single value all fit here.

```java
Player player = db.load(Player.class, 42);
boolean swapped = player.exchange("score", 100);
int next = player.updateAndGet("score", (Integer score) -> score + 1);
```

`exchange` is a compare-and-swap. The expected value is the value the instance last observed for that field, so the write lands only if no other writer changed the stored value in the meantime. A `null` in-memory value expects absence. `getAndUpdate` and `updateAndGet` run the same compare-and-swap in a retry loop. They apply your function to the current value and retry on contention, within the bounds of the `AtomicRetryPolicy` on the `Runway` instance. `getAndUpdate` returns the replaced value and `updateAndGet` returns the replacement. When the retries run out, they throw `RetryExhaustedException` and write nothing.

Limits:

- Only a single-value intrinsic field is eligible. Transient fields, `@Unique` fields, collections, arrays, and links are not eligible.
- `exchange` never succeeds against a record that does not exist in the database. Nothing is written in that case.
- These are targeted writes, not saves. The `beforeSave()` hook and the save listeners do not run.
- Runway refuses them on a record that is bound to an open `Transaction`, and on a record that is staged for deletion. `getAndUpdate` and `updateAndGet` also require a record with no unsaved changes.
- The update function can run more than once, so it must be free of side effects.
- One call covers one field on one record. Two fields that must change together are outside its reach.

### Preventing Stale Writes

Use `save(true)` when you loaded records, changed them in memory, and a concurrent edit to that data must not be silently overwritten. This is the read-modify-write case: an edit form, an import that patches existing records, or any flow where two writers can hold the same record at once.

```java
try {
    db.save(true, player, team);
}
catch (StaleDataException e) {
    // Another writer changed a record in the graph. Reload and try again.
}
```

Inside the save's transaction, Runway audits every record in the saved graph, including the linked records it reaches. If any of them changed in the database since it was loaded or last saved, the save throws `StaleDataException` and persists nothing.

A save is already all-or-nothing across the records it touches. `db.save(a, b, c)` commits in a single ACID transaction, and so does `a.save()` for `a` and every record linked from it. "These records must commit together" is therefore not by itself a reason to open a transaction.

Limits:

- Stale-write prevention detects a conflict. It does not merge one. The caller reloads and decides what to do.
- Genuine staleness never retries automatically.
- A commit conflict that no stale root record explains is spurious. Runway retries a spurious conflict automatically, up to an internal attempt limit, when the `SpuriousSaveFailureStrategy` is `RETRY`. The default strategy is `FAIL_FAST`, which propagates the failure instead.
- The audit costs a query for each record in the saved graph, so a large graph pays more.
- This protects the data you loaded. It does not make a `find` and a later write atomic, because the records a `find` did not return are outside the saved graph.

### Find-Then-Write Primitives

Use these when the decision is "create the record if none exists" or "update whichever record matches". They already do the atomic work, so they need no transaction around them.

`intern` returns the one record that agrees with every `@Unique` constraint of the record you pass, and saves that record when none exists:

```java
Player player = new Player();
player.email = "serena@example.com";
player = db.intern(player);
```

`findUniqueAndUpdate` updates one field on the single record that matches a criteria, and returns `null` when nothing matches:

```java
Player updated = db.findUniqueAndUpdate(Player.class,
        Criteria.where().key("email")
                .operator(Operator.EQUALS)
                .value("serena@example.com").build(),
        "score", (Integer score) -> score + 1);
```

The siblings `findAnyUniqueAndUpdate`, `findFirstAndUpdate`, and `findAnyFirstAndUpdate` follow the polymorphic and ordered conventions described above.

In each case the lookup and the write commit as one transaction, and conflicts retry within the bounds of the `AtomicRetryPolicy`. Concurrent callers converge on one record instead of racing to create or update two.

Limits:

- `intern` derives the identity from the `@Unique` constraints, so the record must carry a non-null value under at least one of them. A record that agrees with some but not all of the identity is not a match, and the save then fails `@Unique` enforcement.
- `intern` and the unique variants throw `DuplicateEntryException` when more than one record matches.
- A `find*AndUpdate` call updates one field, under the same eligibility rules as `getAndUpdate`.
- The update operator can run more than once, so it must be free of side effects.
- On `Runway`, `intern` runs in its own transaction, independent of a transaction the caller holds open. Call it on the transaction view to stage it within that transaction instead.

### Transaction

Use a `Transaction` only when a read and a write that depends on it must commit together, and none of the mechanisms above covers the case. The read that makes this necessary is usually a `find`: you search for the records that qualify, decide something from the result, and write that decision.

`run` and `supply` manage the lifecycle for you:

```java
Seat reserved = db.supply(transaction -> {
    Seat seat = transaction.findUnique(Seat.class, criteria);
    seat.taken = true;
    seat.save();
    return seat;
});
```

`stage` (or its alias `startTransaction`) hands you a transaction whose lifecycle you own:

```java
try (Transaction transaction = db.stage()) {
    Seat seat = transaction.findUnique(Seat.class, criteria);
    seat.taken = true;
    seat.save();
    transaction.commit();
}
```

Every read observes the transaction's isolated snapshot, including the transaction's own uncommitted writes, and joins its conflict footprint. If a concurrent writer changes data the transaction read, then the commit fails instead of persisting a decision that no longer holds. Every record loaded or created through the transaction is bound to it, along with the records reachable from it, so each `save()` stages within the transaction and becomes durable only when the commit succeeds. Save and delete notifications fire after the commit.

Limits and costs:

- A transaction is confined to the thread that starts it, and it holds one pooled connection for its whole life.
- `run` and `supply` replay their work when the commit conflicts, so that work must be idempotent. A transaction from `stage` never retries on its own, because a retry would mean replaying the caller's own code.
- A read joins the conflict footprint only when it runs through the transaction. A read on a record that is bound elsewhere does not.
- A `find` reads an index range, so a concurrent write anywhere in that range can fail the commit. A load by id is scoped to that one record, and writes to other records do not affect it.
- Enforcement of a `@Unique` constraint performs a `find` as part of the save. A save of a record with a `@Unique` field therefore carries a range read, and holding such a save inside a long transaction widens the exposure to conflict.
- Do not wrap a request, a route, or a service call in a transaction. Scope it to the read and the write that must agree.

## Record Linking

Fields whose type is another `Record` subclass are automatically stored as Links in Concourse. When a Record is loaded, its linked Records are loaded too.

```java
public class Team extends Record {
    public String name;
    public Set<Player> roster = new LinkedHashSet<>();
}
```

### Deferred References

Use `DeferredReference<T>` for lazy loading. The linked Record is only loaded from the database when `get()` is called, improving performance for large object graphs:

```java
public class Team extends Record {
    public DeferredReference<Coach> coach;
}

// Later
Coach coach = team.coach.get();
```

### Delete Hooks

Annotations control what happens to linked Records when a Record is deleted:

| Annotation | Behavior |
|------------------|---------------------------------------------------|
| `@CascadeDelete` | Deleting this record also deletes the linked record |
| `@JoinDelete` | Deleting the linked record also deletes this record |
| `@CaptureDelete` | Deleting the linked record nullifies the reference |

```java
public class Blog extends Record {
    @CascadeDelete
    public Set<Comment> comments;   // Deleting blog deletes comments

    @JoinDelete
    public Author author;           // Deleting author deletes this blog

    @CaptureDelete
    public Category category;       // Deleting category sets this to null
}
```

## Derived and Computed Properties

Add virtual properties to Records that are included in `json()` and `map()` output but are not stored in the database.

### @Derived

Derived properties are lightweight, cached computations based on intrinsic fields:

```java
public class Player extends Record {
    public String firstName;
    public String lastName;

    @Derived
    public String fullName() {
        return firstName + " " + lastName;
    }
}
```

### @Computed

Computed properties are recalculated on every access and are never cached. Use them for expensive or time-sensitive computations:

```java
public class Player extends Record {
    @Computed
    public int ranking() {
        // Expensive calculation that should always be fresh
        return calculateGlobalRanking();
    }
}
```

Both annotations accept an optional `value` parameter to customize the property name:

```java
@Derived("full_name")
public String fullName() { ... }
```

## Realms

Realms provide logical data segregation within a single Concourse environment. A Record can belong to multiple realms simultaneously, and records with no realm assignment are visible in all realms.

### Assigning Realms

```java
player.addRealm("east-coast");
player.addRealm("all-star");
player.removeRealm("east-coast");
Set<String> realms = player.realms();
```

### Querying by Realm

Pass a `Realms` matcher to any query method:

```java
// Only load players in the "east-coast" realm
Set<Player> eastern = db.load(Player.class,
        Realms.only("east-coast"));

// Load players in either realm
Set<Player> selected = db.load(Player.class,
        Realms.anyOf("east-coast", "west-coast"));

// Load from all realms (default behavior)
Set<Player> everyone = db.load(Player.class, Realms.any());
```

## Access Control (Audience Framework)

Runway includes a built-in access control framework that governs how different users (called Audiences) interact with Records. This is activated by having a Record implement the `AccessControl` interface and having your user type implement the `Audience` interface.

### Defining an Access-Controlled Record

```java
public class Document extends Record implements AccessControl {

    public String title;
    private String content;
    private Player owner;

    @Override
    public boolean $isCreatableBy(Audience audience) {
        return true; // Any authenticated user can create
    }

    @Override
    public boolean $isCreatableByAnonymous() {
        return false;
    }

    @Override
    public boolean $isDiscoverableBy(Audience audience) {
        return true; // Anyone can see it exists
    }

    @Override
    public boolean $isDiscoverableByAnonymous() {
        return true;
    }

    @Override
    public boolean $isDeletableBy(Audience audience) {
        return audience.equals(owner);
    }

    @Override
    public Set<String> $readableBy(Audience audience) {
        if(audience.equals(owner)) {
            return ALL_KEYS;  // Owner reads everything
        }
        else {
            return ImmutableSet.of("title");  // Others see title only
        }
    }

    @Override
    public Set<String> $readableByAnonymous() {
        return ImmutableSet.of("title");
    }

    @Override
    public Set<String> $writableBy(Audience audience) {
        if(audience.equals(owner)) {
            return ALL_KEYS;
        }
        else {
            return NO_KEYS;
        }
    }

    @Override
    public Set<String> $writableByAnonymous() {
        return NO_KEYS;
    }
}
```

### Defining an Audience

An Audience is a Record that can perform operations on other records, subject to access rules. Implement the `Audience` interface on a Record type:

```java
public class User extends Record implements Audience {
    public String name;

    @Unique
    @Required
    protected String email;
}
```

### Performing Access-Controlled Operations

Once configured, database operations routed through an `Audience` automatically enforce the access rules:

```java
User user = db.load(User.class, userId);

// Load — returns null if user can't discover the document
Document doc = user.load(Document.class, docId);

// Find — only returns documents visible to the user
Set<Document> docs = user.find(Document.class, criteria);

// Read — throws RestrictedAccessException if denied
Object title = user.read("title", doc);

// Write — throws RestrictedAccessException if denied
user.write("title", "New Title", doc);

// Frame — returns only the fields the user can see (no exception)
Map<String, Object> data = user.frame(doc);

// Create — throws RestrictedAccessException if denied
Document newDoc = user.create(Document.class);

// Delete — throws RestrictedAccessException if denied
user.delete(doc);
```

Access-controlled operations can also be invoked from the record's perspective:

```java
doc.readAs(user, "title");
doc.writeAs(user, "content", "Updated content");
doc.frameAs(user);
doc.deleteAs(user);
```

### Anonymous Access

For unauthenticated contexts, use the anonymous audience:

```java
Audience anon = Audience.anonymous();
Set<Document> publicDocs = anon.find(Document.class, criteria);
```

### Access Rule Constants

| Constant   | Meaning |
|------------|------------------------------------------|
| `ALL_KEYS` | Access to every field on the record |
| `NO_KEYS`  | No access to any field |

You can also return a specific set of field names, or use negative rules (prefix with `-`) to deny specific fields while allowing all others.

### Visibility Scopes

By default, `$isDiscoverableBy` is evaluated per-instance — every record must be loaded and checked individually. For large datasets where only a fraction of records are visible to a given audience, this is expensive and causes problems with pagination and count accuracy.

A `Scope` expresses which records of a type are visible to an audience as a class-level declaration rather than a per-instance evaluation. When registered, a Scope pushes visibility constraints directly to the database, so only matching records are returned.

#### Scope Variants

| Factory Method | Behavior |
|-------------------------|-----------------------------------------------|
| `Scope.of(criteria)` | Visibility expressed as a `Criteria` pushed into the database query |
| `Scope.unrestricted()` | Audience sees all records; no filter applied |
| `Scope.none()` | Audience sees no records; short-circuits immediately |
| `Scope.unsupported()` | Cannot be expressed as a constraint; falls back to per-instance checking |

#### Registering Scopes

Register a Scope provider for a single class:

```java
AccessControl.registerVisibilityScope(Document.class, audience -> {
    if(audience.equals(adminUser)) {
        return Scope.unrestricted();
    }
    else {
        User user = (User) audience;
        return Scope.of(Criteria.where().key("owner")
                .operator(Operator.EQUALS)
                .value(user.name).build());
    }
});
```

Register for an entire type hierarchy:

```java
AccessControl.registerVisibilityScopeHierarchy(
        Document.class, audience -> Scope.unrestricted());
```

Once registered, Scopes are applied automatically when the audience performs queries — no changes to query code are needed.

#### When to Use Scopes

Start with instance-based permissions (`$isDiscoverableBy`), which are simpler to reason about. Introduce a Scope when:

- Queries return large datasets but only a small visible subset
- Pagination results are incorrect because client-side filtering reduces page sizes unpredictably
- Count queries need to reflect the visible subset accurately

## Metadata

Implement the `Metadata` interface on a Record to gain computed temporal properties:

```java
public class Player extends Record implements Metadata {
    public String name;
}

// After saving and reloading
Timestamp created = player.createdAt();
Timestamp updated = player.lastUpdatedAt();
Timestamp nameUpdated = player.lastUpdatedAt("name");
```

These properties are `@Computed` and are never cached — they query the audit log on every access.

## Serialization

Records provide `json()` and `map()` methods for serialization.

### JSON Output

```java
String json = player.json();
String partial = player.json("name", "score");
```

### Map Output

```java
Map<String, Object> data = player.map();
Map<String, Object> partial = player.map("name", "score");
```

### Serialization Options

Customize serialization behavior with `SerializationOptions`:

```java
SerializationOptions options = SerializationOptions.builder()
        .flattenSingleElementCollections(true)
        .serializeNullValues(true)
        .build();

String json = player.json(options);
Map<String, Object> data = player.map(options, "name", "score");
```

### Field Visibility in Serialization

| Modifier | Stored in DB | Appears in `json()`/`map()` |
|-----------|--------------|------------------------------|
| `public` | Yes | Yes |
| `protected` | Yes | Yes |
| `private` | Yes | No (unless `@Readable`) |
| `transient` | No | No |

## Lifecycle Hooks

### beforeSave()

Override `beforeSave()` in your Record subclass to perform logic before data is persisted. This is useful for computing derived values, validating business rules, or setting defaults:

```java
public class Player extends Record {
    public String name;
    public String nameNormalized;

    @Override
    protected void beforeSave() {
        nameNormalized = name.toLowerCase().trim();
    }
}
```

### Save Listeners

Register asynchronous listeners on the `Runway` builder that fire after a Record is successfully saved:

```java
Runway db = Runway.builder()
        .onSave(Player.class, player -> {
            System.out.println("Player saved: " + player.id());
        })
        .onSave(Record.class, record -> {
            // Fires for ALL record types
            audit(record);
        })
        .build();
```

Listeners are type-filtered (including subclasses), compositional (multiple listeners can be registered), and execute asynchronously in a dedicated thread.

## The `@Readable` Annotation

Mark a private field as `@Readable` to include it in `json()`, `map()`, and `get()` output while keeping the field encapsulated:

```java
public class User extends Record {
    @Readable
    private Timestamp joinDate;

    private String passwordHash;  // Not readable, truly private
}
```

## Load Failure Handling

Register a handler for load failures via the builder:

```java
Runway db = Runway.builder()
        .onLoadFailure((clazz, recordId, error) -> {
            logger.error("Failed to load {} #{}: {}",
                    clazz.getSimpleName(), recordId, error);
        })
        .build();
```

## Summary

| Feature | Mechanism |
|-----------------------------|------------------------------------------|
| Schema definition | Non-transient member variables |
| Constraints | `@Required`, `@Unique`, `@ValidatedBy` |
| Record linking | `Record`-typed fields, `DeferredReference` |
| Delete propagation | `@CascadeDelete`, `@JoinDelete`, `@CaptureDelete` |
| Virtual properties | `@Derived`, `@Computed` |
| Multi-tenancy | `Realms` |
| Access control | `AccessControl` + `Audience` |
| Concurrency and atomicity | `exchange`, `save(true)`, `intern`, `Transaction` |
| Temporal metadata | `Metadata` interface |
| Serialization | `json()`, `map()`, `SerializationOptions` |
| Lifecycle hooks | `beforeSave()`, `onSave()` listeners |
| Field visibility | Access modifiers, `@Readable` |
