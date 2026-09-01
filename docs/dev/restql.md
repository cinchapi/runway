# RestQL Spec (Condensed)

RestQL is the spec that Cinchapi HTTP servers follow for query
parameters. This file condenses the canonical documentation in the
private https://github.com/cinchapi/restql repo. Servers parse the
parameters with `RestQuery` (library `com.cinchapi:restql`); never
hand-roll a parser.

## Parameters

| Parameter | Purpose | Examples |
|-----------|---------|----------|
| `select` | Field projection | `select=name,age`; `select=user.{name,email}` |
| `where` | Filter criteria | `where=name='John'`; `where.age.gte=21` |
| `order` / `sort` | Sort order | `order=-name`; `order.name=desc` |
| `page` | Page number (1-based) | `page=2` |
| `size` | Page size | `size=25` |
| `limit` | Max records | `limit=50` |
| `skip` / `offset` | Records to skip | `skip=100` |
| `search.<field>` | Full-text search | `search.description=keyword` |

## Where

| Format | Example | Meaning |
|--------|---------|---------|
| CCL expression | `where=name='John' AND age>21` | Direct CCL |
| Implied equals | `where.name=John` | `name = John` |
| Explicit operator | `where.age.gte=21` | `age >= 21` |
| With conjunction | `where.or.status=active` | `OR status = active` |
| Navigation key | `where.user.role=admin` | `user.role = admin` |

- CCL is the Concourse Criteria Language.
- Multiple `where` parameters combine with `AND` (`where.and.*`) or
  `OR` (`where.or.*`); with no conjunction, `AND` is the default.
- A missing operator means `=`.
- If a field name collides with an operator or conjunction keyword
  (`and`, `or`, `gt`, `like`, ...), use full CCL instead:
  `?where=and = "some value"`.

## Order

| Syntax | Example | Result |
|--------|---------|--------|
| Simple ascending | `order=name` | name ASC |
| Minus prefix | `order=-name` | name DESC |
| Mixed | `order=name,-age` | name ASC, age DESC |
| Suffix style | `order.desc=name` | name DESC |
| Field-value style | `order.name=desc` | name DESC |

- `sort` is an alias for `order`. Parameters chain:
  `order.asc=firstName&order.desc=lastName`.

## Pagination

| Style | Parameters | Example |
|-------|------------|---------|
| Page-based | `page`, `size` | `page=2&size=20` (records 21-40) |
| Offset-based | `limit`, `skip` | `limit=20&skip=40` (records 41-60) |

- `offset` is an alias for `skip`.
- When both styles are present, `limit`/`skip` wins.
- `page` without `size` uses a default size; `size` without `page`
  limits from the start; `skip` without `limit` uses a default limit
  of 20.
- `skip` converts to a page number and rounds down to the nearest
  page boundary; use skip values that are multiples of the limit.
- `page`, `size`, and `limit` must be >= 1; `skip`/`offset` must be
  >= 0. Invalid values throw `IllegalArgumentException`; non-numeric
  values throw `NumberFormatException`. Surface both as HTTP 400.

## Search

- `search.<field>=text` performs a full-text or fuzzy match on one
  field. It is distinct from `where`, which handles structured,
  operator-based constraints.

## Nested Field Selection (Sub-Documents)

- Dot notation reaches nested fields: `parent.child.field`.
- Bracket notation selects several nested fields at once:
  `select=user.{name,email,address.{city,zip}}` expands to
  `user.name`, `user.email`, `user.address.city`, `user.address.zip`.
- The server must handle partial expansion of sub-documents when it
  applies the projection.

## Server-Side Parsing

```java
RestQuery query = new RestQuery(entries); // Map.Entry<String, Object>

Set<String> select = query.select();
Criteria where = query.where();
Order order = query.order();
Page page = query.page();
Map<String, Collection<String>> search = query.search();
```

- Extend `ForwardingRestQuery` to decorate a query without changing
  its source: add default `select` fields, or force constraints such
  as a tenant filter into `where()`.
