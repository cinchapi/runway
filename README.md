Runway
======

Runway is the official ORM (Object-Record Mapping) framework for Concourse. Runway provides a framework for persisting simple POJO-like objects to Concourse while automatically preserving transactional security.

Persistable types must extend the `Record` class. Like Concourse, Runway doesn't insist on being given an explicit schema for Records. The class definition and some optional annotations are all that are required to define the storage schema. By default, Runway respects Java access rules. For example, private fields are saved but never exposed and transient fields are not stored in the database.

Java inheritence is supported, so member variables extend to subclasses.

Runway intelligently detects variable types and stores the correct data in Concourse. For example, variables that are instances of the `Record` class are stored in Concourse as links. Variables that hold arrays or collections have each unique value stored individually for the appropriate key in the record. Finally, variables that are non-primitive serializable types have their serializable forms stored.

### Constraints
You can use annotations to declare optional database contraints on member variables within the class. Runway checks for these annotations when saving records and enforces the appropriate constraint.
  * `Required` - the record cannot be saved if the value of the variable is `null` or an empty collection/array
  * `Unique` - the record cannot be saved if another record in the class has the same value
  * `ValidatedBy` - the record cannot be saved in the value does not pass the specified validator

### Creating a Record type
Each Record type should extend the `Record` class. Once the class is created, you can define the schema using the member variables in the class. All non-transient variables are stored in the database. All non-private variables are automatically printed out when creating a `dump` or `toString` representation of the record.

Each Record has a unique `id` that is automatically assigned during creation. You can get the id for any record using the `getId()` method.

```
public class User extends Record {

    /**
     * Login and load the appropriate user instance that has the {@code email}
     * and {@code password} login information. If those credentials do not
     * identify a user, then return null.
     *
     * @param email
     * @param password
     * @return the User instance or {@code null} if the credentials are not
     *         correct.
     */
    @Nullable
    public static User login(String email, String password) {
        try {
            User user = Iterables.getOnlyElement(Record.findEvery(User.class,
                    Criteria.where().key("email").operator(Operator.EQUALS)
                            .value(email).build()));
            if(user.password.equals(Secrets.getExpectedPasswordHash(email,
                    password, user.salt, user.rounds))) {
                return user;
            }
            else {
                return null;
            }
        }
        catch (NoSuchElementException | IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * The user's human readable name
     */
    public String name;

    /**
     * The email account associated with the user account that also functions as
     * a username for login purposes. This is a unique key within the database.
     */
    @Unique
    @Required
    @ValidatedBy(EmailValidator.class)
    protected String email;

    /**
     * The hashed password.
     */
    @Unique
    @Required
    private String password;

    /**
     * The number of rounds user to hash the {@link #salt}.
     */
    @Required
    private int rounds = 0;

    /**
     * The salt used when hashing the password.
     */
    @Required
    private String salt = null;

    /**
     * Return the email address for this user. To set this value, use the
     * {@link #setLoginInfo(String, String)} method so that the password hash is
     * appropriately updated.
     *
     * @return the email address
     */
    public String getEmail() {
        return email;
    }

    /**
     * Set the login information for this user.
     *
     * @param email
     * @param password
     */
    public void setLoginInfo(String email, String password) {
        initSecurityData();
        this.email = email;
        this.password = Secrets.getExpectedPasswordHash(email, password, salt,
                rounds);
    }

    /**
     * Initialize the data that is necessary to perform security functions (e.g.
     * assign a number of hash rounds and select a random salt).
     */
    private void initSecurityData() {
        if(rounds == 0 || Strings.isNullOrEmpty(salt)) {
            rounds = Secrets.SRAND.nextInt(10) + 1;
            salt = BaseEncoding.base64().encode(
                    Longs.toByteArray(Crypt.srand.nextLong()));
        }
    }

}
```
#### Creating a Record instance
You can create a Record instance by calling the class-defined constructor.

#### Loading a Record instance
Use the static `load` method to load a previously stored Record instance. You will need to pass along the class of the Record type you wish to load and the `id` of the record.
```
Runway db = Runway.connect();
User user = db.load(User.class, 1);
```

### Saving a Record
Once you have made changes to a record, you need to save it before those changes are persisted to Concourse. Every record exposes a `save()` method that calculates diffs and stores those to the database. If the save succeeds, the method returns `true`. Otherwise, it returns `false`, which usually means that a constraint was violated.
```
user.name = "Jeff Nelson";
user.setLoginInfo("jeff@cinchapi.org", "ihatesecurity");
user.save();
```

If a record fails to save for some reason, you can always throw an Exception with a complete stacktrace of the failure's cause using the `throwSuppressedExceptions()` method.

### Saving multiple Records at once
The `save()` method saves changes to a record within an ACID transaction to ensure that the state remains consistent. You can save multiple records within a single transaction using the `Record.saveAll()` method. This is useful in situations when changes to multiple records are dependant or newly created records link to one another.
```
Runway db = Runway.connect()
User user1 = User.create();
User user2 = User.create();
user1.followers.add(user2);
db.save(user1, user2);
```
