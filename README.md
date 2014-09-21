runway
======

Runway is the official ORM (Object-Record Mapping) framework for Concourse. Runway provides a framework for persisting simple POJO-like objects to Concourse while automatically preserving transactional security.

Each class represents a different "section" within Concourse and defines a common storage schema with its non-transient member variables. Java inheritence is supported, so member variables extend to subclasses. Runway intelligently detects variable types and stores the correct data in Concourse. For example, variables that are instances of the `Record` class are stored in Concourse as links. Variables that hold arrays or collections have each unique value stored individually for the appropriate key in the record. Finally, variables that are non-primitive serializable types have their serializable forms stored.

### Constraints
You can use annotations to declare database contraints on member variables within the class. Runway checks for these annotations when saving records and enforces the appropriate constraint.
  * `Required` - the record cannot be saved if the value of the variable is `null` or an empty collection/array
  * `Unique` - the record cannot be saved if another record in the class has the same value
  * `ValidatedBy` - the record cannot be saved in the value does not pass the specified validator

### Creating a Record type
Each Record type should extend the `Record` class. Once the class is created, you can define the schema using the member variables in the class. All non-transient variables are stored in the database. All non-private variables are automatically printed out when creating a `dump` or `toString` representation of the record.

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
