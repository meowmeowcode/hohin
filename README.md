# Hōhin

Hohin is a database toolkit that contains generic implementations of the Repository pattern.

## Supported database systems

At the moment, ClickHouse, MySQL, PostgreSQL, and SQLite3 are supported.

## Usage example

```go
package example

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "github.com/google/uuid"
    _ "github.com/mattn/go-sqlite3"
    "github.com/meowmeowcode/hohin"
    "github.com/meowmeowcode/hohin/sqldb"
    "github.com/meowmeowcode/hohin/sqlite3"
)

func Example() {
    // Suppose we have this entity in our application:
    type User struct {
        Id   uuid.UUID
        Name string
    }

    // We need to connect to a database and create a table for this entity:
    pool, err := sql.Open("sqlite3", ":memory:")
    if err != nil {
        panic(err)
    }
    defer pool.Close()

    _, err = pool.Exec(`
        CREATE TABLE users (
            Id uuid PRIMARY KEY,
            Name text NOT NULL
        )
    `)
    if err != nil {
        panic(err)
    }

    // Everything is set up. Let's see what we can do now.

    // Creating a repository:
    usersRepo := sqlite3.NewRepo(sqlite3.Conf[User]{Table: "users"}).Simple()

    // Saving an entity:
    db := sqlite3.NewDB(pool).Simple()
    alice := User{Id: uuid.New(), Name: "Alice"}
    err = usersRepo.Add(db, alice)
    if err != nil {
        panic(err)
    }

    // Saving several entities:
    bob := User{Id: uuid.New(), Name: "Bob"}
    eve := User{Id: uuid.New(), Name: "Eve"}
    err = usersRepo.AddMany(db, []User{bob, eve})
    if err != nil {
        panic(err)
    }

    // Loading an entity:
    user, err := usersRepo.Get(db, hohin.Eq("Name", "Alice"))
    if err != nil {
        panic(err)
    }
    fmt.Println(user == alice)

    user, err = usersRepo.Get(db, hohin.Contains("Name", "o"))
    if err != nil {
        panic(err)
    }
    fmt.Println(user == bob)

    user, err = usersRepo.Get(
        db,
        hohin.And(hohin.HasSuffix("Name", "e"), hohin.HasPrefix("Name", "E")),
    )
    if err != nil {
        panic(err)
    }
    fmt.Println(user == eve)

    // Loading several entities:
    users, err := usersRepo.GetMany(
        db,
        hohin.Query{Filter: hohin.HasSuffix("Name", "e")}.OrderBy(hohin.Asc("Name")),
    )
    if err != nil {
        panic(err)
    }
    fmt.Println(len(users) == 2)
    fmt.Println(users[0] == alice)
    fmt.Println(users[1] == eve)

    // Updating an entity:
    bob.Name = "Robert"
    err = usersRepo.Update(db, hohin.Eq("Id", bob.Id), bob)
    if err != nil {
        panic(err)
    }
    user, err = usersRepo.Get(db, hohin.Eq("Id", bob.Id))
    if err != nil {
        panic(err)
    }
    fmt.Println(user.Name == "Robert")

    // Removing an entity:
    err = usersRepo.Delete(db, hohin.Eq("Name", "Robert"))
    if err != nil {
        panic(err)
    }

    // Using transactions:
    err = db.Transaction(func(db hohin.SimpleDB) error {
        alice, err := usersRepo.GetForUpdate(db, hohin.Eq("Name", "Alice"))
        if err != nil {
            return err
        }
        eve, err := usersRepo.GetForUpdate(db, hohin.Eq("Name", "Eve"))
        if err != nil {
            return err
        }
        alice.Name = "Eve"
        eve.Name = "Alice"
        err = usersRepo.Update(db, hohin.Eq("Id", alice.Id), alice)
        if err != nil {
            return err
        }
        err = usersRepo.Update(db, hohin.Eq("Id", eve.Id), eve)
        if err != nil {
            return err
        }
        return nil
    })
    if err != nil {
        panic(err)
    }
    user, err = usersRepo.Get(db, hohin.Eq("Id", alice.Id))
    if err != nil {
        panic(err)
    }
    fmt.Println(user.Name == "Eve")

    // Using a context:
    usersRepo2 := sqlite3.NewRepo(sqlite3.Conf[User]{Table: "users"})
    db2 := sqlite3.NewDB(pool)
    user, err = usersRepo2.Get(context.Background(), db2, hohin.Eq("Name", "Alice"))
    if err != nil {
        panic(err)
    }

    // Configuring a repository:
    //
    // For this example we need another entity and a couple of tables:
    type Contact struct {
        Id     uuid.UUID
        Name   string
        Emails []string
    }

    _, err = pool.Exec(`
        CREATE TABLE contacts (
            pk uuid PRIMARY KEY,
            name text NOT NULL
        )
    `)
    if err != nil {
        panic(err)
    }

    _, err = pool.Exec(`
        CREATE TABLE emails (
            pk uuid PRIMARY KEY,
            email text NOT NULL,
            contact_pk text NOT NULL
        )
    `)
    if err != nil {
        panic(err)
    }

    contactsRepo := sqlite3.NewRepo(sqlite3.Conf[Contact]{
        Table: "contacts",
        Mapping: map[string]string{
            "Id":   "pk",
            "Name": "name",
        },
        Query: `
            SELECT * FROM (
                SELECT contacts.pk, contacts.name, json_group_array(emails.email) AS emails
                FROM contacts
                LEFT JOIN emails ON emails.contact_pk = contacts.pk
                GROUP BY contacts.pk, contacts.name
            ) AS query
        `,
        Load: func(s sqlite3.Scanner) (Contact, error) {
            var entity Contact
            var emailsData string
            err := s.Scan(&entity.Id, &entity.Name, &emailsData)
            err = json.Unmarshal([]byte(emailsData), &entity.Emails)
            return entity, err
        },
        AfterAdd: func(c Contact) []*sqldb.SQL {
            var qs []*sqldb.SQL
            for _, e := range c.Emails {
                q := sqlite3.NewSQL("INSERT INTO emails (pk, email, contact_pk) VALUES (").
                    JoinParams(", ", uuid.New(), e, c.Id).
                    Add(")")
                qs = append(qs, q)
            }
            return qs
        },
        AfterUpdate: func(c Contact) []*sqldb.SQL {
            var qs []*sqldb.SQL
            qs = append(qs, sqlite3.NewSQL("DELETE FROM emails WHERE contact_pk = ").Param(c.Id))

            for _, e := range c.Emails {
                q := sqlite3.NewSQL("INSERT INTO emails (id, email, contact_pk) VALUES (").
                    JoinParams(", ", uuid.New(), e, c.Id).
                    Add(")")
                qs = append(qs, q)
            }
            return qs
        },
    }).Simple()

    contact := Contact{Id: uuid.New(), Name: "Bob", Emails: []string{"bob@test.com", "bob123@test.com"}}
    err = contactsRepo.Add(db, contact)
    if err != nil {
        panic(err)
    }

    contact = Contact{Id: uuid.New(), Name: "Alice", Emails: []string{"alice@test.com"}}
    err = contactsRepo.Add(db, contact)
    if err != nil {
        panic(err)
    }

    contact, err = contactsRepo.Get(db, hohin.Eq("Name", "Bob"))
    if err != nil {
        panic(err)
    }
    fmt.Println(contact.Name, contact.Emails)

    contact, err = contactsRepo.Get(db, hohin.Eq("Name", "Alice"))
    if err != nil {
        panic(err)
    }
    fmt.Println(contact.Name, contact.Emails)

    // Output:
    // true
    // true
    // true
    // true
    // true
    // true
    // true
    // true
    // Bob [bob@test.com bob123@test.com]
    // Alice [alice@test.com]
}
```
