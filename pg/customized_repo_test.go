package pg

import (
	"database/sql"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/meowmeowcode/hohin"
	"github.com/meowmeowcode/hohin/sqldb"
	"reflect"
	"testing"
)

type Contact struct {
	Pk     uuid.UUID
	Name   string
	Emails []string
}

func (c *Contact) Equal(c2 *Contact) bool {
	return reflect.DeepEqual(c, c2)
}

func makeContactsRepo() hohin.SimpleRepo[Contact] {
	return NewRepo(Conf[Contact]{
		Table: "contacts",
		Mapping: map[string]string{
			"Pk":   "id",
			"Name": "name",
		},
		Query: `
SELECT * FROM (SELECT contacts.id, contacts.name, array_agg(emails.email) AS emails
FROM contacts
LEFT JOIN emails ON emails.contact_id = contacts.id
GROUP BY contacts.id, contacts.name) AS query
        `,
		Load: func(row Scanner) (Contact, error) {
			var entity Contact
			err := row.Scan(&entity.Pk, &entity.Name, (*pq.StringArray)(&entity.Emails))
			return entity, err
		},
		AfterAdd: func(c Contact) []*sqldb.Sql {
			qs := make([]*sqldb.Sql, 0)
			for _, e := range c.Emails {
				q := NewSql("INSERT INTO emails (id, email, contact_id) VALUES (").
					JoinParams(", ", uuid.New(), e, c.Pk).
					Add(")")
				qs = append(qs, q)
			}
			return qs
		},
		AfterUpdate: func(c Contact) []*sqldb.Sql {
			qs := []*sqldb.Sql{NewSql("DELETE FROM emails WHERE contact_id = ").Param(c.Pk)}

			for _, e := range c.Emails {
				q := NewSql("INSERT INTO emails (id, email, contact_id) VALUES (").
					JoinParams(", ", uuid.New(), e, c.Pk).
					Add(")")
				qs = append(qs, q)
			}
			return qs
		},
	}).Simple()
}

func makeContactsDb() hohin.SimpleDb {
	pool, err := sql.Open("postgres", "user=hohin dbname=hohin password=hohin sslmode=disable")
	if err != nil {
		panic(err)
	}
	_, err = pool.Exec(`
CREATE TABLE IF NOT EXISTS contacts (
    id uuid PRIMARY KEY,
    name text NOT NULL
)
    `)
	if err != nil {
		panic(err)
	}
	_, err = pool.Exec(`
CREATE TABLE IF NOT EXISTS emails (
    id uuid PRIMARY KEY,
    email text NOT NULL UNIQUE,
    contact_id uuid REFERENCES contacts(id) ON DELETE CASCADE
)
    `)
	_, err = pool.Exec(`DELETE FROM contacts`)
	if err != nil {
		panic(err)
	}
	_, err = pool.Exec(`DELETE FROM emails`)
	if err != nil {
		panic(err)
	}
	return NewDb(pool).Simple()
}

func TestOneToMany(t *testing.T) {
	db := makeContactsDb()
	repo := makeContactsRepo()
	bob := Contact{Pk: uuid.New(), Name: "Bob", Emails: []string{"bob@test.com", "bob123@test.com"}}
	err := repo.Add(db, bob)
	if err != nil {
		t.Fatal(err)
	}
	alice := Contact{Pk: uuid.New(), Name: "Alice", Emails: []string{"alice@test.com"}}
	err = repo.Add(db, alice)
	if err != nil {
		t.Fatal(err)
	}
	b, err := repo.Get(db, hohin.Eq("Name", "Bob"))
	if err != nil {
		t.Fatal(err)
	}
	if !b.Equal(&bob) {
		t.Fatalf("%v != %v", b, bob)
	}
	a, err := repo.Get(db, hohin.Eq("Name", "Alice"))
	if err != nil {
		t.Fatal(err)
	}
	if !a.Equal(&alice) {
		t.Fatalf("%v != %v", a, alice)
	}
}
