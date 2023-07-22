package pg

import (
	"database/sql"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/meowmeowcode/hohin"
	"reflect"
	"testing"
)

type Contact struct {
	Id     uuid.UUID
	Name   string
	Emails []string
}

func (c *Contact) Equal(c2 *Contact) bool {
	return reflect.DeepEqual(c, c2)
}

func makeContactsRepo() *Repo[Contact] {
	return NewRepo(Conf[Contact]{
		Table: "contacts",
		Mapping: map[string]string{
			"Id":   "id",
			"Name": "name",
		},
		Query: `
SELECT contacts.id, contacts.name, array_agg(emails.email) AS emails
FROM contacts
LEFT JOIN emails ON emails.contact_id = contacts.id
GROUP BY contacts.id, contacts.name
        `,
		Load: func(row Scanner) (Contact, error) {
			var entity Contact
			err := row.Scan(&entity.Id, &entity.Name, (*pq.StringArray)(&entity.Emails))
			return entity, err
		},
		AfterAdd: func(c Contact) []*Sql {
			qs := make([]*Sql, 0)
			for _, e := range c.Emails {
				q := NewSql("INSERT INTO emails (id, email, contact_id) VALUES (").
					AddParamsSep(", ", uuid.New(), e, c.Id).
					Add(")")
				qs = append(qs, q)
			}
			return qs
		},
		AfterUpdate: func(c Contact) []*Sql {
			qs := []*Sql{NewSql("DELETE FROM emails WHERE contact_id = ").AddParam(c.Id)}

			for _, e := range c.Emails {
				q := NewSql("INSERT INTO emails (id, email, contact_id) VALUES (").
					AddParamsSep(", ", uuid.New(), e, c.Id).
					Add(")")
				qs = append(qs, q)
			}
			return qs
		},
	})
}

func makeContactsDb() hohin.Db {
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
	return NewDb(pool)
}

func TestOneToMany(t *testing.T) {
	db := makeContactsDb()
	repo := makeContactsRepo()
	bob := Contact{Id: uuid.New(), Name: "Bob", Emails: []string{"bob@test.com", "bob123@test.com"}}
	err := repo.Add(db, bob)
	if err != nil {
		t.Fatal(err)
	}
	alice := Contact{Id: uuid.New(), Name: "Alice", Emails: []string{"alice@test.com"}}
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
