package mysql

import (
	"database/sql"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/meowmeowcode/hohin"
	"github.com/meowmeowcode/hohin/sqldb"
	"reflect"
	"sort"
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
SELECT * FROM (SELECT contacts.id, contacts.name, json_arrayagg(emails.email) AS emails
FROM contacts
LEFT JOIN emails ON emails.contact_id = contacts.id
GROUP BY contacts.id, contacts.name) AS query
        `,
		Load: func(row Scanner) (Contact, error) {
			var entity Contact
			var emailsData string
			err := row.Scan(&entity.Pk, &entity.Name, &emailsData)
			if err != nil {
				return entity, err
			}
			err = json.Unmarshal([]byte(emailsData), &entity.Emails)
			sort.Strings(entity.Emails)
			return entity, err
		},
		AfterAdd: func(c Contact) []*sqldb.SQL {
			qs := make([]*sqldb.SQL, 0)
			for _, e := range c.Emails {
				q := NewSQL("INSERT INTO emails (id, email, contact_id) VALUES (").
					JoinParams(", ", uuid.New(), e, c.Pk).
					Add(")")
				qs = append(qs, q)
			}
			return qs
		},
		AfterUpdate: func(c Contact) []*sqldb.SQL {
			qs := []*sqldb.SQL{NewSQL("DELETE FROM emails WHERE contact_id = ").Param(c.Pk)}

			for _, e := range c.Emails {
				q := NewSQL("INSERT INTO emails (id, email, contact_id) VALUES (").
					JoinParams(", ", uuid.New(), e, c.Pk).
					Add(")")
				qs = append(qs, q)
			}
			return qs
		},
	}).Simple()
}

func TestCustomizedRepo(t *testing.T) {
	pool, err := sql.Open("mysql", "hohin:hohin@/hohin?parseTime=true")
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	_, err = pool.Exec(`
CREATE TABLE IF NOT EXISTS contacts (
    Id char(36) PRIMARY KEY,
    name varchar(36) NOT NULL
)
    `)
	if err != nil {
		panic(err)
	}
	defer func() {
		if _, err := pool.Exec(`DROP TABLE IF EXISTS contacts`); err != nil {
			panic(err)
		}
	}()

	_, err = pool.Exec(`
CREATE TABLE IF NOT EXISTS emails (
    Id char(36) PRIMARY KEY,
    email varchar(100) NOT NULL UNIQUE,
    contact_id char(36) REFERENCES contacts(id) ON DELETE CASCADE
)
    `)
	if err != nil {
		panic(err)
	}
	defer func() {
		if _, err := pool.Exec(`DROP TABLE IF EXISTS emails`); err != nil {
			panic(err)
		}
	}()

	cleanDB := func() {
		if _, err := pool.Exec(`DELETE FROM contacts`); err != nil {
			panic(err)
		}
		if _, err := pool.Exec(`DELETE FROM emails`); err != nil {
			panic(err)
		}
	}

	db := NewDB(pool).Simple()
	repo := makeContactsRepo()

	t.Run("TestOneToMany", func(t *testing.T) {
		cleanDB()
		bob := Contact{Pk: uuid.New(), Name: "Bob", Emails: []string{"bob123@test.com", "bob@test.com"}}
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
	})
}
