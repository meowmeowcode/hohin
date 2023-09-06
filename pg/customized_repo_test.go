package pg

import (
	"context"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
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
			err := row.Scan(&entity.Pk, &entity.Name, &entity.Emails)
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

func TestCustomizedRepo(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgresql://hohin:hohin@localhost:5432/hohin?options=-c%20TimeZone%3DUTC")
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS contacts (
    id uuid PRIMARY KEY,
    name text NOT NULL
)
    `)
	if err != nil {
		panic(err)
	}
	defer func() {
		if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS contacts`); err != nil {
			panic(err)
		}
	}()

	_, err = pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS emails (
    id uuid PRIMARY KEY,
    email text NOT NULL UNIQUE,
    contact_id uuid REFERENCES contacts(id) ON DELETE CASCADE
)
    `)
	defer func() {
		if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS emails`); err != nil {
			panic(err)
		}
	}()

	cleanDb := func() {
		if _, err := pool.Exec(ctx, `DELETE FROM contacts`); err != nil {
			panic(err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM emails`); err != nil {
			panic(err)
		}
	}

	db := NewDb(pool).Simple()
	repo := makeContactsRepo()

	t.Run("TestOneToMany", func(t *testing.T) {
		cleanDb()
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
	})
}
