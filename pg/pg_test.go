package pg

import (
	"database/sql"
	"errors"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/meowmeowcode/hohin"
	"github.com/shopspring/decimal"
	"testing"
	"time"
)

type User struct {
	Id           uuid.UUID
	Name         string
	Age          int
	Active       bool
	Weight       float64
	Money        decimal.Decimal
	RegisteredAt time.Time
}

func (u *User) Equal(u2 *User) bool {
	return u.Id == u2.Id &&
		u.Name == u2.Name &&
		u.Age == u2.Age &&
		u.Active == u2.Active &&
		u.Weight == u2.Weight &&
		u.RegisteredAt == u2.RegisteredAt &&
		u.Money.Equal(u2.Money)
}

func usersEqual(u, u2 []User) bool {
	if len(u) != len(u2) {
		return false
	}

	for i := range u {
		if !u[i].Equal(&u2[i]) {
			return false
		}
	}

	return true
}

func makeDb() hohin.Db {
	pool, err := sql.Open("postgres", "user=hohin dbname=hohin password=hohin sslmode=disable")
	if err != nil {
		panic(err)
	}
	_, err = pool.Exec(`
CREATE TABLE IF NOT EXISTS users (
    Id uuid PRIMARY KEY,
    Name text NOT NULL,
    Age bigint NOT NULL,
    Active boolean NOT NULL,
    Weight float8 NOT NULL,
    Money decimal NOT NULL,
    RegisteredAt timestamptz NOT NULL
)
       `)
	if err != nil {
		panic(err)
	}
	_, err = pool.Exec(`DELETE FROM users`)
	if err != nil {
		panic(err)
	}
	return NewDb(pool)
}

func makeRepo() hohin.Repo[User] {
	return NewRepo(Conf[User]{Table: "users"})
}

func addAlice(db hohin.Db, repo hohin.Repo[User]) User {
	money, err := decimal.NewFromString("120.50")
	if err != nil {
		panic(err)
	}
	u := User{
		Id:           uuid.New(),
		Name:         "Alice",
		Age:          23,
		Active:       true,
		Weight:       60.5,
		RegisteredAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
		Money:        money,
	}
	if err := repo.Add(db, u); err != nil {
		panic(err)
	}
	return u
}

func addBob(db hohin.Db, repo hohin.Repo[User]) User {
	money, err := decimal.NewFromString("136.02")
	if err != nil {
		panic(err)
	}
	u := User{
		Id:           uuid.New(),
		Name:         "Bob",
		Age:          27,
		Active:       true,
		Weight:       75.6,
		RegisteredAt: time.Date(2009, time.December, 10, 23, 0, 0, 0, time.UTC),
		Money:        money,
	}
	if err := repo.Add(db, u); err != nil {
		panic(err)
	}
	return u
}

func addEve(db hohin.Db, repo hohin.Repo[User]) User {
	money, err := decimal.NewFromString("168.31")
	if err != nil {
		panic(err)
	}
	u := User{
		Id:           uuid.New(),
		Name:         "Eve",
		Age:          36,
		Weight:       75.7,
		RegisteredAt: time.Date(2009, time.October, 10, 23, 0, 0, 0, time.UTC),
		Money:        money,
	}
	if err := repo.Add(db, u); err != nil {
		panic(err)
	}
	return u
}

func TestAdd(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	alice := User{Name: "Alice"}
	if err := repo.Add(db, alice); err != nil {
		t.Fatal(err)
	}
	u, err := repo.Get(db, hohin.Eq("Name", "Alice"))
	if err != nil {
		t.Fatal(err)
	}
	if !u.Equal(&alice) {
		t.Fatalf("%v != %v", alice, u)
	}
}

func TestGet(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	alice := addAlice(db, repo)
	bob := addBob(db, repo)
	u, err := repo.Get(db, hohin.Eq("Name", "Alice"))
	if err != nil {
		t.Fatal(err)
	}
	if !u.Equal(&alice) {
		t.Fatalf("%v != %v", alice, u)
	}
	u, err = repo.Get(db, hohin.Eq("Name", "Bob"))
	if err != nil {
		t.Fatal(err)
	}
	if !u.Equal(&bob) {
		t.Fatalf("%v != %v", bob, u)
	}
	_, err = repo.Get(db, hohin.Eq("Name", "Eve"))
	if err != hohin.NotFound {
		t.Fatalf("%v != %v", err, hohin.NotFound)
	}
}

func TestGetForUpdate(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	alice := addAlice(db, repo)
	bob := addBob(db, repo)
	u, err := repo.GetForUpdate(db, hohin.Eq("Name", "Alice"))
	if err != nil {
		t.Fatal(err)
	}
	if !u.Equal(&alice) {
		t.Fatalf("%v != %v", alice, u)
	}
	u, err = repo.GetForUpdate(db, hohin.Eq("Name", "Bob"))
	if err != nil {
		t.Fatal(err)
	}
	if !u.Equal(&bob) {
		t.Fatalf("%v != %v", bob, u)
	}
	_, err = repo.GetForUpdate(db, hohin.Eq("Name", "Eve"))
	if err != hohin.NotFound {
		t.Fatalf("%v != %v", err, hohin.NotFound)
	}
}

func TestExists(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	addAlice(db, repo)
	addBob(db, repo)
	addEve(db, repo)
	if err := repo.Delete(db, hohin.Contains("Name", "e")); err != nil {
		t.Fatal(err)
	}
	exists, err := repo.Exists(db, hohin.Eq("Name", "Alice"))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("Alice is not deleted")
	}
	exists, err = repo.Exists(db, hohin.Eq("Name", "Bob"))
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("Bob is deleted")
	}
	exists, err = repo.Exists(db, hohin.Eq("Name", "Eve"))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("Eve is not deleted")
	}
}

func TestUpdate(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	alice := addAlice(db, repo)
	bob := addBob(db, repo)
	bob.Name = "Robert"
	if err := repo.Update(db, hohin.Eq("Id", bob.Id), bob); err != nil {
		t.Fatal(err)
	}
	u, err := repo.Get(db, hohin.Eq("Name", bob.Name))
	if err != nil {
		t.Fatal(err)
	}
	if !u.Equal(&bob) {
		t.Fatalf("%v != %v", u, bob)
	}
	u, err = repo.Get(db, hohin.Eq("Name", alice.Name))
	if err != nil {
		t.Fatal(err)
	}
	if !u.Equal(&alice) {
		t.Fatalf("%v != %v", u, alice)
	}
}

func TestDelete(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	addAlice(db, repo)
	exists, err := repo.Exists(db, hohin.Eq("Name", "Alice"))
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("%v != %v", exists, true)
	}
	exists, err = repo.Exists(db, hohin.Eq("Name", "Bob"))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("%v != %v", exists, false)
	}
}

func TestCount(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	addAlice(db, repo)
	addBob(db, repo)
	addEve(db, repo)
	count, err := repo.Count(db, hohin.Contains("Name", "e"))
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("%v != %v", count, 2)
	}
}

func TestLimit(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	alice := addAlice(db, repo)
	bob := addBob(db, repo)
	addEve(db, repo)
	users, err := repo.GetMany(db, hohin.Query{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	expectedUsers := []User{alice, bob}
	if !usersEqual(users, expectedUsers) {
		t.Fatalf("%v != %v", users, expectedUsers)
	}
}

func TestOffset(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	addAlice(db, repo)
	bob := addBob(db, repo)
	eve := addEve(db, repo)
	users, err := repo.GetMany(db, hohin.Query{Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	expectedUsers := []User{bob, eve}
	if !usersEqual(users, expectedUsers) {
		t.Fatalf("%v != %v", users, expectedUsers)
	}
}

func TestOrder(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	alice := addAlice(db, repo)
	bob := addBob(db, repo)
	eve := addEve(db, repo)

	users, err := repo.GetMany(db, hohin.Query{}.OrderBy(hohin.Desc("Name")))
	if err != nil {
		t.Fatal(err)
	}
	expectedUsers := []User{eve, bob, alice}
	if !usersEqual(users, expectedUsers) {
		t.Fatalf("%v != %v", users, expectedUsers)
	}

	expectedUsers = []User{eve, alice, bob}
	users, err = repo.GetMany(db, hohin.Query{}.OrderBy(hohin.Asc("Active"), hohin.Asc("Name")))
	if err != nil {
		t.Fatal(err)
	}
	if !usersEqual(users, expectedUsers) {
		t.Fatalf("%v != %v", users, expectedUsers)
	}
}

func TestFilters(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	alice := addAlice(db, repo)
	bob := addBob(db, repo)
	eve := addEve(db, repo)
	cases := []struct {
		filter hohin.Filter
		result []User
	}{
		// int operations:
		{
			filter: hohin.Eq("Age", eve.Age),
			result: []User{eve},
		},
		{
			filter: hohin.Ne("Age", eve.Age),
			result: []User{alice, bob},
		},
		{
			filter: hohin.Lt("Age", bob.Age),
			result: []User{alice},
		},
		{
			filter: hohin.Gt("Age", bob.Age),
			result: []User{eve},
		},
		{
			filter: hohin.Lte("Age", bob.Age),
			result: []User{alice, bob},
		},
		{
			filter: hohin.Gte("Age", bob.Age),
			result: []User{bob, eve},
		},
		{
			filter: hohin.In("Age", []int{alice.Age, eve.Age}),
			result: []User{alice, eve},
		},
		// float64 operations:
		{
			filter: hohin.Eq("Weight", bob.Weight),
			result: []User{bob},
		},
		{
			filter: hohin.Ne("Weight", bob.Weight),
			result: []User{alice, eve},
		},
		{
			filter: hohin.Lt("Weight", bob.Weight),
			result: []User{alice},
		},
		{
			filter: hohin.Gt("Weight", bob.Weight),
			result: []User{eve},
		},
		{
			filter: hohin.Lte("Weight", bob.Weight),
			result: []User{alice, bob},
		},
		{
			filter: hohin.Gte("Weight", bob.Weight),
			result: []User{bob, eve},
		},
		{
			filter: hohin.In("Weight", []float64{alice.Weight, eve.Weight}),
			result: []User{alice, eve},
		},
		// decimal operations:
		{
			filter: hohin.Eq("Money", eve.Money),
			result: []User{eve},
		},
		{
			filter: hohin.Ne("Money", eve.Money),
			result: []User{alice, bob},
		},
		{
			filter: hohin.Lt("Money", bob.Money),
			result: []User{alice},
		},
		{
			filter: hohin.Gt("Money", bob.Money),
			result: []User{eve},
		},
		{
			filter: hohin.Lte("Money", bob.Money),
			result: []User{alice, bob},
		},
		{
			filter: hohin.Gte("Money", bob.Money),
			result: []User{bob, eve},
		},
		// bool operations:
		{
			filter: hohin.Eq("Active", true),
			result: []User{alice, bob},
		},
		{
			filter: hohin.Ne("Active", true),
			result: []User{eve},
		},
		// string operations:
		{
			filter: hohin.Eq("Name", "Bob"),
			result: []User{bob},
		},
		{
			filter: hohin.Ne("Name", "Bob"),
			result: []User{alice, eve},
		},
		{
			filter: hohin.In("Name", []string{"Alice", "Bob"}),
			result: []User{alice, bob},
		},
		{
			filter: hohin.HasPrefix("Name", "A"),
			result: []User{alice},
		},
		{
			filter: hohin.HasSuffix("Name", "e"),
			result: []User{alice, eve},
		},
		{
			filter: hohin.Contains("Name", "o"),
			result: []User{bob},
		},
		// time.Time operations:
		{
			filter: hohin.Eq("RegisteredAt", eve.RegisteredAt),
			result: []User{eve},
		},
		{
			filter: hohin.Ne("RegisteredAt", eve.RegisteredAt),
			result: []User{alice, bob},
		},
		{
			filter: hohin.Lt("RegisteredAt", alice.RegisteredAt),
			result: []User{eve},
		},
		{
			filter: hohin.Gt("RegisteredAt", alice.RegisteredAt),
			result: []User{bob},
		},
		{
			filter: hohin.Lte("RegisteredAt", alice.RegisteredAt),
			result: []User{alice, eve},
		},
		{
			filter: hohin.Gte("RegisteredAt", alice.RegisteredAt),
			result: []User{alice, bob},
		},
		// uuid operations:
		{
			filter: hohin.Eq("Id", eve.Id),
			result: []User{eve},
		},
		{
			filter: hohin.Ne("Id", eve.Id),
			result: []User{alice, bob},
		},
		// Not, And, Or:
		{
			filter: hohin.Not(hohin.Contains("Name", "e")),
			result: []User{bob},
		},
		{
			filter: hohin.And(hohin.HasPrefix("Name", "E"), hohin.HasSuffix("Name", "e")),
			result: []User{eve},
		},
		{
			filter: hohin.Or(hohin.Eq("Name", "Eve"), hohin.Eq("Name", "Alice")),
			result: []User{alice, eve},
		},
	}
	for _, cs := range cases {
		result, err := repo.GetMany(db, hohin.Query{Filter: cs.filter})
		if err != nil {
			t.Fatal(err)
		}
		if !usersEqual(result, cs.result) {
			t.Errorf("filter: %v; expected result: %v; actual result: %v", cs.filter, cs.result, result)
		}
	}
}

func TestCountAll(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	addAlice(db, repo)
	addBob(db, repo)
	addEve(db, repo)
	count, err := repo.CountAll(db)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("%v != 3", count)
	}
}

func TestClear(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	addAlice(db, repo)
	addBob(db, repo)
	addEve(db, repo)
	if err := repo.Clear(db); err != nil {
		t.Fatal(err)
	}
	count, err := repo.CountAll(db)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%v != 0", count)
	}
}

func TestTransaction(t *testing.T) {
	db := makeDb()
	repo := makeRepo()
	addAlice(db, repo)
	bob := addBob(db, repo)
	addEve(db, repo)
	err := db.Transaction(func(db hohin.Db) error {
		repo.Delete(db, hohin.Eq("Id", bob.Id))
		return errors.New("fail")
	})
	if err == nil {
		t.Fatal("Transaction didn't fail")
	}
	exists, err := repo.Exists(db, hohin.Eq("Id", bob.Id))
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("Transaction wasn't rolled back")
	}
	err = db.Transaction(func(db hohin.Db) error {
		repo.Delete(db, hohin.Eq("Id", bob.Id))
		return nil
	})
	if err != nil {
		t.Fatal("Transaction failed")
	}
	exists, err = repo.Exists(db, hohin.Eq("Id", bob.Id))
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("Transaction wasn't commited")
	}
}
