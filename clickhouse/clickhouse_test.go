package clickhouse

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/meowmeowcode/hohin"
	"github.com/shopspring/decimal"
	"net/netip"
	"testing"
	"time"
)

type User struct {
	Id           uuid.UUID
	Name         string
	Age          uint32
	Active       bool
	Weight       float64
	Money        decimal.Decimal
	IpAddress    netip.Addr
	RegisteredAt time.Time
}

func (u *User) Equal(u2 *User) bool {
	return u.Id == u2.Id &&
		u.Name == u2.Name &&
		u.Age == u2.Age &&
		u.Active == u2.Active &&
		u.Weight == u2.Weight &&
		u.Money.Equal(u2.Money) &&
		u.IpAddress == u2.IpAddress &&
		u.RegisteredAt == u2.RegisteredAt
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

func addAlice(db hohin.SimpleDB, repo hohin.SimpleRepo[User]) User {
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
		Money:        money,
		IpAddress:    netip.MustParseAddr("192.168.1.1"),
		RegisteredAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
	}
	if err := repo.Add(db, u); err != nil {
		panic(err)
	}
	return u
}

func addBob(db hohin.SimpleDB, repo hohin.SimpleRepo[User]) User {
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
		Money:        money,
		IpAddress:    netip.MustParseAddr("192.168.1.2"),
		RegisteredAt: time.Date(2009, time.December, 10, 23, 0, 0, 0, time.UTC),
	}
	if err := repo.Add(db, u); err != nil {
		panic(err)
	}
	return u
}

func addEve(db hohin.SimpleDB, repo hohin.SimpleRepo[User]) User {
	money, err := decimal.NewFromString("168.31")
	if err != nil {
		panic(err)
	}
	u := User{
		Id:           uuid.New(),
		Name:         "Eve",
		Age:          36,
		Weight:       75.7,
		Money:        money,
		IpAddress:    netip.MustParseAddr("192.168.2.1"),
		RegisteredAt: time.Date(2009, time.October, 10, 23, 0, 0, 0, time.UTC),
	}
	if err := repo.Add(db, u); err != nil {
		panic(err)
	}
	return u
}

func TestRepo(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{Database: "hohin", Username: "hohin", Password: "hohin"},
	})

	if err != nil {
		panic(err)
	}
	defer conn.Close()
	ctx := context.Background()

	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS users`); err != nil {
		panic(err)
	}

	err = conn.Exec(ctx, `
		CREATE TABLE users (
			Id UUID NOT NULL,
			Name String NOT NULL,
			Age UInt32 NOT NULL,
			Active Boolean NOT NULL,
			Weight Float64 NOT NULL,
			Money Decimal(12, 2) NOT NULL,
			IpAddress IPv4 NOT NULL,
			RegisteredAt DateTime64 NOT NULL
		) ENGINE = MergeTree() ORDER BY RegisteredAt
	`)
	if err != nil {
		panic(err)
	}

	cleanDB := func() {
		if err = conn.Exec(ctx, `TRUNCATE TABLE users`); err != nil {
			panic(err)
		}
	}

	db := NewDB(conn).Simple()
	repo := NewRepo(Conf[User]{Table: "users"}).Simple()

	t.Run("TestAdd", func(t *testing.T) {
		cleanDB()
		alice := User{
			Name:         "Alice",
			RegisteredAt: time.Now().UTC().Round(time.Second),
			IpAddress:    netip.MustParseAddr("1.1.1.1"),
		}
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
	})

	t.Run("TestAddMany", func(t *testing.T) {
		cleanDB()
		users := []User{
			User{
				Id:           uuid.New(),
				Name:         "Alice",
				RegisteredAt: time.Now().UTC().Round(time.Second),
				IpAddress:    netip.MustParseAddr("1.1.1.1"),
			},
			User{
				Id:           uuid.New(),
				Name:         "Bob",
				RegisteredAt: time.Now().UTC().Round(time.Second),
				IpAddress:    netip.MustParseAddr("2.2.2.2"),
			},
		}
		if err := repo.AddMany(db, users); err != nil {
			t.Fatal(err)
		}
		result, err := repo.GetMany(db, hohin.Query{}.OrderBy(hohin.Asc("Name")))
		if err != nil {
			t.Fatal(err)
		}
		if !usersEqual(result, users) {
			t.Fatalf("%v != %v", result, users)
		}
	})

	t.Run("TestGet", func(t *testing.T) {
		cleanDB()
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
	})

	t.Run("TestGetForUpdate", func(t *testing.T) {
		cleanDB()
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
	})

	t.Run("TestExists", func(t *testing.T) {
		cleanDB()
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
	})

	t.Run("TestUpdate", func(t *testing.T) {
		cleanDB()
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
	})

	t.Run("TestDelete", func(t *testing.T) {
		cleanDB()
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
	})

	t.Run("TestCount", func(t *testing.T) {
		cleanDB()
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
	})

	t.Run("TestLimit", func(t *testing.T) {
		cleanDB()
		alice := addAlice(db, repo)
		bob := addBob(db, repo)
		addEve(db, repo)
		users, err := repo.GetMany(db, hohin.Query{Limit: 2}.OrderBy(hohin.Asc("Name")))
		if err != nil {
			t.Fatal(err)
		}
		expectedUsers := []User{alice, bob}
		if !usersEqual(users, expectedUsers) {
			t.Fatalf("%v != %v", users, expectedUsers)
		}
	})

	t.Run("TestOffset", func(t *testing.T) {
		cleanDB()
		addAlice(db, repo)
		bob := addBob(db, repo)
		eve := addEve(db, repo)
		users, err := repo.GetMany(db, hohin.Query{Offset: 1}.OrderBy(hohin.Asc("Name")))
		if err != nil {
			t.Fatal(err)
		}
		expectedUsers := []User{bob, eve}
		if !usersEqual(users, expectedUsers) {
			t.Fatalf("%v != %v", users, expectedUsers)
		}
	})

	t.Run("TestOrder", func(t *testing.T) {
		cleanDB()
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
	})

	t.Run("TestFilters", func(t *testing.T) {
		cleanDB()
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
				filter: hohin.In("Age", []any{alice.Age, eve.Age}),
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
				filter: hohin.In("Weight", []any{alice.Weight, eve.Weight}),
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
				filter: hohin.Eq("Name", "bob"),
				result: []User{},
			},
			{
				filter: hohin.IEq("Name", "bob"),
				result: []User{bob},
			},
			{
				filter: hohin.Ne("Name", "Bob"),
				result: []User{alice, eve},
			},
			{
				filter: hohin.Ne("Name", "bob"),
				result: []User{alice, bob, eve},
			},
			{
				filter: hohin.INe("Name", "bob"),
				result: []User{alice, eve},
			},
			{
				filter: hohin.In("Name", []any{"Alice", "Bob"}),
				result: []User{alice, bob},
			},
			{
				filter: hohin.HasPrefix("Name", "A"),
				result: []User{alice},
			},
			{
				filter: hohin.HasPrefix("Name", "a"),
				result: []User{},
			},
			{
				filter: hohin.IHasPrefix("Name", "a"),
				result: []User{alice},
			},
			{
				filter: hohin.HasSuffix("Name", "e"),
				result: []User{alice, eve},
			},
			{
				filter: hohin.HasSuffix("Name", "E"),
				result: []User{},
			},
			{
				filter: hohin.IHasSuffix("Name", "E"),
				result: []User{alice, eve},
			},
			{
				filter: hohin.Contains("Name", "o"),
				result: []User{bob},
			},
			{
				filter: hohin.Contains("Name", "O"),
				result: []User{},
			},
			{
				filter: hohin.IContains("Name", "O"),
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
			// Network operations:
			{
				filter: hohin.IPWithin("IpAddress", "192.168.1.0/24"),
				result: []User{alice, bob},
			},
			{
				filter: hohin.IPWithin("IpAddress", "192.168.2.0/24"),
				result: []User{eve},
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
			result, err := repo.GetMany(db, hohin.Query{Filter: cs.filter}.OrderBy(hohin.Asc("Name")))
			if err != nil {
				t.Fatal(err)
			}
			if !usersEqual(result, cs.result) {
				t.Errorf("filter: %v; expected result: %v; actual result: %v", cs.filter, cs.result, result)
			}
		}
	})

	t.Run("TestGetFirst", func(t *testing.T) {
		cleanDB()
		addAlice(db, repo)
		addBob(db, repo)
		eve := addEve(db, repo)
		u, err := repo.GetFirst(db, hohin.Query{}.OrderBy(hohin.Desc("Name")))
		if err != nil {
			t.Fatal(err)
		}
		if !u.Equal(&eve) {
			t.Fatalf("%v != %v", eve, u)
		}
		_, err = repo.GetFirst(db, hohin.Query{Filter: hohin.Eq("Name", "Robert")}.OrderBy(hohin.Desc("Name")))
		if err != hohin.NotFound {
			t.Fatalf("%v != %v", err, hohin.NotFound)
		}
	})

	t.Run("TestUnknownField", func(t *testing.T) {
		cleanDB()
		_, err := repo.Get(db, hohin.Eq("Test", "something"))
		if err == nil {
			t.Fatalf("err is nil")
		}
		if err.Error() != "unknown field `Test` in a filter" {
			t.Fatalf("Unexpected message `%s`", err.Error())
		}
	})

	t.Run("TestCountAll", func(t *testing.T) {
		cleanDB()
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
	})

	t.Run("TestClear", func(t *testing.T) {
		cleanDB()
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
	})

	t.Run("NullTest", func(t *testing.T) {
		err := conn.Exec(context.Background(), `DROP TABLE IF EXISTS options`)
		if err != nil {
			t.Fatal(err)
		}
		err = conn.Exec(context.Background(), `
			CREATE TABLE options (
				Value Nullable(String),
				CreatedAt DateTime64
			) ENGINE = MergeTree() ORDER BY CreatedAt
		`)
		if err != nil {
			t.Fatal(err)
		}
		type Option struct {
			Value     *string
			CreatedAt time.Time
		}
		optionsRepo := NewRepo(Conf[Option]{Table: "options"}).Simple()

		val := "test"
		optionsRepo.Add(db, Option{Value: &val, CreatedAt: time.Now().UTC()})
		optionsRepo.Add(db, Option{Value: &val, CreatedAt: time.Now().UTC()})
		optionsRepo.Add(db, Option{CreatedAt: time.Now().UTC()})
		count, err := optionsRepo.Count(db, hohin.IsNull("Value"))
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%v != 1", count)
		}
		count, err = optionsRepo.Count(db, hohin.Not(hohin.IsNull("Value")))
		if err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("%v != 2", count)
		}
	})
}
