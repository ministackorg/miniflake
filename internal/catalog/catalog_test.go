package catalog

import (
	"testing"
)

func TestInit(t *testing.T) {
	c := New()
	c.Init()

	db, err := c.GetDatabase("SNOWFLAKE_SAMPLE_DATA")
	if err != nil {
		t.Fatalf("expected default database: %v", err)
	}
	if db.Name != "SNOWFLAKE_SAMPLE_DATA" {
		t.Fatalf("unexpected database name: %s", db.Name)
	}

	wh, err := c.GetWarehouse("COMPUTE_WH")
	if err != nil {
		t.Fatalf("expected default warehouse: %v", err)
	}
	if wh.Size != "X-Small" {
		t.Fatalf("unexpected warehouse size: %s", wh.Size)
	}
}

func TestCreateDropDatabase(t *testing.T) {
	c := New()

	if err := c.CreateDatabase("testdb", "ADMIN"); err != nil {
		t.Fatalf("create database: %v", err)
	}

	// Duplicate should fail.
	if err := c.CreateDatabase("TESTDB", "ADMIN"); err == nil {
		t.Fatal("expected error on duplicate database")
	}

	// Case insensitive retrieval.
	db, err := c.GetDatabase("testdb")
	if err != nil {
		t.Fatalf("get database: %v", err)
	}
	if db.Name != "TESTDB" {
		t.Fatalf("expected uppercase name, got %s", db.Name)
	}

	// Should have PUBLIC and INFORMATION_SCHEMA schemas.
	if _, ok := db.Schemas["PUBLIC"]; !ok {
		t.Fatal("missing PUBLIC schema")
	}
	if _, ok := db.Schemas["INFORMATION_SCHEMA"]; !ok {
		t.Fatal("missing INFORMATION_SCHEMA schema")
	}

	// Drop.
	if err := c.DropDatabase("testdb"); err != nil {
		t.Fatalf("drop database: %v", err)
	}
	if _, err := c.GetDatabase("testdb"); err == nil {
		t.Fatal("expected error after drop")
	}

	// Drop non-existent.
	if err := c.DropDatabase("nosuchdb"); err == nil {
		t.Fatal("expected error dropping non-existent database")
	}
}

func TestListDatabases(t *testing.T) {
	c := New()
	_ = c.CreateDatabase("db1", "ADMIN")
	_ = c.CreateDatabase("db2", "ADMIN")

	dbs := c.ListDatabases()
	if len(dbs) != 2 {
		t.Fatalf("expected 2 databases, got %d", len(dbs))
	}
}

func TestCreateDropSchema(t *testing.T) {
	c := New()
	_ = c.CreateDatabase("mydb", "ADMIN")

	if err := c.CreateSchema("mydb", "analytics", "ADMIN"); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	// Duplicate.
	if err := c.CreateSchema("mydb", "ANALYTICS", "ADMIN"); err == nil {
		t.Fatal("expected error on duplicate schema")
	}

	// Get.
	s, err := c.GetSchema("mydb", "analytics")
	if err != nil {
		t.Fatalf("get schema: %v", err)
	}
	if s.Name != "ANALYTICS" {
		t.Fatalf("unexpected schema name: %s", s.Name)
	}

	// List (PUBLIC + INFORMATION_SCHEMA + ANALYTICS = 3).
	schemas, err := c.ListSchemas("mydb")
	if err != nil {
		t.Fatalf("list schemas: %v", err)
	}
	if len(schemas) != 3 {
		t.Fatalf("expected 3 schemas, got %d", len(schemas))
	}

	// Drop.
	if err := c.DropSchema("mydb", "analytics"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := c.GetSchema("mydb", "analytics"); err == nil {
		t.Fatal("expected error after drop")
	}

	// Schema in non-existent db.
	if err := c.CreateSchema("nodb", "s", "ADMIN"); err == nil {
		t.Fatal("expected error for non-existent database")
	}
}

func TestRegisterDropTable(t *testing.T) {
	c := New()
	_ = c.CreateDatabase("mydb", "ADMIN")

	tbl := &TableMeta{
		Name: "users",
		Columns: []ColumnMeta{
			{Name: "ID", Type: "NUMBER", Nullable: false},
			{Name: "NAME", Type: "VARCHAR", Nullable: true},
		},
		Owner: "ADMIN",
	}

	if err := c.RegisterTable("mydb", "public", tbl); err != nil {
		t.Fatalf("register table: %v", err)
	}

	// Duplicate.
	tbl2 := &TableMeta{Name: "USERS"}
	if err := c.RegisterTable("mydb", "public", tbl2); err == nil {
		t.Fatal("expected error on duplicate table")
	}

	// Get.
	got, err := c.GetTable("mydb", "public", "users")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if len(got.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(got.Columns))
	}

	// List.
	tables, err := c.ListTables("mydb", "public")
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}

	// Drop.
	if err := c.DropTable("mydb", "public", "users"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := c.GetTable("mydb", "public", "users"); err == nil {
		t.Fatal("expected error after drop")
	}

	// Drop non-existent.
	if err := c.DropTable("mydb", "public", "nope"); err == nil {
		t.Fatal("expected error dropping non-existent table")
	}
}

func TestWarehouse(t *testing.T) {
	c := New()

	if err := c.CreateWarehouse("WH1", "Small", 300); err != nil {
		t.Fatalf("create warehouse: %v", err)
	}

	// Duplicate.
	if err := c.CreateWarehouse("wh1", "Medium", 600); err == nil {
		t.Fatal("expected error on duplicate warehouse")
	}

	wh, err := c.GetWarehouse("wh1")
	if err != nil {
		t.Fatalf("get warehouse: %v", err)
	}
	if wh.State != "STARTED" {
		t.Fatalf("expected STARTED, got %s", wh.State)
	}

	whs := c.ListWarehouses()
	if len(whs) != 1 {
		t.Fatalf("expected 1 warehouse, got %d", len(whs))
	}
}

func TestShowDatabases(t *testing.T) {
	c := New()
	c.Init()

	rows := c.ShowDatabases()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	name, ok := rows[0][1].(string)
	if !ok || name != "SNOWFLAKE_SAMPLE_DATA" {
		t.Fatalf("unexpected database name in SHOW: %v", rows[0][1])
	}
}

func TestShowSchemas(t *testing.T) {
	c := New()
	_ = c.CreateDatabase("mydb", "ADMIN")

	rows, err := c.ShowSchemas("mydb")
	if err != nil {
		t.Fatalf("show schemas: %v", err)
	}
	// PUBLIC + INFORMATION_SCHEMA
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestShowTablesAndColumns(t *testing.T) {
	c := New()
	_ = c.CreateDatabase("mydb", "ADMIN")
	_ = c.RegisterTable("mydb", "public", &TableMeta{
		Name: "orders",
		Columns: []ColumnMeta{
			{Name: "ID", Type: "NUMBER"},
			{Name: "AMOUNT", Type: "FLOAT"},
		},
		Comment: "order table",
		Owner:   "ADMIN",
	})

	rows, err := c.ShowTables("mydb", "public")
	if err != nil {
		t.Fatalf("show tables: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	cols, err := c.ShowColumns("mydb", "public", "orders")
	if err != nil {
		t.Fatalf("show columns: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
}
