// Package storagetest wires the scripted drivers in internal/fakedb into real
// storage clients, so a package can test code that holds a *storage.PostgresClient
// or *storage.ClickHouseClient without a server.
package storagetest

import (
	"bifract/internal/fakedb"
	"bifract/pkg/storage"
)

// Postgres returns a client over a fresh scripted Postgres driver.
func Postgres() (*storage.PostgresClient, *fakedb.PG) {
	pg := fakedb.NewPG()
	return storage.NewPostgresClientFromDB(pg.DB()), pg
}

// ClickHouse returns a single-node client over a fresh scripted connection,
// using the "logs" database.
func ClickHouse() (*storage.ClickHouseClient, *fakedb.CH) {
	return ClickHouseWith(storage.Topology{Kind: storage.DeploymentSingleNode})
}

// ClickHouseWith is ClickHouse for another deployment shape, e.g. a cluster
// topology from storage.DeriveTopology.
func ClickHouseWith(topo storage.Topology) (*storage.ClickHouseClient, *fakedb.CH) {
	ch := fakedb.NewCH()
	return storage.NewClickHouseClientFromConn(ch, "logs", topo), ch
}
