package tests

import (
	"context"
	"testing"

	"github.com/blueprint-uservices/blueprint/examples/dsb_hotel/workflow/hotelreservation"
	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/simplecache"
	"github.com/blueprint-uservices/blueprint/runtime/plugins/simplenosqldb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

// countDocuments is the number of documents in one collection of db.
func countDocuments(t *testing.T, ctx context.Context, db backend.NoSQLDatabase, name, collection string) int {
	c, err := db.GetCollection(ctx, name, collection)
	require.NoError(t, err)
	cursor, err := c.FindMany(ctx, bson.D{})
	require.NoError(t, err)
	var documents []bson.D
	require.NoError(t, cursor.All(ctx, &documents))
	return len(documents)
}

// A hotel service restarted against a database that outlived it -- every arm recreates the
// faulted services -- must not insert its seed rows a second time (APPS-CONTRACT 5.1).
func TestRestartedServicesDoNotReseed(t *testing.T) {
	ctx := context.Background()
	newDB := func() backend.NoSQLDatabase {
		db, err := simplenosqldb.NewSimpleNoSQLDB(ctx)
		require.NoError(t, err)
		return db
	}
	newCache := func() backend.Cache {
		cache, err := simplecache.NewSimpleCache(ctx)
		require.NoError(t, err)
		return cache
	}

	geoDB := newDB()
	profileDB := newDB()
	rateDB := newDB()
	recommendDB := newDB()
	reserveDB := newDB()
	userDB := newDB()
	var geo hotelreservation.GeoService
	for start := 0; start < 3; start++ {
		var err error
		geo, err = hotelreservation.NewGeoServiceImpl(ctx, geoDB)
		require.NoError(t, err)
		_, err = hotelreservation.NewProfileServiceImpl(ctx, newCache(), profileDB)
		require.NoError(t, err)
		_, err = hotelreservation.NewRateServiceImpl(ctx, newCache(), rateDB)
		require.NoError(t, err)
		_, err = hotelreservation.NewRecommendationServiceImpl(ctx, recommendDB)
		require.NoError(t, err)
		_, err = hotelreservation.NewReservationServiceImpl(ctx, newCache(), reserveDB)
		require.NoError(t, err)
		_, err = hotelreservation.NewUserServiceImpl(ctx, userDB)
		require.NoError(t, err)
	}

	assert.Equal(t, 80, countDocuments(t, ctx, geoDB, "geo-db", "geo"))
	assert.Equal(t, 80, countDocuments(t, ctx, profileDB, "profile-db", "hotels"))
	assert.Equal(t, 27, countDocuments(t, ctx, rateDB, "rate-db", "inventory"))
	assert.Equal(t, 80, countDocuments(t, ctx, recommendDB, "recommendation-db", "recommendation"))
	assert.Equal(t, 1, countDocuments(t, ctx, reserveDB, "reservation-db", "reservation"))
	assert.Equal(t, 80, countDocuments(t, ctx, reserveDB, "reservation-db", "number"))
	assert.Equal(t, 501, countDocuments(t, ctx, userDB, "user-db", "user"))

	// And the rebuilt index still answers: five distinct ids, no duplicate points.
	hotels, err := geo.Nearby(ctx, 37.7835, -122.41)
	require.NoError(t, err)
	assert.Len(t, hotels, 5)
	seen := map[string]bool{}
	for _, id := range hotels {
		assert.False(t, seen[id], "duplicate hotel id %s", id)
		seen[id] = true
	}
}
