package hotelreservation

import (
	"context"

	"github.com/blueprint-uservices/blueprint/runtime/core/backend"
	"go.mongodb.org/mongo-driver/bson"
)

// alreadySeeded reports whether a collection holds any document.
//
// Every hotel service seeds its own collection in its constructor, and upstream inserted
// unconditionally. The campaign recreates a service container whenever its profile changes --
// the faulted services on every arm -- while its database container keeps running, so each
// restart inserted the rows again: geo's index then held every point twice and Nearby returned
// duplicate ids, and profile, rate and recommendation grew by 80, 27 and 80 rows per arm.
// Seeding only an empty collection keeps the dataset at what the constructors describe
// (APPS-CONTRACT 5.1), however often a service restarts. The in-memory state -- geo's index,
// recommendation's and user's maps -- was already rebuilt from the database, not from the
// seed loop, so skipping the seed leaves it intact.
func alreadySeeded(ctx context.Context, c backend.NoSQLCollection) (bool, error) {
	cursor, err := c.FindOne(ctx, bson.D{})
	if err != nil {
		return false, err
	}
	var document bson.D
	return cursor.One(ctx, &document)
}
