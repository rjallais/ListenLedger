//go:build goexperiment.jsonv2

package app

import (
	"context"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/correlation"
	"ListenLedger/internal/messaging"
)

func registerArtistUpdateFanout(app *pocketbase.PocketBase, js jetstream.JetStream) {
	publish := func(record *core.Record, requestID string) {
		if requestID == "" {
			requestID = correlation.Get(record.Id)
		}
		correlation.Clear(record.Id)

		updatedAt := record.GetDateTime("last_updated").Time().Format(time.RFC3339)
		update := messaging.ArtistUpdated{
			Version:          messaging.SchemaVersionV1,
			RequestID:        requestID,
			ArtistID:         record.Id,
			Name:             record.GetString("name"),
			MonthlyListeners: record.GetInt("monthly_listeners"),
			FetchStatus:      record.GetString("fetch_status"),
			UpdatedAt:        updatedAt,
		}

		data, err := messaging.MarshalArtistUpdated(update)
		if err != nil {
			app.Logger().Warn("[hooks] failed to marshal artist.updated", "err", err)
			return
		}

		msgID := "artist.updated:" + record.Id + ":" + strconv.FormatInt(time.Now().UnixNano(), 36)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := js.Publish(ctx, messaging.SubjectArtistUpdated, data, jetstream.WithMsgID(msgID)); err != nil {
			app.Logger().Warn("[hooks] failed to publish artist.updated to JetStream", "err", err)
		} else if requestID != "" {
			app.Logger().Debug("[hooks] published artist.updated", "artist_id", record.Id, "request_id", requestID)
		}
	}

	app.OnRecordAfterUpdateSuccess("artists").BindFunc(func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		publish(e.Record, "")
		return nil
	})

	app.OnRecordAfterCreateSuccess("artists").BindFunc(func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		publish(e.Record, "")
		return nil
	})
}
