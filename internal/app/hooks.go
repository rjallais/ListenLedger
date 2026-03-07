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

func registerArtistUpdateFanout(ctx context.Context, app *pocketbase.PocketBase, js jetstream.JetStream) {
	publish := func(record *core.Record, requestID string) {
		if requestID == "" {
			requestID = correlation.Pop(record.Id)
		} else {
			correlation.Clear(record.Id)
		}

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

		artistID := record.Id
		msgID := "artist.updated:" + artistID + ":" + strconv.FormatInt(time.Now().UnixNano(), 36)
		logger := app.Logger()

		go func(artistID, requestID, msgID string, data []byte) {
			publishCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			if _, err := js.Publish(publishCtx, messaging.SubjectArtistUpdated, data, jetstream.WithMsgID(msgID)); err != nil {
				logger.Warn("[hooks] failed to publish artist.updated to JetStream", "err", err)
			} else if requestID != "" {
				logger.Debug("[hooks] published artist.updated", "artist_id", artistID, "request_id", requestID)
			}
		}(artistID, requestID, msgID, data)
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
