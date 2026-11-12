package main

import "testing"

func TestUpdateTicketNotes(t *testing.T) {
	for _, tc := range []struct {
		name string
		args string
		want string
	}{
		{"clear", `{"card_id":"card-1","notes":""}`, ""},
		{"whitespace", `{"card_id":"card-1","notes":" \t\n "}`, "Old context"},
		{"padded", `{"card_id":"card-1","notes":" New context "}`, "New context"},
		{"non-string", `{"card_id":"card-1","notes":42}`, "Old context"},
		{"replace", `{"card_id":"card-1","notes":"New context"}`, "New context"},
		{"omitted", `{"card_id":"card-1","title":"New title"}`, "Old context"},
		{"null", `{"card_id":"card-1","notes":null}`, "Old context"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := &kanbanBoardApp{cards: []kanbanCard{{
				ID: "card-1", Title: "Original title", Notes: "Old context", Status: kanbanStatusBacklog,
			}}}
			result, changed, err := app.applyToolCall(kanbanRealtimeOutputItem{
				Name: "update_ticket", Arguments: tc.args,
			})
			if err != nil || !changed || result["updated"] != true {
				t.Fatalf("update failed: result=%v changed=%v err=%v", result, changed, err)
			}
			if got := app.snapshotState().Cards[0].Notes; got != tc.want {
				t.Errorf("notes = %q, want %q", got, tc.want)
			}
		})
	}
}
