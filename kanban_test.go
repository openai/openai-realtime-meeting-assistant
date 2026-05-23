package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestHandleRealtimeEventIgnoresPartialFunctionCallEvents(t *testing.T) {
	app := newKanbanBoardApp()
	app.cards = cloneKanbanCards(initialKanbanBoardCards)

	raw := mustJSON(t, kanbanRealtimeEvent{
		Type:      "response.function_call_arguments.done",
		Name:      "move_ticket",
		CallID:    "call-partial",
		Arguments: `{"card_id":"card-003","status":"In Progress"}`,
	})
	app.handleRealtimeEvent(raw)

	if status := cardStatus(t, app, "card-003"); status != kanbanStatusBacklog {
		t.Fatalf("card status changed from partial event: got %q, want %q", status, kanbanStatusBacklog)
	}
}

func TestCreateTicketDefaultsOwnerToMarcusWade(t *testing.T) {
	app := newKanbanBoardApp()

	result, changed, err := app.createTicket(map[string]any{
		"title": "Prepare standup notes",
		"notes": "Get ready for the meeting.",
		"tags":  []any{"meeting"},
	})
	if err != nil {
		t.Fatalf("create ticket: %v", err)
	}
	if !changed {
		t.Fatal("create ticket did not report a board change")
	}

	card, ok := result["card"].(kanbanCard)
	if !ok {
		t.Fatalf("created card missing from result: %#v", result["card"])
	}
	if card.Owner != defaultCardOwner {
		t.Fatalf("card owner = %q, want %q", card.Owner, defaultCardOwner)
	}
}

func TestInitialKanbanCardsDefaultOwnerToMarcusWade(t *testing.T) {
	for _, card := range initialKanbanBoardCards {
		if card.Owner != defaultCardOwner {
			t.Fatalf("initial card %s owner = %q, want %q", card.ID, card.Owner, defaultCardOwner)
		}
	}
}

func TestResetMeetingClearsBoardAndCreatesTranscript(t *testing.T) {
	app := newKanbanBoardApp()
	app.transcriptDir = t.TempDir()

	transcriptPath, err := app.ResetMeeting()
	if err != nil {
		t.Fatalf("reset meeting: %v", err)
	}
	if transcriptPath == "" {
		t.Fatal("reset meeting returned an empty transcript path")
	}
	if len(app.snapshotState().Cards) != 0 {
		t.Fatalf("meeting board was not cleared: got %d cards", len(app.snapshotState().Cards))
	}

	contents, err := os.ReadFile(transcriptPath)
	if err != nil {
		t.Fatalf("read transcript: %v", err)
	}
	if !strings.Contains(string(contents), "# Meeting Transcript") {
		t.Fatalf("transcript header missing: %s", contents)
	}
}

func TestHandleRealtimeEventAppendsCompletedTranscript(t *testing.T) {
	app := newKanbanBoardApp()
	app.transcriptDir = t.TempDir()
	transcriptPath, err := app.ResetMeeting()
	if err != nil {
		t.Fatalf("reset meeting: %v", err)
	}

	app.handleRealtimeEvent(mustJSON(t, kanbanRealtimeEvent{
		Type:       "conversation.item.input_audio_transcription.completed",
		ItemID:     "item-1",
		Transcript: "I started the launch checklist.",
	}))

	contents, err := os.ReadFile(transcriptPath)
	if err != nil {
		t.Fatalf("read transcript: %v", err)
	}
	if !strings.Contains(string(contents), "I started the launch checklist.") {
		t.Fatalf("transcript entry missing: %s", contents)
	}
}

func TestHandleRealtimeEventAppliesFunctionCallFromResponseDone(t *testing.T) {
	app := newKanbanBoardApp()
	app.cards = cloneKanbanCards(initialKanbanBoardCards)

	raw := mustJSON(t, kanbanRealtimeEvent{
		Type: "response.done",
		Response: &struct {
			Output []kanbanRealtimeOutputItem `json:"output,omitempty"`
		}{
			Output: []kanbanRealtimeOutputItem{
				{
					Type:      "function_call",
					Name:      "move_ticket",
					CallID:    "call-complete",
					Arguments: `{"card_id":"card-003","status":"In Progress"}`,
				},
			},
		},
	})
	app.handleRealtimeEvent(raw)

	if status := cardStatus(t, app, "card-003"); status != kanbanStatusInProgress {
		t.Fatalf("card status was not updated from response.done: got %q, want %q", status, kanbanStatusInProgress)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return raw
}

func cardStatus(t *testing.T, app *kanbanBoardApp, cardID string) kanbanStatus {
	t.Helper()

	for _, card := range app.snapshotState().Cards {
		if card.ID == cardID {
			return card.Status
		}
	}
	t.Fatalf("card %s not found", cardID)
	return ""
}
