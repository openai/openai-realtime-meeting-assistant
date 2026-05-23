package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

const (
	realtimeCallsURL          = "https://api.openai.com/v1/realtime/calls"
	defaultRealtimeModel      = "gpt-realtime-2"
	defaultReasoningEffort    = "low"
	realtimeEventChannelLabel = "oai-events"
	realtimeInputTrackID      = "kanban-realtime:mixed-audio"
	realtimeInputStreamID     = "kanban-realtime-input"
	realtimeMixedAudioSinkKey = "kanban-realtime"
	defaultCardOwner          = "Marcus Wade"
	defaultTranscriptDir      = "transcripts"
)

type kanbanStatus string

const (
	kanbanStatusBacklog    kanbanStatus = "Backlog"
	kanbanStatusInProgress kanbanStatus = "In Progress"
	kanbanStatusBlocked    kanbanStatus = "Blocked"
	kanbanStatusDone       kanbanStatus = "Done"
)

var kanbanStatuses = []kanbanStatus{
	kanbanStatusBacklog,
	kanbanStatusInProgress,
	kanbanStatusBlocked,
	kanbanStatusDone,
}

type kanbanCard struct {
	ID     string       `json:"id"`
	Status kanbanStatus `json:"status"`
	Title  string       `json:"title"`
	Notes  string       `json:"notes"`
	Tags   []string     `json:"tags"`
	Owner  string       `json:"owner"`
}

type kanbanBoardState struct {
	Cards     []kanbanCard `json:"cards"`
	UpdatedAt string       `json:"updatedAt,omitempty"`
}

type meetingTranscriptState struct {
	Entries     []meetingTranscriptEntry `json:"entries"`
	DownloadURL string                   `json:"downloadUrl"`
}

type meetingTranscriptEntry struct {
	At      string `json:"at"`
	Speaker string `json:"speaker"`
	Text    string `json:"text"`
}

type kanbanRealtimeEvent struct {
	Type       string `json:"type,omitempty"`
	Transcript string `json:"transcript,omitempty"`
	ItemID     string `json:"item_id,omitempty"`
	Name       string `json:"name,omitempty"`
	Arguments  string `json:"arguments,omitempty"`
	CallID     string `json:"call_id,omitempty"`
	Error      *struct {
		Code    string `json:"code,omitempty"`
		Message string `json:"message,omitempty"`
	} `json:"error,omitempty"`
	Item     *kanbanRealtimeOutputItem `json:"item,omitempty"`
	Response *struct {
		Output []kanbanRealtimeOutputItem `json:"output,omitempty"`
	} `json:"response,omitempty"`
}

type kanbanRealtimeOutputItem struct {
	Type      string `json:"type,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
	CallID    string `json:"call_id,omitempty"`
}

type kanbanBoardApp struct {
	mu                sync.Mutex
	cards             []kanbanCard
	nextCreatedIndex  int
	updatedAt         time.Time
	handledCalls      map[string]struct{}
	transcriptDir     string
	transcriptPath    string
	transcriptEntries []meetingTranscriptEntry

	model      string
	pc         *webrtc.PeerConnection
	events     *webrtc.DataChannel
	inputTrack *webrtc.TrackLocalStaticSample
	inputEnc   *opusEncoder
	connected  bool
	closeOnce  sync.Once
}

var initialKanbanBoardCards = []kanbanCard{
	{
		ID:     "card-002",
		Status: kanbanStatusBacklog,
		Title:  "Add RTP Retransmission Buffer",
		Notes:  "Keep recent RTP packets available for NACK-driven retransmission without unbounded memory growth.",
		Tags:   []string{"webrtc", "rtp", "nack"},
		Owner:  defaultCardOwner,
	},
	{
		ID:     "card-003",
		Status: kanbanStatusBacklog,
		Title:  "Implement ICE Restart Handling",
		Notes:  "Support renegotiation paths that refresh ICE credentials and reconnect peers after network changes.",
		Tags:   []string{"webrtc", "ice", "signaling"},
		Owner:  defaultCardOwner,
	},
	{
		ID:     "card-004",
		Status: kanbanStatusBacklog,
		Title:  "Harden DTLS/SRTP Cleanup",
		Notes:  "Ensure failed and closed peer connections release transports, tracks, and SRTP state promptly.",
		Tags:   []string{"webrtc", "dtls", "srtp"},
		Owner:  defaultCardOwner,
	},
	{
		ID:     "card-005",
		Status: kanbanStatusBacklog,
		Title:  "Add Simulcast Forwarding Controls",
		Notes:  "Choose forwarded RTP layers per subscriber so the server can adapt streams to bandwidth and viewport size.",
		Tags:   []string{"webrtc", "simulcast", "bandwidth"},
		Owner:  defaultCardOwner,
	},
	{
		ID:     "card-001",
		Status: kanbanStatusBacklog,
		Title:  "Finish RTP HEVC Packetizer",
		Notes:  "Complete HEVC payload fragmentation, aggregation, and marker-bit handling for outbound RTP streams.",
		Tags:   []string{"webrtc", "rtp", "hevc"},
		Owner:  defaultCardOwner,
	},
}

func newKanbanBoardApp() *kanbanBoardApp {
	return &kanbanBoardApp{
		cards:            []kanbanCard{},
		nextCreatedIndex: 1,
		updatedAt:        time.Now().UTC(),
		handledCalls:     map[string]struct{}{},
		transcriptDir:    defaultTranscriptDir,
	}
}

func (app *kanbanBoardApp) ResetMeeting() (string, error) {
	transcriptPath, err := app.createTranscriptFile()

	app.mu.Lock()
	app.cards = []kanbanCard{}
	app.nextCreatedIndex = 1
	app.updatedAt = time.Now().UTC()
	app.handledCalls = map[string]struct{}{}
	app.transcriptEntries = nil
	if err == nil {
		app.transcriptPath = transcriptPath
	}
	app.mu.Unlock()

	return transcriptPath, err
}

func (app *kanbanBoardApp) createTranscriptFile() (string, error) {
	app.mu.Lock()
	transcriptDir := app.transcriptDir
	app.mu.Unlock()
	if transcriptDir == "" {
		transcriptDir = defaultTranscriptDir
	}

	startedAt := time.Now().UTC()
	if err := os.MkdirAll(transcriptDir, 0o755); err != nil {
		return "", fmt.Errorf("create transcript directory: %w", err)
	}

	transcriptPath := filepath.Join(transcriptDir, "meeting-"+startedAt.Format("20060102-150405")+".md")
	header := fmt.Sprintf("# Meeting Transcript\n\nStarted: %s\n\n", startedAt.Format(time.RFC3339))
	if err := os.WriteFile(transcriptPath, []byte(header), 0o644); err != nil {
		return "", fmt.Errorf("create transcript file: %w", err)
	}

	return transcriptPath, nil
}

func (app *kanbanBoardApp) JoinConferenceRoom() error {
	apiKey := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	if apiKey == "" {
		return fmt.Errorf("OPENAI_API_KEY is not configured")
	}

	peerConnection, err := newPeerConnection()
	if err != nil {
		return fmt.Errorf("create Realtime peer connection: %w", err)
	}

	inputTrack, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: roomAudioSampleRate,
			Channels:  roomAudioChannels,
		},
		realtimeInputTrackID,
		realtimeInputStreamID,
	)
	if err != nil {
		_ = peerConnection.Close()
		return fmt.Errorf("create Realtime mixed audio input track: %w", err)
	}

	inputEnc, err := newOpusEncoder(roomAudioSampleRate, roomAudioChannels)
	if err != nil {
		_ = peerConnection.Close()
		return fmt.Errorf("create Realtime mixed audio encoder: %w", err)
	}

	inputSender, err := peerConnection.AddTrack(inputTrack)
	if err != nil {
		_ = peerConnection.Close()
		return fmt.Errorf("attach Realtime mixed audio input track: %w", err)
	}
	go drainRTCP(inputSender)

	events, err := peerConnection.CreateDataChannel(realtimeEventChannelLabel, nil)
	if err != nil {
		_ = peerConnection.Close()
		return fmt.Errorf("create Realtime event data channel: %w", err)
	}

	model := realtimeModel()
	app.mu.Lock()
	app.model = model
	app.pc = peerConnection
	app.events = events
	app.inputTrack = inputTrack
	app.inputEnc = inputEnc
	app.mu.Unlock()

	peerConnection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Infof("OpenAI Realtime peer state changed: %s", state.String())
		broadcastKanbanEvent("status", "OpenAI Realtime: "+state.String())
	})
	events.OnOpen(func() {
		log.Infof("OpenAI Realtime event channel opened")
		_ = app.SendEvent(app.sessionUpdateEvent())
		broadcastKanbanEvent("status", "Kanban assistant is listening")
	})
	events.OnMessage(func(message webrtc.DataChannelMessage) {
		app.handleRealtimeEvent(message.Data)
	})

	go func() {
		if err := app.connectRealtimePeer(apiKey, model); err != nil {
			log.Errorf("Failed to connect OpenAI Realtime peer: %v", err)
			broadcastKanbanEvent("status", "OpenAI Realtime disabled: "+err.Error())
			_ = peerConnection.Close()
			return
		}
		if roomMixer != nil {
			roomMixer.setSink(realtimeMixedAudioSinkKey, app)
		}
	}()

	return nil
}

func (app *kanbanBoardApp) Close() error {
	var closeErr error
	app.closeOnce.Do(func() {
		if roomMixer != nil {
			roomMixer.removeSink(realtimeMixedAudioSinkKey)
		}

		app.mu.Lock()
		peerConnection := app.pc
		app.mu.Unlock()
		if peerConnection != nil {
			closeErr = peerConnection.Close()
		}
	})

	return closeErr
}

func (app *kanbanBoardApp) connectRealtimePeer(apiKey string, model string) error {
	app.mu.Lock()
	if app.connected {
		app.mu.Unlock()
		return nil
	}
	peerConnection := app.pc
	app.mu.Unlock()

	if peerConnection == nil {
		return fmt.Errorf("Realtime peer connection is unavailable")
	}

	offer, err := peerConnection.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create Realtime offer: %w", err)
	}

	gatherComplete := webrtc.GatheringCompletePromise(peerConnection)
	if err := peerConnection.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set Realtime local description: %w", err)
	}
	<-gatherComplete

	localDescription := peerConnection.LocalDescription()
	if localDescription == nil || strings.TrimSpace(localDescription.SDP) == "" {
		return fmt.Errorf("Realtime peer connection did not produce a local description")
	}

	answerSDP, err := app.createRealtimeCall(apiKey, model, localDescription.SDP)
	if err != nil {
		return err
	}

	if err := peerConnection.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answerSDP,
	}); err != nil {
		return fmt.Errorf("set Realtime remote description: %w", err)
	}

	app.mu.Lock()
	app.connected = true
	app.mu.Unlock()

	return nil
}

func (app *kanbanBoardApp) WriteMixedPCM(roomPCM []int16) error {
	if len(roomPCM) == 0 {
		return nil
	}
	if len(roomPCM)%roomAudioMixFrameSize != 0 {
		return fmt.Errorf("mixed PCM length %d must be a multiple of %d samples", len(roomPCM), roomAudioMixFrameSize)
	}

	app.mu.Lock()
	inputTrack := app.inputTrack
	inputEnc := app.inputEnc
	app.mu.Unlock()

	if inputTrack == nil || inputEnc == nil {
		return fmt.Errorf("Realtime mixed audio input is unavailable")
	}

	for offset := 0; offset < len(roomPCM); offset += roomAudioMixFrameSize {
		frame := roomPCM[offset : offset+roomAudioMixFrameSize]

		opusFrame, err := inputEnc.Encode(frame)
		if err != nil {
			return fmt.Errorf("encode mixed room audio: %w", err)
		}

		if err := inputTrack.WriteSample(media.Sample{
			Data:     opusFrame,
			Duration: roomAudioMixInterval,
		}); err != nil {
			return fmt.Errorf("write mixed room audio sample: %w", err)
		}
	}

	return nil
}

func drainRTCP(sender *webrtc.RTPSender) {
	buffer := make([]byte, 1500)
	for {
		if _, _, err := sender.Read(buffer); err != nil {
			return
		}
	}
}

func (app *kanbanBoardApp) createRealtimeCall(apiKey string, model string, offerSDP string) (string, error) {
	contentType, body, err := buildRealtimeCallRequest(offerSDP, app.sessionConfig(model))
	if err != nil {
		return "", err
	}

	request, err := http.NewRequest(http.MethodPost, realtimeCallsURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create Realtime request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+apiKey)
	request.Header.Set("Content-Type", contentType)

	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return "", fmt.Errorf("create Realtime session: %w", err)
	}
	defer response.Body.Close()

	answerSDP, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("read Realtime answer: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("Realtime session failed: status=%s body=%s", response.Status, strings.TrimSpace(string(answerSDP)))
	}

	return string(answerSDP), nil
}

func buildRealtimeCallRequest(offerSDP string, session map[string]any) (string, []byte, error) {
	sessionJSON, err := json.Marshal(session)
	if err != nil {
		return "", nil, fmt.Errorf("marshal Realtime session: %w", err)
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("sdp", offerSDP); err != nil {
		return "", nil, fmt.Errorf("write SDP offer: %w", err)
	}
	if err := writer.WriteField("session", string(sessionJSON)); err != nil {
		return "", nil, fmt.Errorf("write session config: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", nil, fmt.Errorf("finalize multipart request: %w", err)
	}

	return writer.FormDataContentType(), body.Bytes(), nil
}

func (app *kanbanBoardApp) SendEvent(payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal Realtime event: %w", err)
	}

	app.mu.Lock()
	events := app.events
	app.mu.Unlock()
	if events == nil || events.ReadyState() != webrtc.DataChannelStateOpen {
		return fmt.Errorf("Realtime event channel is unavailable")
	}

	return events.SendText(string(raw))
}

func (app *kanbanBoardApp) sessionConfig(model string) map[string]any {
	session := map[string]any{
		"type":              "realtime",
		"model":             model,
		"output_modalities": []string{"text"},
		"audio": map[string]any{
			"input": map[string]any{
				"noise_reduction": map[string]any{
					"type": "near_field",
				},
				"transcription": map[string]any{
					"model":    "gpt-4o-mini-transcribe",
					"language": "en",
				},
				"turn_detection": map[string]any{
					"type":                "server_vad",
					"threshold":           0.5,
					"prefix_padding_ms":   300,
					"silence_duration_ms": 200,
					"create_response":     true,
					"interrupt_response":  false,
				},
			},
		},
		"instructions": app.sessionInstructions(),
		"tools":        app.kanbanTools(),
		"tool_choice":  "required",
	}

	if usesAdvancedCommandProfile(model) {
		session["reasoning"] = map[string]any{
			"effort": defaultReasoningEffort,
		}
	}

	return session
}

func (app *kanbanBoardApp) sessionUpdateEvent() map[string]any {
	return map[string]any{
		"type":    "session.update",
		"session": app.sessionConfig(app.model),
	}
}

func realtimeModel() string {
	if model := strings.TrimSpace(os.Getenv("OPENAI_REALTIME_MODEL")); model != "" {
		return model
	}

	return defaultRealtimeModel
}

func usesAdvancedCommandProfile(model string) bool {
	normalizedModel := strings.ToLower(strings.TrimSpace(model))
	return normalizedModel == "gpt-realtime-2"
}

func (app *kanbanBoardApp) sessionInstructions() string {
	return strings.Join([]string{
		"You are a voice-operated Kanban board operator.",
		"Listen to the user and decide whether they want to create a ticket, move a ticket between columns, add tags to a ticket, update a ticket, delete a ticket, or do nothing.",
		"Use the board card ids exactly as provided when operating on existing tickets.",
		"Users may say ticket, card, task, issue, or sticky note; treat those as Kanban cards.",
		"Available columns are Backlog, In Progress, Blocked, and Done.",
		"This is used during standups and meetings. Treat concrete first-person status updates as implicit board operations; do not wait for the user to say create a ticket.",
		"If a user says they shipped, fixed, completed, closed, or finished work, move an existing related ticket to Done if one exists; otherwise create a concise Done ticket.",
		"If a user says they started, began, picked up, or are working on something, move an existing related ticket to In Progress if one exists; otherwise create a concise In Progress ticket.",
		"If a user says they are blocked, waiting on something, dependent on another team, or that work might slip, move or create the related ticket in Blocked and add blocked, dependency, or risk tags as appropriate.",
		"Track meeting context across turns. If a follow-up sentence adds dependency, blocker, or schedule-risk context for the most recently discussed related card, update or move that existing ticket instead of creating a duplicate.",
		"If a transcript includes a speaker label such as Sean:, do not include the label in the title; use it only as context for notes or tags when useful.",
		"If a user asks to start, work on, pick up, or begin a ticket, move it to In Progress.",
		"If a user asks to block, mark blocked, or note a dependency for a ticket, move it to Blocked and preserve the blocker details in notes.",
		"If a user asks to ship, finish, complete, close, or mark done, move it to Done.",
		"If a user asks to park, punt, defer, or move something back, move it to Backlog.",
		"If a user asks to add a tag, call add_tags; do not replace existing tags.",
		"If one transcript contains multiple status updates, call one tool for each board operation.",
		"If the user asks for an operation or gives an implicit status update, call the relevant tool. Prefer tools over text replies.",
		"If the user is only wrapping up, handing off, giving filler, or saying something like That's it from me, call do_nothing with a short reason.",
		"If the user is not asking for a board operation and is not giving a concrete status update, call do_nothing with a short reason.",
		"Do not narrate board operations aloud.",
		fmt.Sprintf("Current Kanban board JSON: %s", app.boardContextJSON()),
	}, " ")
}

func (app *kanbanBoardApp) boardContextJSON() string {
	raw, err := json.Marshal(app.snapshotState().Cards)
	if err != nil {
		return "[]"
	}

	return string(raw)
}

func (app *kanbanBoardApp) kanbanTools() []map[string]any {
	statusProperty := map[string]any{
		"type":        "string",
		"description": "Kanban column for the ticket.",
		"enum":        []string{"Backlog", "In Progress", "Blocked", "Done"},
	}
	tagsProperty := map[string]any{
		"type":        "array",
		"description": "Short labels that capture people, area, state, or risk. Use blocked/dependency/risk tags for blockers when appropriate.",
		"items":       map[string]any{"type": "string"},
	}

	return []map[string]any{
		{
			"type":        "function",
			"name":        "create_ticket",
			"description": "Create a new Kanban ticket/card for explicit requests or implicit meeting status updates such as shipped, started, or blocked work.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"title":  map[string]any{"type": "string", "description": "Concise title for the work, without speaker prefixes such as Sean:."},
					"notes":  map[string]any{"type": "string", "description": "Useful context from the utterance, including blocker, dependency, or schedule-risk details."},
					"tags":   tagsProperty,
					"status": statusProperty,
				},
				"required":             []string{"title", "notes", "tags"},
				"additionalProperties": false,
			},
		},
		{
			"type":        "function",
			"name":        "move_ticket",
			"description": "Move an existing Kanban ticket/card to another column, including Blocked when work is waiting on a dependency.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"card_id": map[string]any{"type": "string", "description": "Existing board card id."},
					"status":  statusProperty,
				},
				"required":             []string{"card_id", "status"},
				"additionalProperties": false,
			},
		},
		{
			"type":        "function",
			"name":        "add_tags",
			"description": "Add one or more tags to an existing Kanban ticket/card without removing existing tags.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"card_id": map[string]any{"type": "string", "description": "Existing board card id."},
					"tags":    tagsProperty,
				},
				"required":             []string{"card_id", "tags"},
				"additionalProperties": false,
			},
		},
		{
			"type":        "function",
			"name":        "update_ticket",
			"description": "Update the title or notes of an existing Kanban ticket/card. Use this to merge follow-up standup details, dependency details, or slip-risk context into the existing notes.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"card_id": map[string]any{"type": "string", "description": "Existing board card id."},
					"title":   map[string]any{"type": "string", "description": "Replacement title, when the existing title should be made clearer."},
					"notes":   map[string]any{"type": "string", "description": "Full replacement notes. Preserve useful existing notes while adding the new context."},
				},
				"required":             []string{"card_id"},
				"additionalProperties": false,
			},
		},
		{
			"type":        "function",
			"name":        "delete_ticket",
			"description": "Delete an existing Kanban ticket/card.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"card_id": map[string]any{"type": "string", "description": "Existing board card id."},
				},
				"required":             []string{"card_id"},
				"additionalProperties": false,
			},
		},
		{
			"type":        "function",
			"name":        "do_nothing",
			"description": "Use this when the user is not asking to operate on the Kanban board, is only wrapping up, or says a handoff phrase like That's it from me.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"reason": map[string]any{"type": "string"},
				},
				"required":             []string{"reason"},
				"additionalProperties": false,
			},
		},
	}
}

func (app *kanbanBoardApp) handleRealtimeEvent(raw []byte) {
	var event kanbanRealtimeEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		log.Errorf("Failed to parse OpenAI Realtime event: %v", err)
		return
	}

	switch event.Type {
	case "error":
		if event.Error != nil {
			log.Errorf("OpenAI Realtime error code=%s message=%s", event.Error.Code, event.Error.Message)
			broadcastKanbanEvent("status", event.Error.Message)
		}
	case "conversation.item.input_audio_transcription.completed":
		if err := app.appendTranscript(event.Transcript); err != nil {
			log.Errorf("Failed to append meeting transcript: %v", err)
		}
	case "response.done":
		if event.Response == nil {
			return
		}
		for _, outputItem := range event.Response.Output {
			if outputItem.Type == "function_call" {
				app.handleToolCall(outputItem)
			}
		}
	}
}

func (app *kanbanBoardApp) appendTranscript(transcript string) error {
	transcript = strings.TrimSpace(transcript)
	if transcript == "" {
		return nil
	}

	app.mu.Lock()
	transcriptPath := app.transcriptPath
	app.mu.Unlock()
	if transcriptPath == "" {
		var err error
		transcriptPath, err = app.createTranscriptFile()
		if err != nil {
			return err
		}
		app.mu.Lock()
		if app.transcriptPath == "" {
			app.transcriptPath = transcriptPath
		}
		app.mu.Unlock()
	}

	entry := meetingTranscriptEntry{
		At:      time.Now().UTC().Format(time.RFC3339),
		Speaker: "Meeting audio",
		Text:    transcript,
	}
	markdownEntry := fmt.Sprintf("- %s - **%s:** %s\n", entry.At, entry.Speaker, entry.Text)
	file, err := os.OpenFile(transcriptPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open transcript file: %w", err)
	}
	defer file.Close()

	if _, err := file.WriteString(markdownEntry); err != nil {
		return fmt.Errorf("write transcript entry: %w", err)
	}

	app.mu.Lock()
	app.transcriptEntries = append(app.transcriptEntries, entry)
	app.mu.Unlock()

	broadcastKanbanEvent("transcript", app.snapshotTranscript())

	return nil
}

func (app *kanbanBoardApp) handleToolCall(outputItem kanbanRealtimeOutputItem) {
	if strings.TrimSpace(outputItem.CallID) == "" {
		log.Errorf("Ignoring Kanban tool call %q without call_id", outputItem.Name)
		return
	}

	app.mu.Lock()
	if _, ok := app.handledCalls[outputItem.CallID]; ok {
		app.mu.Unlock()
		return
	}
	app.handledCalls[outputItem.CallID] = struct{}{}
	app.mu.Unlock()

	result, changed, err := app.applyToolCall(outputItem)
	if err != nil {
		result = map[string]any{
			"ok":    false,
			"error": err.Error(),
		}
	}

	if err := app.SendEvent(map[string]any{
		"type": "conversation.item.create",
		"item": map[string]any{
			"type":    "function_call_output",
			"call_id": outputItem.CallID,
			"output":  mustMarshalJSON(result),
		},
	}); err != nil {
		log.Errorf("Failed to send Kanban function output: %v", err)
	}

	if !changed {
		return
	}

	broadcastKanbanEvent("board", app.snapshotState())
	if err := app.SendEvent(app.sessionUpdateEvent()); err != nil {
		log.Errorf("Failed to refresh Kanban Realtime session: %v", err)
	}
}

func (app *kanbanBoardApp) applyToolCall(outputItem kanbanRealtimeOutputItem) (map[string]any, bool, error) {
	args := map[string]any{}
	if rawArgs := strings.TrimSpace(outputItem.Arguments); rawArgs != "" {
		if err := json.Unmarshal([]byte(rawArgs), &args); err != nil {
			return nil, false, fmt.Errorf("parse %s arguments: %w", outputItem.Name, err)
		}
	}

	switch outputItem.Name {
	case "create_ticket":
		return app.createTicket(args)
	case "move_ticket":
		return app.moveTicket(args)
	case "add_tags":
		return app.addTags(args)
	case "update_ticket":
		return app.updateTicket(args)
	case "delete_ticket":
		return app.deleteTicket(args)
	case "do_nothing":
		reason := asString(args["reason"])
		if reason == "" {
			reason = "No board update requested."
		}
		return map[string]any{
			"ok":     true,
			"reason": reason,
		}, false, nil
	default:
		return nil, false, fmt.Errorf("unsupported function %q", outputItem.Name)
	}
}

func (app *kanbanBoardApp) createTicket(args map[string]any) (map[string]any, bool, error) {
	title := asString(args["title"])
	if title == "" {
		return nil, false, fmt.Errorf("title is required")
	}

	notes := asString(args["notes"])
	tags := uniqueStrings(asStringSlice(args["tags"]))
	status := kanbanStatusBacklog
	if rawStatus, ok := args["status"]; ok {
		parsedStatus, err := parseKanbanStatus(rawStatus)
		if err != nil {
			return nil, false, err
		}
		status = parsedStatus
	}

	app.mu.Lock()
	defer app.mu.Unlock()

	card := kanbanCard{
		ID:     app.createCardIDLocked(),
		Status: status,
		Title:  title,
		Notes:  notes,
		Tags:   tags,
		Owner:  defaultCardOwner,
	}
	app.cards = append(app.cards, card)
	app.touchLocked()

	return map[string]any{
		"ok":      true,
		"created": true,
		"card":    cloneKanbanCard(card),
	}, true, nil
}

func (app *kanbanBoardApp) moveTicket(args map[string]any) (map[string]any, bool, error) {
	cardID := asString(args["card_id"])
	if cardID == "" {
		return nil, false, fmt.Errorf("card_id is required")
	}

	status, err := parseKanbanStatus(args["status"])
	if err != nil {
		return nil, false, err
	}

	app.mu.Lock()
	defer app.mu.Unlock()

	card, ok := app.findCardLocked(cardID)
	if !ok {
		return nil, false, fmt.Errorf("unknown card_id: %s", cardID)
	}
	card.Status = status
	app.touchLocked()

	return map[string]any{
		"ok":      true,
		"moved":   true,
		"card_id": cardID,
		"status":  status,
	}, true, nil
}

func (app *kanbanBoardApp) addTags(args map[string]any) (map[string]any, bool, error) {
	cardID := asString(args["card_id"])
	if cardID == "" {
		return nil, false, fmt.Errorf("card_id is required")
	}

	tags := uniqueStrings(asStringSlice(args["tags"]))

	app.mu.Lock()
	defer app.mu.Unlock()

	card, ok := app.findCardLocked(cardID)
	if !ok {
		return nil, false, fmt.Errorf("unknown card_id: %s", cardID)
	}
	card.Tags = uniqueStrings(append(card.Tags, tags...))
	app.touchLocked()

	return map[string]any{
		"ok":         true,
		"tags_added": true,
		"card_id":    cardID,
		"tags":       append([]string(nil), tags...),
	}, true, nil
}

func (app *kanbanBoardApp) updateTicket(args map[string]any) (map[string]any, bool, error) {
	cardID := asString(args["card_id"])
	if cardID == "" {
		return nil, false, fmt.Errorf("card_id is required")
	}

	title := asString(args["title"])
	notes := asString(args["notes"])

	app.mu.Lock()
	defer app.mu.Unlock()

	card, ok := app.findCardLocked(cardID)
	if !ok {
		return nil, false, fmt.Errorf("unknown card_id: %s", cardID)
	}
	if title != "" {
		card.Title = title
	}
	if notes != "" {
		card.Notes = notes
	}
	app.touchLocked()

	return map[string]any{
		"ok":      true,
		"updated": true,
		"card_id": cardID,
	}, true, nil
}

func (app *kanbanBoardApp) deleteTicket(args map[string]any) (map[string]any, bool, error) {
	cardID := asString(args["card_id"])
	if cardID == "" {
		return nil, false, fmt.Errorf("card_id is required")
	}

	app.mu.Lock()
	defer app.mu.Unlock()

	index := -1
	for candidateIndex, card := range app.cards {
		if card.ID == cardID {
			index = candidateIndex
			break
		}
	}
	if index == -1 {
		return nil, false, fmt.Errorf("unknown card_id: %s", cardID)
	}
	app.cards = append(app.cards[:index], app.cards[index+1:]...)
	app.touchLocked()

	return map[string]any{
		"ok":      true,
		"deleted": true,
		"card_id": cardID,
	}, true, nil
}

func (app *kanbanBoardApp) snapshotState() kanbanBoardState {
	app.mu.Lock()
	defer app.mu.Unlock()

	state := kanbanBoardState{
		Cards: cloneKanbanCards(app.cards),
	}
	if !app.updatedAt.IsZero() {
		state.UpdatedAt = app.updatedAt.UTC().Format(time.RFC3339Nano)
	}

	return state
}

func (app *kanbanBoardApp) snapshotTranscript() meetingTranscriptState {
	app.mu.Lock()
	defer app.mu.Unlock()

	entries := append([]meetingTranscriptEntry(nil), app.transcriptEntries...)
	return meetingTranscriptState{
		Entries:     entries,
		DownloadURL: "/transcript",
	}
}

func (app *kanbanBoardApp) currentTranscriptPath() string {
	app.mu.Lock()
	defer app.mu.Unlock()

	return app.transcriptPath
}

func (app *kanbanBoardApp) createCardIDLocked() string {
	for {
		cardID := fmt.Sprintf("kanban-card-%03d", app.nextCreatedIndex)
		app.nextCreatedIndex++
		if _, exists := app.findCardLocked(cardID); exists {
			continue
		}
		return cardID
	}
}

func (app *kanbanBoardApp) findCardLocked(cardID string) (*kanbanCard, bool) {
	for index := range app.cards {
		if app.cards[index].ID == cardID {
			return &app.cards[index], true
		}
	}

	return nil, false
}

func (app *kanbanBoardApp) touchLocked() {
	app.updatedAt = time.Now().UTC()
}

func cloneKanbanCards(cards []kanbanCard) []kanbanCard {
	clonedCards := make([]kanbanCard, 0, len(cards))
	for _, card := range cards {
		clonedCards = append(clonedCards, cloneKanbanCard(card))
	}

	return clonedCards
}

func cloneKanbanCard(card kanbanCard) kanbanCard {
	return kanbanCard{
		ID:     card.ID,
		Status: card.Status,
		Title:  card.Title,
		Notes:  card.Notes,
		Tags:   append([]string(nil), card.Tags...),
		Owner:  card.Owner,
	}
}

func asString(value any) string {
	candidate, ok := value.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(candidate)
}

func asStringSlice(value any) []string {
	rawValues, ok := value.([]any)
	if !ok {
		return nil
	}

	values := make([]string, 0, len(rawValues))
	for _, rawValue := range rawValues {
		if value := asString(rawValue); value != "" {
			values = append(values, value)
		}
	}

	return values
}

func parseKanbanStatus(value any) (kanbanStatus, error) {
	status := kanbanStatus(asString(value))
	for _, candidate := range kanbanStatuses {
		if candidate == status {
			return status, nil
		}
	}

	return "", fmt.Errorf("unknown Kanban status: %v", value)
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		normalizedValue := strings.TrimSpace(value)
		if normalizedValue == "" {
			continue
		}
		if _, ok := seen[normalizedValue]; ok {
			continue
		}
		seen[normalizedValue] = struct{}{}
		result = append(result, normalizedValue)
	}

	return result
}

func mustMarshalJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return `{"ok":false,"error":"Could not encode function output."}`
	}

	return string(raw)
}

func sendKanbanEvent(websocket *threadSafeWriter, event string, data any) error {
	raw, err := json.Marshal(map[string]any{
		"event": event,
		"data":  data,
	})
	if err != nil {
		return err
	}

	return websocket.WriteJSON(&websocketMessage{
		Event: "kanban",
		Data:  string(raw),
	})
}

func broadcastKanbanEvent(event string, data any) {
	raw, err := json.Marshal(map[string]any{
		"event": event,
		"data":  data,
	})
	if err != nil {
		log.Errorf("Failed to encode Kanban event: %v", err)
		return
	}

	listLock.RLock()
	websockets := make([]*threadSafeWriter, 0, len(peerConnections))
	for _, state := range peerConnections {
		if state.websocket != nil {
			websockets = append(websockets, state.websocket)
		}
	}
	listLock.RUnlock()

	for _, websocket := range websockets {
		if err := websocket.WriteJSON(&websocketMessage{
			Event: "kanban",
			Data:  string(raw),
		}); err != nil {
			log.Errorf("Failed to send Kanban event: %v", err)
		}
	}
}
