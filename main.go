package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

const telegramAPI = "https://api.telegram.org/bot"

type Chat struct {
	ID int64 `json:"id"`
}

type User struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name"`
	Username  string `json:"username"`
}

type Message struct {
	MessageID int    `json:"message_id"`
	Text      string `json:"text"`
	Date      int64  `json:"date"`
	From      *User  `json:"from"`
	Chat      Chat   `json:"chat"`
}

type Update struct {
	UpdateID      int      `json:"update_id"`
	Message       *Message `json:"message"`
	EditedMessage *Message `json:"edited_message"`
}

type UpdatesResponse struct {
	OK     bool     `json:"ok"`
	Result []Update `json:"result"`
}

type TGSendResponse struct {
	OK     bool    `json:"ok"`
	Result Message `json:"result"`
}

// RabbitMessage is published to the outbound queue when a TG message is received/edited.
type RabbitMessage struct {
	MessageID int    `json:"message_id"`
	ChatID    int64  `json:"chat_id"`
	Text      string `json:"text"`
	Author    string `json:"author"`
	Timestamp string `json:"timestamp"`
	IsEdit    bool   `json:"is_edit"`
}

// SendCommand is consumed from the inbound queue to create or edit a TG message.
type SendCommand struct {
	ID          int    `json:"id"`     // internal ID from the sender
	Action      string `json:"action"` // "create" or "edit"
	ChatID      int64  `json:"chat_id"`
	Text        string `json:"text"`
	TGMessageID int    `json:"tg_message_id"` // required for "edit"
}

// SendResponse is published after a TG message is created.
type SendResponse struct {
	ID          int `json:"id"`
	TGMessageID int `json:"tg_message_id"`
}

func getUpdates(token string, offset int) ([]Update, error) {
	apiURL := fmt.Sprintf("%s%s/getUpdates?offset=%d&timeout=30", telegramAPI, token, offset)

	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result UpdatesResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if !result.OK {
		return nil, fmt.Errorf("telegram API returned not OK")
	}

	return result.Result, nil
}

func sendTGMessage(token string, chatID int64, text string) (int, error) {
	apiURL := fmt.Sprintf("%s%s/sendMessage", telegramAPI, token)

	payload := map[string]any{
		"chat_id": chatID,
		"text":    text,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}

	resp, err := http.Post(apiURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var result TGSendResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, err
	}

	if !result.OK {
		return 0, fmt.Errorf("telegram sendMessage returned not OK: %s", respBody)
	}

	return result.Result.MessageID, nil
}

func editTGMessage(token string, chatID int64, messageID int, text string) error {
	apiURL := fmt.Sprintf("%s%s/editMessageText", telegramAPI, token)

	payload := map[string]any{
		"chat_id":    chatID,
		"message_id": messageID,
		"text":       text,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(apiURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var result TGSendResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return err
	}

	if !result.OK {
		return fmt.Errorf("telegram editMessageText returned not OK: %s", respBody)
	}

	return nil
}

// declareQueue создаёт очередь при старте через amqp091 напрямую.
// go-rabbitmq не декларирует очереди на стороне publisher-а.
func declareQueue(rabbitURL, queueName string) error {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	return err
}

func publishToRabbit(publisher *rabbitmq.Publisher, queueName string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	fmt.Printf("Published to %s: %s\n", queueName, body)

	return publisher.Publish(
		body,
		[]string{queueName},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsPersistentDelivery,
	)
}

func startSendConsumer(conn *rabbitmq.Conn, publisher *rabbitmq.Publisher, botToken, tgCommandsQueue, tgCommandsResponsesQueue string) error {
	consumer, err := rabbitmq.NewConsumer(
		conn,
		tgCommandsQueue,
		rabbitmq.WithConsumerOptionsQueueDurable,
	)
	if err != nil {
		return err
	}

	handler := func(d rabbitmq.Delivery) rabbitmq.Action {
		var cmd SendCommand
		if err := json.Unmarshal(d.Body, &cmd); err != nil {
			log.Printf("Failed to parse SendCommand: %v", err)
			return rabbitmq.NackDiscard
		}

		switch cmd.Action {
		case "create":
			tgID, err := sendTGMessage(botToken, cmd.ChatID, cmd.Text)
			if err != nil {
				log.Printf("Failed to send TG message (cmd_id=%d): %v", cmd.ID, err)
				return rabbitmq.NackRequeue
			}
			log.Printf("Created TG message: cmd_id=%d tg_message_id=%d", cmd.ID, tgID)

			resp := SendResponse{ID: cmd.ID, TGMessageID: tgID}
			if err := publishToRabbit(publisher, tgCommandsResponsesQueue, resp); err != nil {
				log.Printf("Failed to publish SendResponse: %v", err)
			}

		case "update":
			if err := editTGMessage(botToken, cmd.ChatID, cmd.TGMessageID, cmd.Text); err != nil {
				log.Printf("Failed to edit TG message (cmd_id=%d tg_message_id=%d): %v", cmd.ID, cmd.TGMessageID, err)
				return rabbitmq.NackRequeue
			}
			log.Printf("Edited TG message: cmd_id=%d tg_message_id=%d", cmd.ID, cmd.TGMessageID)

		default:
			log.Printf("Unknown action %q in SendCommand (cmd_id=%d)", cmd.Action, cmd.ID)
			return rabbitmq.NackDiscard
		}

		return rabbitmq.Ack
	}

	go func() {
		log.Printf("Send consumer started, listening on %s", tgCommandsQueue)
		if err := consumer.Run(handler); err != nil {
			log.Printf("Send consumer error: %v", err)
		}
	}()

	return nil
}

func main() {
	_ = godotenv.Load()
	botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	rabbitURL := os.Getenv("RABBITMQ_URL")
	tgEventsQueue := os.Getenv("RABBITMQ_TG_EVENTS_QUEUE")
	tgCommandsQueue := os.Getenv("RABBITMQ_TG_COMMANDS_QUEUE")
	tgCommandsResponsesQueue := os.Getenv("RABBITMQ_TG_COMMANDS_RESPONSES_QUEUE")

	var ignoreUserID int64
	if raw := os.Getenv("TELEGRAM_IGNORE_USER_ID"); raw != "" {
		if id, err := strconv.ParseInt(raw, 10, 64); err == nil {
			ignoreUserID = id
		}
	}

	for _, q := range []string{tgEventsQueue, tgCommandsQueue, tgCommandsResponsesQueue} {
		if q == "" {
			continue
		}
		if err := declareQueue(rabbitURL, q); err != nil {
			log.Fatalf("Failed to declare queue %q: %v", q, err)
		}
	}

	conn, err := rabbitmq.NewConn(
		rabbitURL,
		rabbitmq.WithConnectionOptionsLogging,
	)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogging,
	)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Close()

	if tgCommandsQueue != "" && tgCommandsResponsesQueue != "" {
		if err := startSendConsumer(conn, publisher, botToken, tgCommandsQueue, tgCommandsResponsesQueue); err != nil {
			log.Fatalf("Failed to start send consumer: %v", err)
		}
	}

	offset := 0
	for {
		updates, err := getUpdates(botToken, offset)
		if err != nil {
			log.Printf("Error getting updates: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, update := range updates {
			if update.UpdateID >= offset {
				offset = update.UpdateID + 1
			}

			var msg *Message
			isEdit := false

			switch {
			case update.Message != nil && update.Message.Text != "":
				msg = update.Message
			case update.EditedMessage != nil && update.EditedMessage.Text != "":
				msg = update.EditedMessage
				isEdit = true
			default:
				continue
			}

			if ignoreUserID != 0 && msg.From != nil && msg.From.ID == ignoreUserID {
				continue
			}

			author := msg.From.FirstName
			if msg.From.Username != "" {
				author = "@" + msg.From.Username
			}

			rabbitMsg := RabbitMessage{
				MessageID: msg.MessageID,
				ChatID:    msg.Chat.ID,
				Text:      msg.Text,
				Author:    author,
				Timestamp: time.Unix(msg.Date, 0).Format(time.RFC3339),
				IsEdit:    isEdit,
			}

			if err := publishToRabbit(publisher, tgEventsQueue, rabbitMsg); err != nil {
				log.Printf("Failed to publish message from %s: %v", author, err)
			} else {
				log.Printf("Published: chat_id=%d author=%s is_edit=%v text=%q", rabbitMsg.ChatID, author, isEdit, rabbitMsg.Text)
			}
		}
	}
}
