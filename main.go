package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
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

type RabbitMessage struct {
	MessageID int    `json:"message_id"`
	ChatID    int64  `json:"chat_id"`
	Text      string `json:"text"`
	Author    string `json:"author"`
	Timestamp string `json:"timestamp"`
	IsEdit    bool   `json:"is_edit"`
}

func getUpdates(token string) ([]Update, error) {
	apiURL := fmt.Sprintf("%s%s/getUpdates", telegramAPI, token)

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

func publishToRabbit(ch *amqp.Channel, queueName string, msg RabbitMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Published: %s\n", body)

	return ch.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	rabbitURL := os.Getenv("RABBITMQ_URL")
	queueName := os.Getenv("RABBITMQ_QUEUE")

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
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
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	updates, err := getUpdates(botToken)
	if err != nil {
		log.Fatalf("Error getting updates: %v", err)
	}

	for _, update := range updates {
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

		if err := publishToRabbit(ch, queueName, rabbitMsg); err != nil {
			log.Printf("Failed to publish message from %s: %v", author, err)
		} else {
			log.Printf("Published: chat_id=%d author=%s is_edit=%v text=%q", rabbitMsg.ChatID, author, isEdit, rabbitMsg.Text)
		}
	}
}
