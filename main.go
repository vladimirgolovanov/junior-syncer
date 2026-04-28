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

type RabbitMessage struct {
	MessageID int    `json:"message_id"`
	ChatID    int64  `json:"chat_id"`
	Text      string `json:"text"`
	Author    string `json:"author"`
	Timestamp string `json:"timestamp"`
	IsEdit    bool   `json:"is_edit"`
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

func publishToRabbit(publisher *rabbitmq.Publisher, queueName string, msg RabbitMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Published: %s\n", body)

	return publisher.Publish(
		body,
		[]string{queueName},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsPersistentDelivery,
	)
}

func main() {
	_ = godotenv.Load()
	botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	rabbitURL := os.Getenv("RABBITMQ_URL")
	queueName := os.Getenv("RABBITMQ_QUEUE")

	if err := declareQueue(rabbitURL, queueName); err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
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

			if err := publishToRabbit(publisher, queueName, rabbitMsg); err != nil {
				log.Printf("Failed to publish message from %s: %v", author, err)
			} else {
				log.Printf("Published: chat_id=%d author=%s is_edit=%v text=%q", rabbitMsg.ChatID, author, isEdit, rabbitMsg.Text)
			}
		}
	}
}
