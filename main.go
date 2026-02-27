package main

import "os"

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
	UpdateID int      `json:"update_id"`
	Message  *Message `json:"message"`
}

type UpdatesResponse struct {
	OK     bool     `json:"ok"`
	Result []Update `json:"result"`
}

func main() {
	botToken := os.Getenv("TELEGRAM_BOT_TOKEN")
	chatID := os.Getenv("TELEGRAM_CHAT_ID")

}
