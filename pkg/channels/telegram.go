package channels

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mymmrac/telego"
	"github.com/mymmrac/telego/telegohandler"
	th "github.com/mymmrac/telego/telegohandler"
	tu "github.com/mymmrac/telego/telegoutil"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/utils"
	"github.com/sipeed/picoclaw/pkg/voice"
)

type TelegramChannel struct {
	*BaseChannel
	bot          *telego.Bot
	commands     TelegramCommander
	config       *config.Config
	chatIDs      map[string]int64
	transcriber  *voice.GroqTranscriber
	placeholders sync.Map // chatID -> messageID
	stopThinking sync.Map // chatID -> thinkingCancel
}

type thinkingCancel struct {
	fn context.CancelFunc
}

func (c *thinkingCancel) Cancel() {
	if c != nil && c.fn != nil {
		c.fn()
	}
}

func NewTelegramChannel(cfg *config.Config, bus *bus.MessageBus) (*TelegramChannel, error) {
	var opts []telego.BotOption
	telegramCfg := cfg.Channels.Telegram

	if telegramCfg.Proxy != "" {
		proxyURL, parseErr := url.Parse(telegramCfg.Proxy)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid proxy URL %q: %w", telegramCfg.Proxy, parseErr)
		}
		opts = append(opts, telego.WithHTTPClient(&http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyURL(proxyURL),
			},
		}))
	} else if os.Getenv("HTTP_PROXY") != "" || os.Getenv("HTTPS_PROXY") != "" {
		// Use environment proxy if configured
		opts = append(opts, telego.WithHTTPClient(&http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
			},
		}))
	}

	bot, err := telego.NewBot(telegramCfg.Token, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create telegram bot: %w", err)
	}

	base := NewBaseChannel("telegram", telegramCfg, bus, telegramCfg.AllowFrom)

	return &TelegramChannel{
		BaseChannel:  base,
		commands:     NewTelegramCommands(bot, cfg),
		bot:          bot,
		config:       cfg,
		chatIDs:      make(map[string]int64),
		transcriber:  nil,
		placeholders: sync.Map{},
		stopThinking: sync.Map{},
	}, nil
}

func (c *TelegramChannel) SetTranscriber(transcriber *voice.GroqTranscriber) {
	c.transcriber = transcriber
}

func (c *TelegramChannel) Start(ctx context.Context) error {
	logger.InfoC("telegram", "Starting Telegram bot (polling mode)...")

	updates, err := c.bot.UpdatesViaLongPolling(ctx, &telego.GetUpdatesParams{
		Timeout: 30,
	})
	if err != nil {
		return fmt.Errorf("failed to start long polling: %w", err)
	}

	bh, err := telegohandler.NewBotHandler(c.bot, updates)
	if err != nil {
		return fmt.Errorf("failed to create bot handler: %w", err)
	}

	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		c.commands.Help(ctx, message)
		return nil
	}, th.CommandEqual("help"))
	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		return c.commands.Start(ctx, message)
	}, th.CommandEqual("start"))

	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		return c.commands.Show(ctx, message)
	}, th.CommandEqual("show"))

	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		return c.commands.List(ctx, message)
	}, th.CommandEqual("list"))

	bh.HandleMessage(func(ctx *th.Context, message telego.Message) error {
		return c.handleMessage(ctx, &message)
	}, th.AnyMessage())

	bh.HandleCallbackQuery(func(ctx *th.Context, query telego.CallbackQuery) error {
		return c.handleCallbackQuery(ctx, &query)
	}, th.AnyCallbackQuery())

	c.setRunning(true)
	logger.InfoCF("telegram", "Telegram bot connected", map[string]any{
		"username": c.bot.Username(),
	})

	go bh.Start()

	go func() {
		<-ctx.Done()
		bh.Stop()
	}()

	return nil
}

func (c *TelegramChannel) Stop(ctx context.Context) error {
	logger.InfoC("telegram", "Stopping Telegram bot...")
	c.setRunning(false)
	return nil
}

func (c *TelegramChannel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	if !c.IsRunning() {
		return fmt.Errorf("telegram bot not running")
	}

	chatID, err := parseChatID(msg.ChatID)
	if err != nil {
		return fmt.Errorf("invalid chat ID: %w", err)
	}

	c.stopThinkingForChat(msg.ChatID)

	replyMarkup := buildTelegramInlineKeyboard(msg.Buttons)

	if len(msg.Attachments) == 0 {
		return c.sendTextResponse(ctx, chatID, msg.ChatID, msg.Content, replyMarkup)
	}

	if strings.TrimSpace(msg.Content) != "" || replyMarkup != nil {
		if err = c.sendTextResponse(ctx, chatID, msg.ChatID, msg.Content, replyMarkup); err != nil {
			return err
		}
	} else {
		c.clearThinkingPlaceholder(msg.ChatID)
	}

	for idx, attachment := range msg.Attachments {
		if err = c.sendAttachment(ctx, chatID, attachment); err != nil {
			return fmt.Errorf("send attachment[%d]: %w", idx, err)
		}
	}

	logger.InfoCF("telegram", "Sent rich response", map[string]any{
		"chat_id":        msg.ChatID,
		"content_chars":  len(msg.Content),
		"attachment_count": len(msg.Attachments),
		"button_count":   len(msg.Buttons),
	})

	return nil
}

// sendTextResponse sends or edits a text response with optional inline keyboard.
func (c *TelegramChannel) sendTextResponse(
	ctx context.Context,
	chatID int64,
	chatIDStr, content string,
	replyMarkup *telego.InlineKeyboardMarkup,
) error {
	htmlContent := markdownToTelegramHTML(content)
	if strings.TrimSpace(htmlContent) == "" {
		htmlContent = "Please choose an option."
	}

	if pID, ok := c.placeholders.Load(chatIDStr); ok {
		c.placeholders.Delete(chatIDStr)
		editMsg := tu.EditMessageText(tu.ID(chatID), pID.(int), htmlContent)
		editMsg.ParseMode = telego.ModeHTML
		editMsg.ReplyMarkup = replyMarkup

		if _, err := c.bot.EditMessageText(ctx, editMsg); err == nil {
			logger.InfoCF("telegram", "Edited thinking placeholder with final response", map[string]any{
				"chat_id":       chatIDStr,
				"message_id":    pID.(int),
				"content_chars": len(content),
			})
			return nil
		}
	}

	tgMsg := tu.Message(tu.ID(chatID), htmlContent)
	tgMsg.ParseMode = telego.ModeHTML
	tgMsg.ReplyMarkup = replyMarkup

	if _, err := c.bot.SendMessage(ctx, tgMsg); err != nil {
		logger.WarnCF("telegram", "HTML parse failed, retrying plain text", map[string]any{
			"chat_id": chatIDStr,
			"error":   err.Error(),
		})
		tgMsg.ParseMode = ""
		_, err = c.bot.SendMessage(ctx, tgMsg)
		return err
	}

	return nil
}

// sendAttachment sends one Telegram media attachment as photo or document.
func (c *TelegramChannel) sendAttachment(ctx context.Context, chatID int64, attachment bus.OutboundAttachment) error {
	inputFile, cleanup, err := toTelegramInputFile(attachment)
	if err != nil {
		return err
	}
	defer cleanup()

	caption := markdownToTelegramHTML(attachment.Caption)
	attachmentType := strings.ToLower(strings.TrimSpace(attachment.Type))
	if attachmentType == "image" {
		attachmentType = "photo"
	}

	switch attachmentType {
	case "photo":
		params := &telego.SendPhotoParams{
			ChatID:    telego.ChatID{ID: chatID},
			Photo:     inputFile,
			Caption:   caption,
			ParseMode: telego.ModeHTML,
		}
		_, err = c.bot.SendPhoto(ctx, params)
		if err == nil {
			return nil
		}
		params.ParseMode = ""
		_, err = c.bot.SendPhoto(ctx, params)
		return err
	case "document", "file":
		params := &telego.SendDocumentParams{
			ChatID:    telego.ChatID{ID: chatID},
			Document:  inputFile,
			Caption:   caption,
			ParseMode: telego.ModeHTML,
		}
		_, err = c.bot.SendDocument(ctx, params)
		if err == nil {
			return nil
		}
		params.ParseMode = ""
		_, err = c.bot.SendDocument(ctx, params)
		return err
	default:
		return fmt.Errorf("unsupported attachment type %q", attachment.Type)
	}
}

// toTelegramInputFile converts an outbound attachment source to Telegram input format.
func toTelegramInputFile(attachment bus.OutboundAttachment) (telego.InputFile, func(), error) {
	if attachment.FileID != "" {
		return telego.InputFile{FileID: attachment.FileID}, func() {}, nil
	}

	if attachment.URL != "" {
		return telego.InputFile{URL: attachment.URL}, func() {}, nil
	}

	if attachment.Path != "" {
		file, err := os.Open(attachment.Path)
		if err != nil {
			return telego.InputFile{}, func() {}, fmt.Errorf("open attachment path %q: %w", attachment.Path, err)
		}
		cleanup := func() {
			_ = file.Close()
		}
		return telego.InputFile{File: file}, cleanup, nil
	}

	return telego.InputFile{}, func() {}, fmt.Errorf("attachment source is empty")
}

// buildTelegramInlineKeyboard creates Telegram inline keyboard markup grouped by button row.
func buildTelegramInlineKeyboard(buttons []bus.OutboundButton) *telego.InlineKeyboardMarkup {
	if len(buttons) == 0 {
		return nil
	}

	rows := map[int][]telego.InlineKeyboardButton{}
	for _, btn := range buttons {
		if strings.TrimSpace(btn.Text) == "" {
			continue
		}

		row := btn.Row
		if row < 0 {
			row = 0
		}

		keyboardButton := telego.InlineKeyboardButton{Text: btn.Text}
		if btn.URL != "" {
			keyboardButton.URL = btn.URL
		} else if btn.CallbackData != "" {
			keyboardButton.CallbackData = btn.CallbackData
		} else {
			continue
		}

		rows[row] = append(rows[row], keyboardButton)
	}

	if len(rows) == 0 {
		return nil
	}

	rowIndexes := make([]int, 0, len(rows))
	for row := range rows {
		rowIndexes = append(rowIndexes, row)
	}
	sort.Ints(rowIndexes)

	inlineRows := make([][]telego.InlineKeyboardButton, 0, len(rowIndexes))
	for _, row := range rowIndexes {
		inlineRows = append(inlineRows, rows[row])
	}

	return &telego.InlineKeyboardMarkup{InlineKeyboard: inlineRows}
}

// stopThinkingForChat stops active thinking indicators for a chat.
func (c *TelegramChannel) stopThinkingForChat(chatID string) {
	if stop, ok := c.stopThinking.Load(chatID); ok {
		if cf, ok := stop.(*thinkingCancel); ok && cf != nil {
			cf.Cancel()
		}
		c.stopThinking.Delete(chatID)
	}
}

// clearThinkingPlaceholder removes placeholder tracking state for a chat.
func (c *TelegramChannel) clearThinkingPlaceholder(chatID string) {
	c.placeholders.Delete(chatID)
}

// sendThinkingPlaceholder sends Telegram typing state plus a temporary thinking message.
func (c *TelegramChannel) sendThinkingPlaceholder(ctx context.Context, chatID int64, senderID string) {
	err := c.bot.SendChatAction(ctx, tu.ChatAction(tu.ID(chatID), telego.ChatActionTyping))
	if err != nil {
		logger.ErrorCF("telegram", "Failed to send chat action", map[string]any{
			"error": err.Error(),
		})
	}

	chatIDStr := fmt.Sprintf("%d", chatID)
	c.stopThinkingForChat(chatIDStr)

	_, thinkCancel := context.WithTimeout(ctx, 5*time.Minute)
	c.stopThinking.Store(chatIDStr, &thinkingCancel{fn: thinkCancel})

	pMsg, err := c.bot.SendMessage(ctx, tu.Message(tu.ID(chatID), "Thinking... 💭"))
	if err == nil {
		c.placeholders.Store(chatIDStr, pMsg.MessageID)
		return
	}

	logger.WarnCF("telegram", "Failed to send thinking placeholder", map[string]any{
		"chat_id":   chatIDStr,
		"sender_id": senderID,
		"error":     err.Error(),
	})
}

// handleCallbackQuery processes inline keyboard callback actions and publishes them to the agent bus.
func (c *TelegramChannel) handleCallbackQuery(ctx context.Context, query *telego.CallbackQuery) error {
	if query == nil {
		return fmt.Errorf("callback query is nil")
	}

	from := query.From
	senderID := fmt.Sprintf("%d", from.ID)
	if from.Username != "" {
		senderID = fmt.Sprintf("%d|%s", from.ID, from.Username)
	}

	if !c.IsAllowed(senderID) {
		logger.DebugCF("telegram", "Callback query rejected by allowlist", map[string]any{"user_id": senderID})
		return nil
	}

	if err := c.bot.AnswerCallbackQuery(ctx, &telego.AnswerCallbackQueryParams{
		CallbackQueryID: query.ID,
		Text:            "Received",
	}); err != nil {
		logger.WarnCF("telegram", "Failed to answer callback query", map[string]any{"error": err.Error()})
	}

	chatID := query.Message.GetChat().ID
	if chatID == 0 {
		logger.WarnCF("telegram", "Callback query has no chat ID", map[string]any{
			"callback_query_id": query.ID,
			"sender_id":         senderID,
		})
		return nil
	}

	messageID := query.Message.GetMessageID()
	content := "[button callback]"
	if strings.TrimSpace(query.Data) != "" {
		content = fmt.Sprintf("[button callback: %s]", query.Data)
	}

	sendChatID := fmt.Sprintf("%d", chatID)
	c.sendThinkingPlaceholder(ctx, chatID, senderID)

	metadata := map[string]string{
		"message_id":        fmt.Sprintf("%d", messageID),
		"user_id":           fmt.Sprintf("%d", from.ID),
		"username":          from.Username,
		"first_name":        from.FirstName,
		"is_group":          fmt.Sprintf("%t", query.Message.GetChat().Type != "private"),
		"peer_kind":         map[bool]string{true: "group", false: "direct"}[query.Message.GetChat().Type != "private"],
		"peer_id":           map[bool]string{true: sendChatID, false: fmt.Sprintf("%d", from.ID)}[query.Message.GetChat().Type != "private"],
		"callback_query_id": query.ID,
		"callback_data":     query.Data,
		"is_callback":       "true",
	}

	c.HandleMessage(senderID, sendChatID, content, nil, metadata)
	return nil
}

func (c *TelegramChannel) handleMessage(ctx context.Context, message *telego.Message) error {
	if message == nil {
		return fmt.Errorf("message is nil")
	}

	user := message.From
	if user == nil {
		return fmt.Errorf("message sender (user) is nil")
	}

	senderID := fmt.Sprintf("%d", user.ID)
	if user.Username != "" {
		senderID = fmt.Sprintf("%d|%s", user.ID, user.Username)
	}

	// check allowlist to avoid downloading attachments for rejected users
	if !c.IsAllowed(senderID) {
		logger.DebugCF("telegram", "Message rejected by allowlist", map[string]any{
			"user_id": senderID,
		})
		return nil
	}

	chatID := message.Chat.ID
	c.chatIDs[senderID] = chatID

	content := ""
	mediaPaths := []string{}
	localFiles := []string{} // track local files that need cleanup

	// ensure temp files are cleaned up when function returns
	defer func() {
		for _, file := range localFiles {
			if err := os.Remove(file); err != nil {
				logger.DebugCF("telegram", "Failed to cleanup temp file", map[string]any{
					"file":  file,
					"error": err.Error(),
				})
			}
		}
	}()

	if message.Text != "" {
		content += message.Text
	}

	if message.Caption != "" {
		if content != "" {
			content += "\n"
		}
		content += message.Caption
	}

	if len(message.Photo) > 0 {
		photo := message.Photo[len(message.Photo)-1]
		photoPath := c.downloadPhoto(ctx, photo.FileID)
		if photoPath != "" {
			localFiles = append(localFiles, photoPath)
			mediaPaths = append(mediaPaths, photoPath)
			if content != "" {
				content += "\n"
			}
			content += "[image: photo]"
		}
	}

	if message.Voice != nil {
		voicePath := c.downloadFile(ctx, message.Voice.FileID, ".ogg")
		if voicePath != "" {
			localFiles = append(localFiles, voicePath)
			mediaPaths = append(mediaPaths, voicePath)

			var transcribedText string
			if c.transcriber != nil && c.transcriber.IsAvailable() {
				transcriberCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()

				result, err := c.transcriber.Transcribe(transcriberCtx, voicePath)
				if err != nil {
					logger.ErrorCF("telegram", "Voice transcription failed", map[string]any{
						"error": err.Error(),
						"path":  voicePath,
					})
					transcribedText = "[voice (transcription failed)]"
				} else {
					transcribedText = fmt.Sprintf("[voice transcription: %s]", result.Text)
					logger.InfoCF("telegram", "Voice transcribed successfully", map[string]any{
						"text": result.Text,
					})
				}
			} else {
				transcribedText = "[voice]"
			}

			if content != "" {
				content += "\n"
			}
			content += transcribedText
		}
	}

	if message.Audio != nil {
		audioPath := c.downloadFile(ctx, message.Audio.FileID, ".mp3")
		if audioPath != "" {
			localFiles = append(localFiles, audioPath)
			mediaPaths = append(mediaPaths, audioPath)
			if content != "" {
				content += "\n"
			}
			content += "[audio]"
		}
	}

	if message.Document != nil {
		docPath := c.downloadFile(ctx, message.Document.FileID, "")
		if docPath != "" {
			localFiles = append(localFiles, docPath)
			mediaPaths = append(mediaPaths, docPath)
			if content != "" {
				content += "\n"
			}
			content += "[file]"
		}
	}

	if content == "" {
		content = "[empty message]"
	}

	logger.DebugCF("telegram", "Received message", map[string]any{
		"sender_id": senderID,
		"chat_id":   fmt.Sprintf("%d", chatID),
		"preview":   utils.Truncate(content, 50),
	})

	c.sendThinkingPlaceholder(ctx, chatID, senderID)
	chatIDStr := fmt.Sprintf("%d", chatID)

	peerKind := "direct"
	peerID := fmt.Sprintf("%d", user.ID)
	if message.Chat.Type != "private" {
		peerKind = "group"
		peerID = fmt.Sprintf("%d", chatID)
	}

	metadata := map[string]string{
		"message_id": fmt.Sprintf("%d", message.MessageID),
		"user_id":    fmt.Sprintf("%d", user.ID),
		"username":   user.Username,
		"first_name": user.FirstName,
		"is_group":   fmt.Sprintf("%t", message.Chat.Type != "private"),
		"peer_kind":  peerKind,
		"peer_id":    peerID,
	}

	c.HandleMessage(senderID, fmt.Sprintf("%d", chatID), content, mediaPaths, metadata)
	logger.InfoCF("telegram", "Published inbound message to bus", map[string]any{
		"chat_id":     chatIDStr,
		"sender_id":   senderID,
		"content_chars": len(content),
		"media_count": len(mediaPaths),
	})
	return nil
}

func (c *TelegramChannel) downloadPhoto(ctx context.Context, fileID string) string {
	file, err := c.bot.GetFile(ctx, &telego.GetFileParams{FileID: fileID})
	if err != nil {
		logger.ErrorCF("telegram", "Failed to get photo file", map[string]any{
			"error": err.Error(),
		})
		return ""
	}

	return c.downloadFileWithInfo(file, ".jpg")
}

func (c *TelegramChannel) downloadFileWithInfo(file *telego.File, ext string) string {
	if file.FilePath == "" {
		return ""
	}

	url := c.bot.FileDownloadURL(file.FilePath)
	logger.DebugCF("telegram", "File URL", map[string]any{"url": url})

	// Use FilePath as filename for better identification
	filename := file.FilePath + ext
	return utils.DownloadFile(url, filename, utils.DownloadOptions{
		LoggerPrefix: "telegram",
	})
}

func (c *TelegramChannel) downloadFile(ctx context.Context, fileID, ext string) string {
	file, err := c.bot.GetFile(ctx, &telego.GetFileParams{FileID: fileID})
	if err != nil {
		logger.ErrorCF("telegram", "Failed to get file", map[string]any{
			"error": err.Error(),
		})
		return ""
	}

	return c.downloadFileWithInfo(file, ext)
}

func parseChatID(chatIDStr string) (int64, error) {
	var id int64
	_, err := fmt.Sscanf(chatIDStr, "%d", &id)
	return id, err
}

func markdownToTelegramHTML(text string) string {
	if text == "" {
		return ""
	}

	codeBlocks := extractCodeBlocks(text)
	text = codeBlocks.text

	inlineCodes := extractInlineCodes(text)
	text = inlineCodes.text

	text = regexp.MustCompile(`^#{1,6}\s+(.+)$`).ReplaceAllString(text, "$1")

	text = regexp.MustCompile(`^>\s*(.*)$`).ReplaceAllString(text, "$1")

	text = escapeHTML(text)

	text = regexp.MustCompile(`\[([^\]]+)\]\(([^)]+)\)`).ReplaceAllString(text, `<a href="$2">$1</a>`)

	text = regexp.MustCompile(`\*\*(.+?)\*\*`).ReplaceAllString(text, "<b>$1</b>")

	text = regexp.MustCompile(`__(.+?)__`).ReplaceAllString(text, "<b>$1</b>")

	reItalic := regexp.MustCompile(`_([^_]+)_`)
	text = reItalic.ReplaceAllStringFunc(text, func(s string) string {
		match := reItalic.FindStringSubmatch(s)
		if len(match) < 2 {
			return s
		}
		return "<i>" + match[1] + "</i>"
	})

	text = regexp.MustCompile(`~~(.+?)~~`).ReplaceAllString(text, "<s>$1</s>")

	text = regexp.MustCompile(`^[-*]\s+`).ReplaceAllString(text, "• ")

	for i, code := range inlineCodes.codes {
		escaped := escapeHTML(code)
		text = strings.ReplaceAll(text, fmt.Sprintf("\x00IC%d\x00", i), fmt.Sprintf("<code>%s</code>", escaped))
	}

	for i, code := range codeBlocks.codes {
		escaped := escapeHTML(code)
		text = strings.ReplaceAll(
			text,
			fmt.Sprintf("\x00CB%d\x00", i),
			fmt.Sprintf("<pre><code>%s</code></pre>", escaped),
		)
	}

	return text
}

type codeBlockMatch struct {
	text  string
	codes []string
}

func extractCodeBlocks(text string) codeBlockMatch {
	re := regexp.MustCompile("```[\\w]*\\n?([\\s\\S]*?)```")
	matches := re.FindAllStringSubmatch(text, -1)

	codes := make([]string, 0, len(matches))
	for _, match := range matches {
		codes = append(codes, match[1])
	}

	i := 0
	text = re.ReplaceAllStringFunc(text, func(m string) string {
		placeholder := fmt.Sprintf("\x00CB%d\x00", i)
		i++
		return placeholder
	})

	return codeBlockMatch{text: text, codes: codes}
}

type inlineCodeMatch struct {
	text  string
	codes []string
}

func extractInlineCodes(text string) inlineCodeMatch {
	re := regexp.MustCompile("`([^`]+)`")
	matches := re.FindAllStringSubmatch(text, -1)

	codes := make([]string, 0, len(matches))
	for _, match := range matches {
		codes = append(codes, match[1])
	}

	i := 0
	text = re.ReplaceAllStringFunc(text, func(m string) string {
		placeholder := fmt.Sprintf("\x00IC%d\x00", i)
		i++
		return placeholder
	})

	return inlineCodeMatch{text: text, codes: codes}
}

func escapeHTML(text string) string {
	text = strings.ReplaceAll(text, "&", "&amp;")
	text = strings.ReplaceAll(text, "<", "&lt;")
	text = strings.ReplaceAll(text, ">", "&gt;")
	return text
}
