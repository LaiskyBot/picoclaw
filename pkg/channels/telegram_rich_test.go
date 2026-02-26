package channels

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mymmrac/telego"
	"github.com/stretchr/testify/require"

	"github.com/sipeed/picoclaw/pkg/bus"
)

func TestBuildTelegramInlineKeyboard(t *testing.T) {
	replyMarkup := buildTelegramInlineKeyboard([]bus.OutboundButton{
		{Text: "A", CallbackData: "a", Row: 1},
		{Text: "B", URL: "https://example.com", Row: 0},
		{Text: "C", CallbackData: "c", Row: 1},
	})

	require.NotNil(t, replyMarkup)
	require.Len(t, replyMarkup.InlineKeyboard, 2)
	require.Len(t, replyMarkup.InlineKeyboard[0], 1)
	require.Equal(t, "B", replyMarkup.InlineKeyboard[0][0].Text)
	require.Len(t, replyMarkup.InlineKeyboard[1], 2)
	require.Equal(t, "A", replyMarkup.InlineKeyboard[1][0].Text)
	require.Equal(t, "C", replyMarkup.InlineKeyboard[1][1].Text)
}

func TestToTelegramInputFile(t *testing.T) {
	fileIDInput, cleanup, err := toTelegramInputFile(bus.OutboundAttachment{FileID: "file-id-1"})
	require.NoError(t, err)
	defer cleanup()
	require.Equal(t, "file-id-1", fileIDInput.FileID)

	urlInput, cleanup, err := toTelegramInputFile(bus.OutboundAttachment{URL: "https://example.com/a.png"})
	require.NoError(t, err)
	defer cleanup()
	require.Equal(t, "https://example.com/a.png", urlInput.URL)

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "sample.txt")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o600))

	pathInput, cleanup, err := toTelegramInputFile(bus.OutboundAttachment{Path: path})
	require.NoError(t, err)
	require.NotNil(t, pathInput.File)
	cleanup()
}

func TestApplyTelegramReplyMarkupToSendMessage(t *testing.T) {
	t.Run("nil markup keeps reply markup unset", func(t *testing.T) {
		params := &telego.SendMessageParams{}
		applyTelegramReplyMarkupToSendMessage(params, nil)
		require.Nil(t, params.ReplyMarkup)
	})

	t.Run("non nil markup is assigned", func(t *testing.T) {
		params := &telego.SendMessageParams{}
		replyMarkup := &telego.InlineKeyboardMarkup{
			InlineKeyboard: [][]telego.InlineKeyboardButton{{
				{Text: "A", CallbackData: "a"},
			}},
		}

		applyTelegramReplyMarkupToSendMessage(params, replyMarkup)
		require.NotNil(t, params.ReplyMarkup)
		assigned, ok := params.ReplyMarkup.(*telego.InlineKeyboardMarkup)
		require.True(t, ok)
		require.Equal(t, replyMarkup, assigned)
	})
}

func TestApplyTelegramReplyMarkupToEditMessage(t *testing.T) {
	t.Run("nil markup keeps reply markup unset", func(t *testing.T) {
		params := &telego.EditMessageTextParams{}
		applyTelegramReplyMarkupToEditMessage(params, nil)
		require.Nil(t, params.ReplyMarkup)
	})

	t.Run("non nil markup is assigned", func(t *testing.T) {
		params := &telego.EditMessageTextParams{}
		replyMarkup := &telego.InlineKeyboardMarkup{
			InlineKeyboard: [][]telego.InlineKeyboardButton{{
				{Text: "A", CallbackData: "a"},
			}},
		}

		applyTelegramReplyMarkupToEditMessage(params, replyMarkup)
		require.NotNil(t, params.ReplyMarkup)
		require.Equal(t, replyMarkup, params.ReplyMarkup)
	})
}

func TestMarkdownToTelegramHTML_LinkWithUnderscoresPreserved(t *testing.T) {
	input := "See [blog_laisky](https://blog.laisky.com/pages/0/?a=b_c&d=e_f) for details."
	got := markdownToTelegramHTML(input)

	require.Contains(t, got, `<a href="https://blog.laisky.com/pages/0/?a=b_c&amp;d=e_f">blog_laisky</a>`)
	require.NotContains(t, got, "<i>")
	require.NotContains(t, got, "</i>")
}

func TestMarkdownToTelegramHTML_ImageMarkdownWithUnderscores(t *testing.T) {
	input := "![blog_laisky](https://example.com/blog_laisky.png)"
	got := markdownToTelegramHTML(input)

	require.Equal(t, `<a href="https://example.com/blog_laisky.png">blog_laisky</a>`, got)
}

func TestMarkdownToTelegramHTML_ItalicStillWorksOutsideLinks(t *testing.T) {
	input := "hello _world_ [link_text](https://example.com/a_b)"
	got := markdownToTelegramHTML(input)

	require.Contains(t, got, "hello <i>world</i>")
	require.Contains(t, got, `<a href="https://example.com/a_b">link_text</a>`)
	require.Equal(t, 1, strings.Count(got, "<i>"))
	require.Equal(t, 1, strings.Count(got, "</i>"))
}

func TestExtractTelegramInlineImageAttachments_LocalPathAndRemoteURL(t *testing.T) {
	input := "Here is screenshot:\n![blog_page.png](/home/bot/.picoclaw/workspace/blog_page.png)\nAnd remote: ![remote_png](https://example.com/path/a_b.png)"

	cleaned, attachments := extractTelegramInlineImageAttachments(input)

	require.Equal(t, "Here is screenshot:\n\nAnd remote:", cleaned)
	require.Len(t, attachments, 2)

	require.Equal(t, "photo", attachments[0].Type)
	require.Equal(t, "/home/bot/.picoclaw/workspace/blog_page.png", attachments[0].Path)
	require.Equal(t, "blog_page.png", attachments[0].Caption)

	require.Equal(t, "photo", attachments[1].Type)
	require.Equal(t, "https://example.com/path/a_b.png", attachments[1].URL)
	require.Equal(t, "remote_png", attachments[1].Caption)
}

func TestExtractTelegramInlineImageAttachments_NormalLinkUnchanged(t *testing.T) {
	input := "See [docs](https://example.com/docs_a_b)"

	cleaned, attachments := extractTelegramInlineImageAttachments(input)

	require.Equal(t, input, cleaned)
	require.Empty(t, attachments)
}

func TestMarkdownImageToOutboundAttachment_FileURISource(t *testing.T) {
	attachment, ok := markdownImageToOutboundAttachment("file:///tmp/screen_shot.png", "screen_shot")

	require.True(t, ok)
	require.Equal(t, "photo", attachment.Type)
	require.Equal(t, "/tmp/screen_shot.png", attachment.Path)
	require.Equal(t, "screen_shot", attachment.Caption)
}

func TestExtractTelegramHTMLImageAttachments_LocalPathAndRemoteURL(t *testing.T) {
	input := "Before <img src=\"/tmp/blog_page.png\" alt=\"blog page\"/> middle <img alt='remote page' src='https://example.com/a_b.png'> after"

	cleaned, attachments := extractTelegramHTMLImageAttachments(input)

	require.Equal(t, "Before  middle  after", cleaned)
	require.Len(t, attachments, 2)

	require.Equal(t, "photo", attachments[0].Type)
	require.Equal(t, "/tmp/blog_page.png", attachments[0].Path)
	require.Equal(t, "blog page", attachments[0].Caption)

	require.Equal(t, "photo", attachments[1].Type)
	require.Equal(t, "https://example.com/a_b.png", attachments[1].URL)
	require.Equal(t, "remote page", attachments[1].Caption)
}

func TestExtractTelegramHTMLImageAttachments_NoSrcKeepsTag(t *testing.T) {
	input := "prefix <img alt=\"missing src\"> suffix"

	cleaned, attachments := extractTelegramHTMLImageAttachments(input)

	require.Equal(t, input, cleaned)
	require.Empty(t, attachments)
}
