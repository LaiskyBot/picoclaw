package channels

import (
	"os"
	"path/filepath"
	"testing"

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
