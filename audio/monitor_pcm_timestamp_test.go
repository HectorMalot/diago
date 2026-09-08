package audio

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/emiago/diago/media"
	"github.com/stretchr/testify/require"
)

type limitedPCMRecording struct {
	bytes.Buffer
	limit int
}

func (w *limitedPCMRecording) Write(p []byte) (int, error) {
	if len(p) > w.limit-w.Len() {
		return 0, io.ErrShortBuffer
	}
	return w.Buffer.Write(p)
}

func TestMonitorPCMWriteTimestampPrecedesFlush(t *testing.T) {
	codec := media.CodecAudioAlaw
	pcm := bytes.Repeat([]byte{0x55}, codec.Samples16())
	recording := &limitedPCMRecording{limit: len(pcm)}
	flushedAt := time.Now()
	monitor := pcmBufioWriter{
		writer:   bufio.NewWriterSize(recording, len(pcm)),
		codec:    codec,
		silence:  make([]byte, len(pcm)),
		lastTime: flushedAt,
	}

	// A pending read/write captured its timestamp before a concurrent flush.
	require.NoError(t, monitor.writePCM(flushedAt.Add(-time.Millisecond), pcm))
	require.Equal(t, flushedAt, monitor.lastTime)
	require.NoError(t, monitor.writer.Flush())
	require.Equal(t, pcm, recording.Bytes())
}
