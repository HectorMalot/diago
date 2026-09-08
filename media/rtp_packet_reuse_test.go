package media

import (
	"bytes"
	"io"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/srtp/v3"
	"github.com/stretchr/testify/require"
)

// framedReader returns exactly one queued datagram per Read, mimicking a UDP
// socket (one packet per ReadFrom) rather than a byte stream.
type framedReader struct{ frames [][]byte }

func (f *framedReader) Read(p []byte) (int, error) {
	if len(f.frames) == 0 {
		return 0, io.EOF
	}
	fr := f.frames[0]
	f.frames = f.frames[1:]
	return copy(p, fr), nil
}

// TestRTPReaderReusedPacketShrinksPayload is a regression test for SRTP DTMF
// loss. RTPPacketReader reuses a single rtp.Packet across reads. On the SRTP
// path, rtpUnmarshalPayload reuses the packet's Payload buffer; if it copies a
// small payload into a larger leftover slice without reslicing, len(Payload)
// stays at the previous (larger) packet's size and the reader's payload-size
// invariant panics ("payload calc do not match"). This bites a small DTMF
// telephone-event packet that follows full-size audio packets, while audio
// itself (uniform size) is unaffected.
func TestRTPReaderReusedPacketShrinksPayload(t *testing.T) {
	profile := srtp.ProtectionProfile(SRTPProfileAes128CmHmacSha1_80)
	keyLen, err := profile.KeyLen()
	require.NoError(t, err)
	saltLen, err := profile.SaltLen()
	require.NoError(t, err)
	masterKey := make([]byte, keyLen)
	masterSalt := make([]byte, saltLen)
	for i := range masterKey {
		masterKey[i] = byte(i + 1)
	}
	for i := range masterSalt {
		masterSalt[i] = byte(i + 100)
	}
	encCtx, err := srtp.CreateContext(masterKey, masterSalt, profile)
	require.NoError(t, err)
	decCtx, err := srtp.CreateContext(masterKey, masterSalt, profile)
	require.NoError(t, err)

	encrypt := func(seq uint16, pt uint8, ext bool, payload []byte) []byte {
		p := rtp.Packet{Header: rtp.Header{
			Version: 2, PayloadType: pt, SequenceNumber: seq,
			Timestamp: uint32(seq) * 160, SSRC: 0x1234,
		}, Payload: payload}
		if ext {
			p.Header.Extension = true
			p.Header.ExtensionProfile = 0xBEDE
			require.NoError(t, p.Header.SetExtension(1, []byte{0xAA, 0xBB}))
		}
		raw, err := p.Marshal()
		require.NoError(t, err)
		enc, err := encCtx.EncryptRTP(nil, raw, nil)
		require.NoError(t, err)
		return enc
	}

	audioPayload := bytes.Repeat([]byte{0x7f}, 160)
	dtmfPayload := []byte{0x05, 0x0a, 0x01, 0xf4} // RFC 4733 telephone-event
	audio := encrypt(100, 9, false, audioPayload)
	dtmf := encrypt(101, 101, true, dtmfPayload)

	sess := fakeMediaSessionReader(0, &framedReader{frames: [][]byte{audio, dtmf}})
	sess.remoteCtxSRTP = decCtx
	reader := newRTPPacketReaderMedia(sess)
	buf := make([]byte, RTPBufSize)

	n1, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(audioPayload), n1)
	require.Equal(t, audioPayload, buf[:n1])

	// Before the fix this panicked: the reused Payload kept len 160 from the
	// audio packet, disagreeing with the DTMF packet's real 4-byte payload.
	n2, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(dtmfPayload), n2)
	require.Equal(t, dtmfPayload, buf[:n2])
	require.Equal(t, len(dtmfPayload), len(reader.packet.Payload))
}
