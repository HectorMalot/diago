// SPDX-License-Identifier: MPL-2.0
// SPDX-FileCopyrightText: Copyright (c) 2024, Emir Aganovic

package media

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/emiago/sipgo/fakes"
	"github.com/pion/rtp"
	"github.com/pion/srtp/v3"
	"github.com/stretchr/testify/require"
)

func fakeMediaSessionReader(lport int, rtpReader io.Reader) *MediaSession {
	sess := &MediaSession{
		Codecs: []Codec{CodecAudioAlaw, CodecAudioUlaw},
		Laddr:  net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: lport},
	}

	conn := &fakes.UDPConn{
		Reader: rtpReader,
	}
	sess.rtpConn = conn
	return sess
}

func TestRTPReader(t *testing.T) {
	rtpConn := bytes.NewBuffer([]byte{})
	sess := fakeMediaSessionReader(0, rtpConn)
	rtpSess := NewRTPSession(sess)
	rtpReader := NewRTPPacketReaderSession(rtpSess)

	payload := []byte("12312313")
	N := 10
	buf := make([]byte, 3200)
	for i := 0; i < N; i++ {
		writePkt := rtp.Packet{
			Header: rtp.Header{
				SSRC:           1234,
				Version:        2,
				PayloadType:    8,
				SequenceNumber: uint16(i),
				Timestamp:      160 * uint32(i),
				Marker:         i == 0,
			},
			Payload: payload,
		}
		data, _ := writePkt.Marshal()
		rtpConn.Reset()
		rtpConn.Write(data)
		// conn.Reader = bytes.NewBuffer(data)

		n, err := rtpReader.Read(buf)
		require.NoError(t, err)

		pkt := rtpReader.PacketHeader
		require.Equal(t, writePkt.PayloadType, pkt.PayloadType)
		require.Equal(t, writePkt.SSRC, pkt.SSRC)
		require.Equal(t, i == 0, pkt.Marker)
		require.Equal(t, len(payload), n)
		require.Equal(t, rtpReader.seqReader.ReadExtendedSeq(), uint64(writePkt.SequenceNumber))
	}
}

// TestRTPReaderSRTPWithExtension is a regression test for SRTP DTMF handling.
// Telephone-event (DTMF) packets carry an RTP header extension. The optimized
// SRTP unmarshal path used to strip the extension from the parsed header AFTER
// slicing the payload at the with-extension offset. That made the reader's
// payload-size invariant (rtpN - Header.MarshalSize() - PaddingSize == len(Payload))
// disagree by the extension length and panic, so the packet was dropped and the
// DTMF digit never reached the application.
func TestRTPReaderSRTPWithExtension(t *testing.T) {
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

	// Two contexts created from the same keying material: one encrypts (peer),
	// one decrypts (us), mirroring a negotiated SRTP session deterministically.
	encCtx, err := srtp.CreateContext(masterKey, masterSalt, profile)
	require.NoError(t, err)
	decCtx, err := srtp.CreateContext(masterKey, masterSalt, profile)
	require.NoError(t, err)

	// Payload resembling an RFC 4733 telephone-event (DTMF) frame.
	payload := []byte{0x05, 0x0a, 0x01, 0xf4}
	writePkt := rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    101,
			SequenceNumber: 4242,
			Timestamp:      160000,
			SSRC:           0xdeadbeef,
			Marker:         true,
		},
		Payload: payload,
	}
	writePkt.Header.Extension = true
	writePkt.Header.ExtensionProfile = 0xBEDE
	require.NoError(t, writePkt.Header.SetExtension(1, []byte{0xAA, 0xBB}))

	raw, err := writePkt.Marshal()
	require.NoError(t, err)

	encrypted, err := encCtx.EncryptRTP(nil, raw, nil)
	require.NoError(t, err)

	sess := fakeMediaSessionReader(0, bytes.NewBuffer(encrypted))
	sess.remoteCtxSRTP = decCtx

	reader := newRTPPacketReaderMedia(sess)
	buf := make([]byte, RTPBufSize)

	// Before the fix this panicked ("payload calc do not match").
	n, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.Equal(t, payload, buf[:n])

	hdr := reader.PacketHeader
	require.Equal(t, writePkt.PayloadType, hdr.PayloadType)
	require.Equal(t, writePkt.SSRC, hdr.SSRC)
	require.True(t, hdr.Marker)
	// The extension is preserved (consistent with the plain-RTP unmarshal path).
	require.True(t, hdr.Extension)
}

func BenchmarkRTPPacketReader(b *testing.B) {
	rtpConn := bytes.NewBuffer([]byte{})
	sess := fakeMediaSessionReader(0, rtpConn)
	rtpSess := NewRTPSession(sess)
	rtpReader := NewRTPPacketReaderSession(rtpSess)

	payload := []byte("12312313")
	buf := make([]byte, 3200)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writePkt := rtp.Packet{
			Header: rtp.Header{
				SSRC:           1234,
				Version:        2,
				PayloadType:    8,
				SequenceNumber: uint16(i % (1 << 16)),
				Timestamp:      160 * uint32(i),
				Marker:         i == 0,
			},
			Payload: payload,
		}
		data, _ := writePkt.Marshal()
		rtpConn.Write(data)

		_, err := rtpReader.Read(buf)
		require.NoError(b, err)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/s")
}
