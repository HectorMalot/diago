// SPDX-License-Identifier: MPL-2.0
// SPDX-FileCopyrightText: Copyright (c) 2024, Emir Aganovic

package media

import (
	"errors"
	"fmt"
	"io"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

var (
	errRTCPFailedToUnmarshal = errors.New("rtcp: failed to unmarshal")
)

// RTPUnmarshal wrapper for now used for optimizing unmarshal
func RTPUnmarshal(buf []byte, p *rtp.Packet) error {
	return p.Unmarshal(buf)
}

func rtpUnmarshalPayload(n int, buf []byte, p *rtp.Packet) error {
	// NOTE: n is the header size including any RTP header extension (it is
	// derived from pkt.Header.MarshalSize() by the caller). The header has
	// already been fully parsed (e.g. by srtp DecryptRTP), so the extension
	// must be kept intact: it keeps Header.MarshalSize() consistent with the
	// payload offset used below, which the reader's invariant check relies on.
	// The extension does reference the read buffer, but so does the plain
	// pion Unmarshal path, and no code reads Header.Extensions after parsing,
	// so keeping it is consistent and safe.
	end := len(buf)
	if p.Header.Padding {
		p.PaddingSize = buf[end-1]
		end -= int(p.PaddingSize)
	} else {
		// p is reused across reads (RTPPacketReader.packet). pion's full
		// Unmarshal resets PaddingSize when a packet has no padding; this
		// optimized path must do the same, otherwise a stale PaddingSize from
		// an earlier padded packet corrupts the reader's payload-size invariant.
		p.PaddingSize = 0
	}
	if end < n {
		return io.ErrShortBuffer
	}

	payload := buf[n:end]
	// p is reused across reads, so the length of p.Payload must be reset to THIS
	// packet's payload length. Reslicing (not just copying into a leftover larger
	// slice) is required: copying 4 DTMF bytes into a 160-byte audio buffer while
	// leaving len(p.Payload)==160 makes len(p.Payload) disagree with the real
	// payload size, which the reader's "payload calc" invariant then panics on.
	if cap(p.Payload) >= len(payload) {
		p.Payload = p.Payload[:len(payload)]
	} else {
		p.Payload = make([]byte, len(payload))
	}
	copy(p.Payload, payload)
	return nil
}

// RTCPUnmarshal is improved version based on pion/rtcp where we allow caller to define and control
// buffer of rtcp packets. This also reduces one allocation
// NOTE: data is still referenced in packet buffer
func RTCPUnmarshal(data []byte, packets []rtcp.Packet) (n int, err error) {
	for i := 0; i < len(packets) && len(data) != 0; i++ {
		var h rtcp.Header

		err = h.Unmarshal(data)
		if err != nil {
			// fmt.Errorf("unmarshal RTCP error: %w", err)
			return 0, errors.Join(err, errRTCPFailedToUnmarshal)
		}

		pktLen := int(h.Length+1) * 4
		if pktLen > len(data) {
			return 0, fmt.Errorf("packet too short: %w", errRTCPFailedToUnmarshal)
		}
		inPacket := data[:pktLen]

		// Check the type and unmarshal
		packet := rtcpTypedPacket(h.Type)
		err = packet.Unmarshal(inPacket)
		if err != nil {
			return 0, err
		}

		packets[i] = packet

		data = data[pktLen:]
		n++
	}

	return n, nil
}

func rtcpMarshal(packets []rtcp.Packet) ([]byte, error) {
	return rtcp.Marshal(packets)
}

// TODO this would be nice that pion exports
func rtcpTypedPacket(htype rtcp.PacketType) rtcp.Packet {
	// Currently we are not interested

	switch htype {
	case rtcp.TypeSenderReport:
		return new(rtcp.SenderReport)

	case rtcp.TypeReceiverReport:
		return new(rtcp.ReceiverReport)

	case rtcp.TypeSourceDescription:
		return new(rtcp.SourceDescription)

	case rtcp.TypeGoodbye:
		return new(rtcp.Goodbye)

	default:
		return new(rtcp.RawPacket)
	}
}
