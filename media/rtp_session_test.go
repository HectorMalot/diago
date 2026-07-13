// SPDX-License-Identifier: MPL-2.0
// SPDX-FileCopyrightText: Copyright (c) 2024, Emir Aganovic

package media

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/emiago/sipgo/fakes"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deadlineTimeoutError struct{}

func (deadlineTimeoutError) Error() string   { return "I/O timeout" }
func (deadlineTimeoutError) Timeout() bool   { return true }
func (deadlineTimeoutError) Temporary() bool { return true }

type deadlineAwarePacketConn struct {
	mu sync.Mutex

	writeStarted     chan struct{}
	deadlineExpired  chan struct{}
	writeStartOnce   sync.Once
	expireOnce       sync.Once
	blockFirstWrite  bool
	writeDeadline    time.Time
	successfulWrites int
}

func newDeadlineAwarePacketConn() *deadlineAwarePacketConn {
	return &deadlineAwarePacketConn{
		writeStarted:    make(chan struct{}),
		deadlineExpired: make(chan struct{}),
		blockFirstWrite: true,
	}
}

func (c *deadlineAwarePacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, net.ErrClosed
}

func (c *deadlineAwarePacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	c.mu.Lock()
	block := c.blockFirstWrite
	if block {
		c.blockFirstWrite = false
	}
	deadline := c.writeDeadline
	c.mu.Unlock()

	if block {
		c.writeStartOnce.Do(func() { close(c.writeStarted) })
		<-c.deadlineExpired
		return 0, deadlineTimeoutError{}
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return 0, deadlineTimeoutError{}
	}

	c.mu.Lock()
	c.successfulWrites++
	c.mu.Unlock()
	return len(p), nil
}

func (c *deadlineAwarePacketConn) Close() error { return nil }

func (c *deadlineAwarePacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9877}
}

func (c *deadlineAwarePacketConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	if !t.IsZero() && !time.Now().Before(t) {
		c.expireOnce.Do(func() { close(c.deadlineExpired) })
	}
	return nil
}

func (c *deadlineAwarePacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *deadlineAwarePacketConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	if !t.IsZero() && !time.Now().Before(t) {
		c.expireOnce.Do(func() { close(c.deadlineExpired) })
	}
	return nil
}

func (c *deadlineAwarePacketConn) successfulWriteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.successfulWrites
}

func fakeSession(lport int, rport int, rtpReader io.Reader, rtpWriter io.Writer, rtcpReader io.Reader, rtcpWriter io.Writer) *RTPSession {
	sess := &MediaSession{
		Codecs:    []Codec{CodecAudioAlaw, CodecAudioUlaw},
		Laddr:     net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: lport},
		Raddr:     net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: rport},
		rtcpRaddr: net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: rport + 1},
	}

	rtpConn := &fakes.UDPConn{
		Reader: rtpReader,
		// Reader: bytes.NewBuffer([]byte{}),
		Writers: map[string]io.Writer{
			sess.Raddr.String(): rtpWriter,
		},
	}
	sess.rtpConn = rtpConn

	rtcpConn := &fakes.UDPConn{
		Reader: rtcpReader,
		Writers: map[string]io.Writer{
			sess.rtcpRaddr.String(): rtcpWriter,
		},
	}
	sess.rtcpConn = rtcpConn

	rtpSess := NewRTPSession(sess)

	return rtpSess
}

func pipeRTP(lport int, rport int) (read *RTPSession, write *RTPSession) {
	read1, write1 := io.Pipe()
	readControl2, writeControl2 := io.Pipe()
	rtpSessRead := fakeSession(lport, rport, read1, nil, readControl2, nil)
	rtpSessWrite := fakeSession(rport, lport, nil, write1, nil, writeControl2)
	return rtpSessRead, rtpSessWrite
}

func useEphemeralRTPPorts(t *testing.T) {
	t.Helper()
	portStart, portEnd := RTPPortStart, RTPPortEnd
	portOffset := rtpPortOffset.Load()
	RTPPortStart, RTPPortEnd = 0, 0
	rtpPortOffset.Store(0)
	t.Cleanup(func() {
		RTPPortStart, RTPPortEnd = portStart, portEnd
		rtpPortOffset.Store(portOffset)
	})
}

func TestRTPSessionReading(t *testing.T) {
	// pipeRTP := bytes.NewBuffer([]byte{})

	rtpSessRead, rtpSessWrite := pipeRTP(9876, 1234)

	// Now setup RTP session as reader
	// rtpSess := newRTPSession(rtpRead, NewRTPWriter(rtpRead.Sess))
	rtpSessRead.rtcpTicker = time.NewTicker(1 * time.Hour) // DO NOT TICK
	// rtpSess.rtcpTicker = time.NewTicker(500 * time.Millisecond) // Make fast rtcp

	// 1 means good, 0 means sequence number skipepd
	rtpStream := []int{
		1, 1, 1, 1, 1,
		0, 1, 0, 0, 1,
		1, 1, 1, 1, 1,
	}

	rtpWriter := NewRTPPacketWriterSession(rtpSessWrite)
	go func() {
		// Setup remote session
		// defer rtpWrite.Sess.Close()

		payload := make([]byte, 160)

		for _, b := range rtpStream {
			switch b {
			case 1:
			case 0:
				rtpWriter.seqWriter.NextSeqNumber()
			}

			_, err := rtpWriter.Write(payload)
			assert.NoError(t, err)
		}
	}()

	rtpReader := NewRTPPacketReaderSession(rtpSessRead)
	readBuf := make([]byte, 1500)
	for i := 0; i < len(rtpStream); i++ {
		_, err := rtpReader.Read(readBuf)
		if err != nil {
			break
		}
	}

	// stream pkts + 3 -1 as increase of seq number
	lostPackets := 2
	expectedPkts := len(rtpStream) + lostPackets

	// rtpSess.readStats.firstPktSequenceNumber
	assert.Equal(t, len(rtpStream), int(rtpSessRead.readStats.IntervalPacketsCount))
	// assert.Equal(t, Npkts, int(rtpSess.readStats.intervalTotalPackets))

	// Now make a sender report
	senderReport := rtcp.SenderReport{}
	rtpSessRead.parseSenderReport(&senderReport, time.Now(), 1234)

	recReport := senderReport.Reports[0]
	assert.Equal(t, uint32(1234), senderReport.SSRC)
	assert.Equal(t, lostPackets, int(recReport.TotalLost))
	// firstpkt + expected pkts = last seq numb
	assert.Equal(t, int(rtpSessRead.readStats.FirstPktSequenceNumber)+expectedPkts, int(recReport.LastSequenceNumber))
	assert.Equal(t, int(float32(lostPackets)/float32(expectedPkts)*256), int(recReport.FractionLost))
}

func TestRTPSessionWriting(t *testing.T) {
	// pipeRTP := bytes.NewBuffer([]byte{})

	rtpSessRead, rtpSessWrite := pipeRTP(9876, 1234)

	// Now setup RTP session as reader
	rtpSessRead.rtcpTicker = time.NewTicker(1 * time.Hour) // DO NOT TICK
	// rtpSess.rtcpTicker = time.NewTicker(500 * time.Millisecond) // Make fast rtcp

	// 1 means good, 0 means sequence number skipepd
	rtpStream := []int{
		1, 1, 1, 1, 1,
		0, 1, 0, 0, 1,
		1, 1, 1, 1, 1,
	}

	rtpReader := NewRTPPacketReaderSession(rtpSessRead)
	go func() {
		readBuf := make([]byte, 1500)
		for i := 0; i < len(rtpStream); i++ {
			_, err := rtpReader.Read(readBuf)
			if err != nil {
				break
			}
		}
	}()

	rtpWriter := NewRTPPacketWriterSession(rtpSessWrite)
	payload := make([]byte, 160)
	for _, b := range rtpStream {
		switch b {
		case 1:
		case 0:
			rtpWriter.seqWriter.NextSeqNumber()
		}

		_, err := rtpWriter.Write(payload)
		assert.NoError(t, err)
	}

	// stream pkts + 3 -1 as increase of seq number
	// lostPackets := 2
	// expectedPkts := len(rtpStream) + lostPackets

	// rtpSess.readStats.firstPktSequenceNumber
	// assert.Equal(t, len(rtpStream), int(rtpSess.readStats.intervalTotalPackets))
	// assert.Equal(t, Npkts, int(rtpSess.readStats.intervalTotalPackets))

	// Now make a sender report
	senderReport := rtcp.SenderReport{}
	fmt.Println(rtpSessWrite.writeStats.lastPacketTime, time.Now())
	now := time.Now()
	rtpSessWrite.parseSenderReport(&senderReport, now, rtpWriter.SSRC)

	N := len(rtpStream)
	assert.Equal(t, rtpWriter.SSRC, senderReport.SSRC)
	assert.Equal(t, uint32(N), senderReport.PacketCount, "packets=%d ", senderReport.PacketCount)
	assert.Equal(t, uint32(N)*160, senderReport.OctetCount, "octes=%d", senderReport.OctetCount)
	assert.LessOrEqual(t, rtpWriter.initTimestamp+uint32(N-1)*160, senderReport.RTPTime, "RTPTime=%d", int(senderReport.RTPTime))

	// recReport := senderReport.Reports[0]
	// assert.Equal(t, lostPackets, int(recReport.TotalLost))
	// firstpkt + expected pkts = last seq numb
	// assert.Equal(t, int(rtpSess.readStats.firstPktSequenceNumber)+expectedPkts, int(recReport.LastSequenceNumber))
	// assert.Equal(t, int(float32(lostPackets)/float32(expectedPkts)*256), int(recReport.FractionLost))
}

// func TestRTPSessionMonitoring(t *testing.T) {
// 	// LSR and DLSR calc
// 	// SenderReport sent and SenderReport received
// 	rtcpReader, rtcpWriter := io.Pipe()
// 	rtpRawReader, rtpRawWriter := io.Pipe()
// 	rtpR, rtpW := fakeSession(1234, 9876, nil, rtpRawWriter, rtcpReader, nil)

// 	rtpSess := newRTPSession(rtpR, rtpW)
// 	rtpSess.Monitor()

// 	// How to trigger RTCP with sent data
// 	rtpSess.Write()
// 	sr := rtcp.SenderReport{
// 		SSRC: 10,
// 	}
// 	data, _ := sr.Marshal()
// 	rtcpWriter.Write(data)

// }

func TestRTPSessionClose(t *testing.T) {
	useEphemeralRTPPorts(t)
	sess, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sess.Close())
	})

	rtpSess := NewRTPSession(sess)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		rtpSess.readRTCP()
	}()

	time.Sleep(100 * time.Millisecond)
	require.NoError(t, rtpSess.Close())

	select {
	case <-time.After(3 * time.Second):
		t.Error("RTP Session did not close RTCP")
		return
	case <-closed:
	}

	require.NoError(t, rtpSess.Close())
	err = rtpSess.readRTCP()
	require.ErrorIs(t, err, net.ErrClosed)
}

func TestRTPSessionCloseInterruptsActiveWriteAndForkCanWrite(t *testing.T) {
	conn := newDeadlineAwarePacketConn()
	mediaSession := &MediaSession{
		Codecs:    []Codec{CodecAudioAlaw, CodecAudioUlaw},
		Raddr:     net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234},
		rtcpRaddr: net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1235},
		rtcpConn:  conn,
	}
	rtpSession := NewRTPSession(mediaSession)
	rtpSession.writeStats = RTPWriteStats{
		SSRC:                0x12345678,
		lastPacketTime:      time.Now(),
		lastPacketTimestamp: 160,
		sampleRate:          8000,
		PacketsCount:        1,
		OctetCount:          160,
	}

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- rtpSession.writeRTCP(time.Now())
	}()
	select {
	case <-conn.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("RTCP write did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- rtpSession.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt active RTCP write I/O")
	}
	select {
	case err := <-writeDone:
		require.ErrorIs(t, err, net.ErrClosed)
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt active RTCP socket I/O")
	}

	nextMedia := mediaSession.Fork()
	nextMedia.SetRemoteAddr(&mediaSession.Raddr)
	nextRTP := NewRTPSession(nextMedia)
	nextRTP.writeStats = rtpSession.writeStats
	require.NoError(t, nextRTP.MonitorBackground())
	require.NoError(t, nextRTP.writeRTCP(time.Now()))
	require.Equal(t, 1, conn.successfulWriteCount())
	require.NoError(t, nextRTP.Close())
}

func TestRTPSessionMonitorContinuesAfterFork(t *testing.T) {
	useEphemeralRTPPorts(t)
	local, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	remote, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, local.Close())
		require.NoError(t, remote.Close())
	})

	local.SetRemoteAddr(&remote.Laddr)
	currentMedia := local
	currentRTP := NewRTPSession(currentMedia)
	require.NoError(t, currentRTP.MonitorBackground())

	for i := range 2 {
		nextMedia := currentMedia.Fork()
		nextMedia.SetRemoteAddr(&remote.Laddr)
		retiredRTP := currentRTP
		require.NoError(t, retiredRTP.Close())

		nextRTP := NewRTPSession(nextMedia)
		nextRTP.rtcpTicker.Stop()
		nextRTP.rtcpTicker = time.NewTicker(10 * time.Millisecond)
		require.NoError(t, nextRTP.MonitorBackground())
		require.NoError(t, retiredRTP.Close())

		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    CodecAudioAlaw.PayloadType,
				SequenceNumber: uint16(i + 1),
				Timestamp:      uint32((i + 1) * 160),
				SSRC:           0x12345678,
			},
			Payload: make([]byte, 160),
		}
		require.NoError(t, nextRTP.WriteRTP(pkt))

		require.NoError(t, remote.rtcpConn.SetReadDeadline(time.Now().Add(time.Second)))
		buf := make([]byte, 1600)
		n, _, err := remote.rtcpConn.ReadFrom(buf)
		require.NoErrorf(t, err, "fork %d did not produce RTCP", i+1)
		packets, err := rtcp.Unmarshal(buf[:n])
		require.NoError(t, err)
		require.IsType(t, &rtcp.SenderReport{}, packets[0])

		currentMedia = nextMedia
		currentRTP = nextRTP
	}

	require.NoError(t, currentRTP.Close())
}

func TestRTPSessionForegroundMonitorDoesNotPoisonFork(t *testing.T) {
	useEphemeralRTPPorts(t)
	local, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	remote, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, local.Close())
		require.NoError(t, remote.Close())
	})

	local.SetRemoteAddr(&remote.Laddr)
	retiredRTP := NewRTPSession(local)
	retiredRTP.rtcpTicker.Stop()
	retiredRTP.rtcpTicker = time.NewTicker(10 * time.Millisecond)
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseCallback) }) })
	retiredRTP.OnWriteRTCP(func(rtcp.Packet, RTPWriteStats) {
		close(callbackStarted)
		<-releaseCallback
	})
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:     2,
			PayloadType: CodecAudioAlaw.PayloadType,
			Timestamp:   160,
			SSRC:        0x12345678,
		},
		Payload: make([]byte, 160),
	}
	require.NoError(t, retiredRTP.WriteRTP(pkt))

	monitorDone := make(chan error, 1)
	go func() { monitorDone <- retiredRTP.Monitor() }()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("foreground RTCP writer callback did not start")
	}

	// The callback is outside the socket-I/O barrier, so Close can retire this
	// session while the foreground Monitor is still unwinding its writer path.
	require.NoError(t, retiredRTP.Close())
	nextMedia := local.Fork()
	nextMedia.SetRemoteAddr(&remote.Laddr)
	nextRTP := NewRTPSession(nextMedia)
	reportReceived := make(chan struct{})
	var reportOnce sync.Once
	nextRTP.OnReadRTCP(func(rtcp.Packet, RTPReadStats) {
		reportOnce.Do(func() { close(reportReceived) })
	})
	require.NoError(t, nextRTP.MonitorBackground())

	releaseOnce.Do(func() { close(releaseCallback) })
	select {
	case err := <-monitorDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("foreground Monitor did not finish after Close")
	}

	report, err := (&rtcp.ReceiverReport{SSRC: 0x87654321}).Marshal()
	require.NoError(t, err)
	_, err = remote.rtcpConn.WriteTo(report, nextMedia.rtcpConn.LocalAddr())
	require.NoError(t, err)
	select {
	case <-reportReceived:
	case <-time.After(time.Second):
		t.Fatal("retired foreground Monitor poisoned the fork's RTCP reader deadline")
	}
	require.NoError(t, nextRTP.Close())
}

func TestRTPSessionCloseDoesNotWaitForCallback(t *testing.T) {
	useEphemeralRTPPorts(t)
	local, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	remote, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, local.Close())
		require.NoError(t, remote.Close())
	})

	local.SetRemoteAddr(&remote.Laddr)
	rtpSession := NewRTPSession(local)
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	callbackDone := make(chan struct{})
	rtpSession.OnReadRTCP(func(rtcp.Packet, RTPReadStats) {
		close(callbackStarted)
		<-releaseCallback
		close(callbackDone)
	})
	require.NoError(t, rtpSession.MonitorBackground())

	report, err := (&rtcp.ReceiverReport{SSRC: 0x87654321}).Marshal()
	require.NoError(t, err)
	_, err = remote.rtcpConn.WriteTo(report, local.rtcpConn.LocalAddr())
	require.NoError(t, err)
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("RTCP callback did not start")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- rtpSession.Close()
	}()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(releaseCallback)
		t.Fatal("Close waited for the RTCP callback")
	}

	select {
	case <-callbackDone:
		t.Fatal("RTCP callback returned before it was released")
	default:
	}
	close(releaseCallback)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("RTCP callback did not return after it was released")
	}
}

func TestRTPSessionOnReadRTCPMayCloseSession(t *testing.T) {
	useEphemeralRTPPorts(t)
	local, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	remote, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, local.Close())
		require.NoError(t, remote.Close())
	})

	local.SetRemoteAddr(&remote.Laddr)
	rtpSession := NewRTPSession(local)
	callbackClosed := make(chan error, 1)
	rtpSession.OnReadRTCP(func(rtcp.Packet, RTPReadStats) {
		callbackClosed <- rtpSession.Close()
	})
	require.NoError(t, rtpSession.MonitorBackground())

	report, err := (&rtcp.ReceiverReport{SSRC: 0x87654321}).Marshal()
	require.NoError(t, err)
	_, err = remote.rtcpConn.WriteTo(report, local.rtcpConn.LocalAddr())
	require.NoError(t, err)
	select {
	case err := <-callbackClosed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked inside the RTCP read callback")
	}
	_, err = remote.rtcpConn.WriteTo(report, local.rtcpConn.LocalAddr())
	require.NoError(t, err)
	select {
	case <-callbackClosed:
		t.Fatal("RTCP reader re-entered socket I/O after Close")
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, rtpSession.Close())
}

func TestRTPSessionOnWriteRTCPMayCloseWithoutWriting(t *testing.T) {
	useEphemeralRTPPorts(t)
	local, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	remote, err := NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, local.Close())
		require.NoError(t, remote.Close())
	})

	local.SetRemoteAddr(&remote.Laddr)
	rtpSession := NewRTPSession(local)
	rtpSession.rtcpTicker.Stop()
	rtpSession.rtcpTicker = time.NewTicker(10 * time.Millisecond)
	callbackClosed := make(chan error, 1)
	rtpSession.OnWriteRTCP(func(rtcp.Packet, RTPWriteStats) {
		callbackClosed <- rtpSession.Close()
	})
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:     2,
			PayloadType: CodecAudioAlaw.PayloadType,
			Timestamp:   160,
			SSRC:        0x12345678,
		},
		Payload: make([]byte, 160),
	}
	require.NoError(t, rtpSession.WriteRTP(pkt))
	require.NoError(t, rtpSession.MonitorBackground())

	select {
	case err := <-callbackClosed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked inside the RTCP write callback")
	}

	require.NoError(t, remote.rtcpConn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	buf := make([]byte, 1600)
	_, _, err = remote.rtcpConn.ReadFrom(buf)
	var netErr net.Error
	require.True(t, errors.As(err, &netErr))
	require.True(t, netErr.Timeout())
	require.NoError(t, rtpSession.Close())
}

func TestRTPSessionOnReadRTCPConcurrentUpdate(t *testing.T) {
	rtpSession := &RTPSession{}
	callbackA := func(rtcp.Packet, RTPReadStats) {}
	callbackB := func(rtcp.Packet, RTPReadStats) {}
	rtpSession.OnReadRTCP(callbackA)
	report := &rtcp.ReceiverReport{SSRC: 0x87654321}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range 10_000 {
			rtpSession.OnReadRTCP(callbackA)
			rtpSession.OnReadRTCP(callbackB)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 10_000 {
			rtpSession.readRTCPPacket(report)
		}
	}()

	close(start)
	wg.Wait()
}

func TestRTPSessionOnWriteRTCPConcurrentUpdate(t *testing.T) {
	rtpSession := fakeSession(9876, 1234, nil, io.Discard, nil, io.Discard)
	rtpSession.writeStats = RTPWriteStats{
		SSRC:                0x12345678,
		lastPacketTime:      time.Now(),
		lastPacketTimestamp: 160,
		sampleRate:          8000,
		PacketsCount:        1,
		OctetCount:          160,
	}
	callbackA := func(rtcp.Packet, RTPWriteStats) {}
	callbackB := func(rtcp.Packet, RTPWriteStats) {}
	rtpSession.OnWriteRTCP(callbackA)

	start := make(chan struct{})
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range 10_000 {
			rtpSession.OnWriteRTCP(callbackA)
			rtpSession.OnWriteRTCP(callbackB)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for range 10_000 {
			if err := rtpSession.writeRTCP(time.Now()); err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
		}
	}()

	close(start)
	wg.Wait()
	select {
	case err := <-errs:
		require.NoError(t, err)
	default:
	}
}

func TestRTTCalc(t *testing.T) {
	now := time.Now()
	lsrTime := now.Add(-6 * time.Second)
	lsrNTP := NTPTimestamp(lsrTime)
	lsr := uint32(lsrNTP >> 16)

	dur, skewed := calcRTT(now, lsr, 0)
	assert.False(t, skewed)
	assert.Equal(t, 6*time.Second, dur)

	dur, skewed = calcRTT(now, lsr, 5*65356) // Delay was 5 second
	assert.False(t, skewed)
	// Due to dividing this can not be exact
	assert.GreaterOrEqual(t, dur, 1*time.Second)
	assert.LessOrEqual(t, dur, 1*time.Second+20*time.Millisecond)
}

func TestJitterCalc(t *testing.T) {
	stats := RTPReadStats{
		SampleRate: 8000,
	}

	now := time.Now()
	stats.firstRTPTime = now
	stats.firstRTPTimestamp = 160
	stats.calcJitter(now.Add(20*time.Millisecond), 160*2)
	assert.EqualValues(t, 0, int(stats.jitter))

	stats.calcJitter(now.Add(40*time.Millisecond), 160*3)
	assert.EqualValues(t, 0, int(stats.jitter))

	stats.calcJitter(now.Add(75*time.Millisecond), 160*4)
	assert.EqualValues(t, 7, int(stats.jitter))

	stats.calcJitter(now.Add(80*time.Millisecond), 160*5)
	assert.EqualValues(t, 14, int(stats.jitter))

	stats.calcJitter(now.Add(100*time.Millisecond), 160*6)
	assert.EqualValues(t, 13, int(stats.jitter))

	// Simulate a gap
	// stats.calcJitterRFC(now.Add(5*time.Second), 640)
	// assert.EqualValues(t, 0, int(stats.jitter))
}

func TestRTPSessionSourceLockProtection(t *testing.T) {
	// slog.SetLogLoggerLevel(slog.LevelDebug)
	// RTPDebug = true

	rtpSessRead, rtpSessWrite := pipeRTP(9876, 1234)
	rtpSessRead.sourceLock = true // Enable source locking

	go func() {
		var seq uint16 = 1
		for ; seq < 5; seq++ {
			pkt := rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					SequenceNumber: seq,
				},
				Payload: []byte{1, 2, 3},
			}
			rtpSessWrite.WriteRTP(&pkt)
		}
	}()

	pkt := rtp.Packet{}
	_, err := rtpSessRead.ReadRTP(make([]byte, 1600), &pkt)
	require.NoError(t, err)

	assert.Equal(t, uint16(4), pkt.SequenceNumber)
}
