// SPDX-License-Identifier: MPL-2.0
// SPDX-FileCopyrightText: Copyright (c) 2024, Emir Aganovic

package diago

import (
	"net"
	"testing"
	"time"

	"github.com/emiago/diago/media"
	"github.com/pion/rtcp"
	"github.com/stretchr/testify/require"
)

func TestDialogMediaRTCPCallbackDoesNotBlockSessionRetirement(t *testing.T) {
	local, err := media.NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	remote, err := media.NewMediaSession(net.IPv4(127, 0, 0, 1), 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, local.Close())
		require.NoError(t, remote.Close())
	})

	local.SetRemoteAddr(&remote.Laddr)
	remote.SetRemoteAddr(&local.Laddr)
	rtpSession := media.NewRTPSession(local)
	dialog := &DialogMedia{}
	dialog.initRTPSessionUnsafe(local, rtpSession)

	callbackStarted := make(chan struct{})
	enterDialog := make(chan struct{})
	callbackDone := make(chan struct{})
	rtpSession.OnReadRTCP(func(rtcp.Packet, media.RTPReadStats) {
		close(callbackStarted)
		<-enterDialog
		dialog.MediaSession()
		close(callbackDone)
	})
	require.NoError(t, rtpSession.MonitorBackground())
	require.NoError(t, remote.WriteRTCP(&rtcp.ReceiverReport{SSRC: 0x87654321}))

	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("RTCP callback did not start")
	}

	closeDone := make(chan error, 1)
	go func() {
		dialog.mu.Lock()
		close(enterDialog)
		err := rtpSession.MonitorClose()
		dialog.mu.Unlock()
		closeDone <- err
	}()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("RTCP session retirement deadlocked with the dialog callback")
	}

	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("RTCP callback did not resume after the dialog lock was released")
	}
}
