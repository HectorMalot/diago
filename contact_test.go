package diago

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/emiago/diago/testdata"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContactTransport(t *testing.T) {
	for _, tc := range []struct {
		name, transport, externalHost, scheme, param string
		noSIPS                                       bool
	}{
		{"TLS external hostname", "tls", "sip.example.com", "sip", "tls", true},
		{"TLS matching hosts", "tls", "127.0.0.1", "sip", "tls", true},
		{"SIPS external hostname", "tls", "sip.example.com", "sips", "", false},
		{"SIPS matching hosts", "tls", "127.0.0.1", "sips", "tls", false},
		{"TCP external hostname", "tcp", "sip.example.com", "sip", "", false},
		{"TCP matching hosts", "tcp", "127.0.0.1", "sip", "tcp", false},
		{"UDP external hostname", "udp", "sip.example.com", "sip", "", false},
		{"UDP matching hosts", "udp", "127.0.0.1", "sip", "udp", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ua, err := sipgo.NewUA()
			require.NoError(t, err)
			t.Cleanup(func() { ua.Close() })
			tran := Transport{
				Transport: tc.transport, BindHost: "127.0.0.1", BindPort: 5061,
				ExternalHost: tc.externalHost, ExternalPort: 5061, TLSURINoSIPS: tc.noSIPS,
			}
			if tc.transport == "tls" {
				tran.TLSConf = &tls.Config{}
			}
			dg := NewDiago(ua, WithTransport(tran))
			d, err := dg.NewDialog(sip.Uri{Scheme: "sip", Host: "127.0.0.1"}, NewDialogOptions{Transport: tc.transport})
			require.NoError(t, err)
			t.Cleanup(func() { d.Close() })
			contact := d.UA.ContactHDR.Address
			assert.Equal(t, tc.scheme, contact.Scheme)
			assert.Equal(t, tc.externalHost, contact.Host)
			assert.Equal(t, 5061, contact.Port)
			assert.Equal(t, tc.param, contact.UriParams.GetOr("transport", ""))
		})
	}
}

func TestTLSInviteContact(t *testing.T) {
	peerUA, err := sipgo.NewUA()
	require.NoError(t, err)
	t.Cleanup(func() { peerUA.Close() })
	peer, err := sipgo.NewServer(peerUA)
	require.NoError(t, err)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{testdata.ServerCertificate()}})
	require.NoError(t, err)
	contacts := make(chan string, 1)
	peer.OnInvite(func(req *sip.Request, tx sip.ServerTransaction) {
		contacts <- req.Contact().Value()
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBusyHere, "Busy Here", nil))
	})
	peer.OnAck(func(*sip.Request, sip.ServerTransaction) {})
	done := make(chan struct{})
	go func() { defer close(done); _ = peer.ServeTLS(ln) }()
	t.Cleanup(func() { _ = ln.Close(); <-done })

	ua, err := sipgo.NewUA(sipgo.WithUserAgent("test-gateway"),
		sipgo.WithUserAgenTLSConfig(&tls.Config{InsecureSkipVerify: true})) // Self-signed loopback peer.
	require.NoError(t, err)
	t.Cleanup(func() { ua.Close() })
	dg := NewDiago(ua, WithTransport(Transport{
		Transport: "tls", BindHost: "127.0.0.1", BindPort: 5061,
		ExternalHost: "sip.example.com", ExternalPort: 5061,
		TLSConf: &tls.Config{}, TLSURINoSIPS: true,
	}))
	d, err := dg.NewDialog(sip.Uri{Scheme: "sip", User: "alice", Host: "127.0.0.1", Port: ln.Addr().(*net.TCPAddr).Port}, NewDialogOptions{Transport: "tls"})
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = d.Invite(ctx, InviteClientOptions{})
	require.ErrorContains(t, err, "486")
	select {
	case contact := <-contacts:
		assert.Equal(t, "<sip:test-gateway@sip.example.com:5061;transport=tls>", contact)
	case <-ctx.Done():
		t.Fatal("peer did not receive the TLS INVITE")
	}
}
