package pool

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
)

func fakeDialer() (*yamux.Session, string, error) {
	c, s := net.Pipe()
	go func() {
		srv, err := yamux.Server(s, yamux.DefaultConfig())
		if err != nil { _ = s.Close(); return }
		defer srv.Close()
		for {
			st, err := srv.AcceptStream(); if err != nil { return }
			go func(){ defer st.Close(); _,_ = io.Copy(io.Discard,st) }()
		}
	}()
	sess, err := yamux.Client(c, yamux.DefaultConfig())
	return sess, "ssh", err
}

func TestSubPoolsTCPandUDP(t *testing.T) {
	hm:=NewHubManager();defer hm.Close()
	cfg:=config.ForeignNode{Alias:"n1",TargetIP:"1.2.3.4",MinConnections:1,MaxConnections:3,BandwidthLimitMbps:50,BandwidthJitterMbps:5}
	hm.RegisterNode(cfg,fakeDialer)
	deadline:=time.Now().Add(10*time.Second)
	for { if hm.GetStats("n1").ActiveConns>=1+udpMinConns{break};if time.Now().After(deadline){t.Fatalf("pools did not warm: %+v",hm.GetStats("n1"))};time.Sleep(100*time.Millisecond) }
	tcp,err:=hm.GetStreamTCP("n1");if err!=nil{t.Fatal(err)};defer tcp.Close()
	udp,err:=hm.GetStreamUDP("n1");if err!=nil{t.Fatal(err)};defer udp.Close()
	if _,err:=hm.GetStreamTCP("unknown");err==nil{t.Fatal("expected unknown node error")}
}

func TestReservationSpreadsBurstAcrossPipes(t *testing.T){
	np:=&NodePool{}
	for i:=0;i<3;i++{sess,_,err:=fakeDialer();if err!=nil{t.Fatal(err)};np.sessions=append(np.sessions,NewYamuxSession(sess,10,0,"ssh",NewLifecyclePolicy("spread")))}
	defer func(){for _,s:=range np.sessions{_=s.Close()}}()
	a:=np.reserveLeastLoaded();b:=np.reserveLeastLoaded();c:=np.reserveLeastLoaded()
	if a==nil||b==nil||c==nil{t.Fatal("missing reservation")}
	defer a.ReleaseOpen();defer b.ReleaseOpen();defer c.ReleaseOpen()
	if a==b||a==c||b==c{t.Fatalf("burst reservations herded onto same pipe: %p %p %p",a,b,c)}
}

func TestAtomicMaxInt32(t *testing.T){var v int32;atomicMaxInt32(&v,3);atomicMaxInt32(&v,2);atomicMaxInt32(&v,7);if v!=7{t.Fatalf("max=%d",v)}}
func TestCompactClosedLocked(t *testing.T){np:=&NodePool{};for i:=0;i<2;i++{sess,_,err:=fakeDialer();if err!=nil{t.Fatal(err)};np.sessions=append(np.sessions,NewYamuxSession(sess,10,0,"ssh",NewLifecyclePolicy("compact")))};defer func(){for _,s:=range np.sessions{_=s.Close()}}();_=np.sessions[0].Close();np.compactClosedLocked();if len(np.sessions)!=1{t.Fatalf("len=%d",len(np.sessions))}}
