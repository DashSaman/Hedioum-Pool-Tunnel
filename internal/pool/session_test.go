package pool

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type fakeConn struct{}
func (fakeConn) Read([]byte) (int,error){return 0,nil}
func (fakeConn) Write(b []byte)(int,error){return len(b),nil}
func (fakeConn) Close()error{return nil}
func (fakeConn) LocalAddr()net.Addr{return nil}
func (fakeConn) RemoteAddr()net.Addr{return nil}
func (fakeConn) SetDeadline(time.Time)error{return nil}
func (fakeConn) SetReadDeadline(time.Time)error{return nil}
func (fakeConn) SetWriteDeadline(time.Time)error{return nil}

func TestMonitoredStreamDoesNotThrottle(t *testing.T){
	ys:=&YamuxSession{};ms:=&monitoredStream{Conn:fakeConn{},parent:ys};payload:=make([]byte,8*1024*1024);start:=time.Now();n,err:=ms.Write(payload)
	if err!=nil{t.Fatalf("write: %v",err)};if n!=len(payload){t.Fatalf("write=%d want %d",n,len(payload))};if d:=time.Since(start);d>time.Second{t.Fatalf("payload stalled %v",d)};if got:=atomic.LoadUint64(&ys.bytesTransferred);got!=uint64(len(payload)){t.Fatalf("bytes=%d",got)}
}
func TestPayloadIORefreshesActivity(t *testing.T){ys:=&YamuxSession{};atomic.StoreInt64(&ys.lastActivityUnixNano,time.Now().Add(-5*time.Minute).UnixNano());ms:=&monitoredStream{Conn:fakeConn{},parent:ys};if _,err:=ms.Write([]byte("keepalive"));err!=nil{t.Fatal(err)};if idle:=ys.IdleTime();idle>time.Second{t.Fatalf("idle=%v",idle)}}
func TestPendingOpenReservation(t *testing.T){
	s:=&YamuxSession{}
	if s.PendingOpens()!=0{t.Fatal("fresh pending opens must be zero")}
	s.ReserveOpen();s.ReserveOpen()
	if s.PendingOpens()!=2||s.LoadScore()!=2{t.Fatalf("reservation not reflected: pending=%d load=%d",s.PendingOpens(),s.LoadScore())}
	s.ReleaseOpen();s.ReleaseOpen()
	if s.PendingOpens()!=0{t.Fatalf("reservation leak: %d",s.PendingOpens())}
}
func TestChaosLimitBounds(t *testing.T){
	s:=&YamuxSession{baseLimitMbps:20,jitterMbps:0};s.UpdateChaosLimit();if got:=s.CurrentCap();got!=20{t.Fatalf("cap=%d",got)}
	s=&YamuxSession{baseLimitMbps:20,jitterMbps:-5};s.UpdateChaosLimit();if got:=s.CurrentCap();got!=20{t.Fatalf("negative cap=%d",got)}
	s=&YamuxSession{baseLimitMbps:40,jitterMbps:15};for i:=0;i<500;i++{s.UpdateChaosLimit();c:=s.CurrentCap();if c<25||c>55{t.Fatalf("cap=%d",c)}}
	s=&YamuxSession{baseLimitMbps:3,jitterMbps:10};for i:=0;i<500;i++{s.UpdateChaosLimit();if c:=s.CurrentCap();c<1{t.Fatalf("cap=%d",c)}}
}
func TestGetAndResetBytes(t *testing.T){s:=&YamuxSession{};s.bytesTransferred=4096;if got:=s.GetAndResetBytes();got!=4096{t.Fatalf("got=%d",got)};if got:=s.GetAndResetBytes();got!=0{t.Fatalf("not reset=%d",got)}}
func TestDrainingState(t *testing.T){s:=&YamuxSession{state:StateActive};if !s.IsActive()||s.IsDraining(){t.Fatal("bad initial")};s.SetDraining();if s.IsActive()||!s.IsDraining(){t.Fatal("bad drain")};s.Revive();if !s.IsActive()||s.IsDraining(){t.Fatal("bad revive")}}
