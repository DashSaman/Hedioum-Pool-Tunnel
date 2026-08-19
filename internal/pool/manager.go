package pool

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
)

const (
	defaultMaxConns = 15
	defaultMinConns = 10
	staggerDelay = 500 * time.Millisecond
	healthCheckFreq = 10 * time.Second
	maxTotalFactor = 3
)

func shouldCloseDraining(streams int, _ time.Duration, _ bool) bool { return streams == 0 }

type DialFunc func() (*yamux.Session, string, error)
type PoolStats struct { ActiveConns, DrainingConns, TotalMbps int }

type NodePool struct {
	Alias string
	label string
	TargetIP string
	minConnections, maxConnections int
	baseLimitMbps, jitterMbps int
	dialer DialFunc
	lifecycle LifecyclePolicy
	sessions []*YamuxSession
	mu sync.RWMutex
	currentMbps int32
	replenishing int32
	replenishWanted int32
	shutdown chan struct{}
	stopOnce sync.Once
}

const ( udpMinConns = 2; udpMaxConns = 6 )
type nodePools struct { tcp, udp *NodePool }
type HubManager struct { pools map[string]*nodePools; mu sync.RWMutex }
func NewHubManager() *HubManager { return &HubManager{pools: make(map[string]*nodePools)} }

func newNodePool(cfg config.ForeignNode, label string, minConns, maxConns int, dialer DialFunc, lifecycle LifecyclePolicy) *NodePool {
	np := &NodePool{Alias: cfg.Alias, label: label, TargetIP: cfg.TargetIP, minConnections: minConns, maxConnections: maxConns, baseLimitMbps: cfg.BandwidthLimitMbps, jitterMbps: cfg.BandwidthJitterMbps, dialer: dialer, lifecycle: lifecycle, sessions: make([]*YamuxSession, 0, maxConns), shutdown: make(chan struct{})}
	go np.monitorAndScale(); return np
}
func (hm *HubManager) RegisterNode(cfg config.ForeignNode, dialer DialFunc) {
	minConns := cfg.MinConnections; if minConns < 1 { minConns = defaultMinConns }
	maxConns := cfg.MaxConnections; if maxConns < minConns { maxConns = minConns + 5 }
	p := NewLifecyclePolicy(cfg.AuthToken)
	fresh := &nodePools{tcp: newNodePool(cfg,"tcp",minConns,maxConns,dialer,p), udp: newNodePool(cfg,"udp",udpMinConns,udpMaxConns,dialer,p)}
	hm.mu.Lock(); old := hm.pools[cfg.Alias]; hm.pools[cfg.Alias] = fresh; hm.mu.Unlock()
	if old != nil { old.tcp.stop(); old.udp.stop() }
}
func (hm *HubManager) lookup(alias string) (*nodePools,error) { hm.mu.RLock(); p,ok:=hm.pools[alias]; hm.mu.RUnlock(); if !ok { return nil,errors.New("foreign node pool not found") }; return p,nil }
func (hm *HubManager) GetStreamTCP(alias string)(net.Conn,error){ p,e:=hm.lookup(alias); if e!=nil{return nil,e}; return p.tcp.getStreamLeastLoaded() }
func (hm *HubManager) GetStreamUDP(alias string)(net.Conn,error){ p,e:=hm.lookup(alias); if e!=nil{return nil,e}; return p.udp.getStreamLeastLoaded() }

// reserveLeastLoaded serializes only the tiny selection/reservation operation; the
// network-blocking OpenStream happens after the pool lock is released. Pending
// reservations are part of LoadScore, so a burst of concurrent users is spread
// across physical pipes instead of every goroutine observing the same stale winner.
func (np *NodePool) reserveLeastLoaded() *YamuxSession {
	np.mu.Lock(); defer np.mu.Unlock()
	var best *YamuxSession
	bestLoad := int(^uint(0)>>1)
	var bestBytes uint64 = ^uint64(0)
	for _, s := range np.sessions {
		if s.IsClosed() || !s.IsActive() { continue }
		load, recent := s.LoadScore(), s.RecentBytes()
		if best == nil || load < bestLoad || (load == bestLoad && recent < bestBytes) {
			best, bestLoad, bestBytes = s, load, recent
		}
	}
	if best != nil { best.ReserveOpen() }
	return best
}

func (np *NodePool) getStreamLeastLoaded() (net.Conn,error) {
	np.mu.RLock(); budget := np.activeCountLocked(); np.mu.RUnlock()
	var lastErr error; failed := 0
	for attempt:=0; attempt<budget; attempt++ {
		s := np.reserveLeastLoaded(); if s==nil { break }
		stream,err := s.OpenStream(); s.ReleaseOpen()
		if err==nil { if failed>0 { np.replenishAsync(failed) }; return stream,nil }
		lastErr=err; failed++; _=s.Close()
		slog.Debug("yamux stream open failed; closed pipe and trying another","node",np.Alias,"pool",np.label,"err",err)
	}
	np.mu.RLock(); active:=np.activeCountLocked(); np.mu.RUnlock()
	needed:=np.minConnections-active; if needed<failed { needed=failed }; if needed<1 { needed=1 }; np.replenishAsync(needed)
	if lastErr!=nil { return nil,fmt.Errorf("no usable active connection in pool: %w",lastErr) }
	return nil,errors.New("no active connections available in the pool")
}

func (np *NodePool) monitorAndScale(){ t:=time.NewTicker(healthCheckFreq); defer t.Stop(); np.replenishAsync(np.minConnections); for { select { case <-np.shutdown: np.cleanup(); return; case <-t.C: np.evaluateHealthAndScale() } } }
func (np *NodePool) evaluateHealthAndScale(){
	np.mu.Lock(); retained:=make([]*YamuxSession,0,len(np.sessions)); idleLimit:=time.Duration(rand.Intn(61)+60)*time.Second; scale:=false; active:=np.activeCountLocked(); draining:=np.drainingCountLocked(); total:=0
	for _,s:=range np.sessions {
		if s.IsClosed(){ slog.Info("purged dead physical connection","node",np.Alias,"pool",np.label); continue }
		b:=s.GetAndResetBytes(); mbps:=int((b*8)/(1024*1024*uint64(healthCheckFreq.Seconds()))); total+=mbps; s.UpdateChaosLimit(); cap:=s.CurrentCap()
		if s.IsActive(){
			retire:=s.ShouldRetire()
			if retire && draining<np.maxConnections && active>np.minConnections { s.SetDraining(); active--; draining++; slog.Info("retired: lifecycle budget reached","node",np.Alias,"pool",np.label,"mimic",s.mimicType,"age",s.Age().Round(time.Second),"mb",s.CumulativeBytes()/(1024*1024))
			} else {
				if retire && active<=np.minConnections && active<np.maxConnections { scale=true }
				if mbps>=int(float64(cap)*0.8){ scale=true }
				if !retire && active>np.minConnections && draining<np.maxConnections && s.ActiveStreams()==0 && s.IdleTime()>idleLimit { s.SetDraining(); active--; draining++; slog.Info("scaled down: unused connection draining","node",np.Alias,"pool",np.label) }
			}
		} else if s.IsDraining(){ streams:=s.ActiveStreams(); age:=s.DrainingFor(); if shouldCloseDraining(streams,age,false){ _=s.Close(); draining--; slog.Info("draining complete: connection closed","node",np.Alias,"pool",np.label,"streams",streams,"drained_for",age.Round(time.Second)); continue } }
		retained=append(retained,s)
	}
	np.sessions=retained; atomic.StoreInt32(&np.currentMbps,int32(total)); np.mu.Unlock()
	if scale { np.executeScaleUp() }; np.mu.RLock(); cur:=np.activeCountLocked(); np.mu.RUnlock(); if cur==0 { slog.Warn("watchdog: pool has no active connections — re-warming","node",np.Alias,"pool",np.label) }; if cur<np.minConnections { np.replenishAsync(np.minConnections-cur) }
}
func (np *NodePool) activeCountLocked()int{n:=0;for _,s:=range np.sessions{if s.IsActive()&&!s.IsClosed(){n++}};return n}
func (np *NodePool) drainingCountLocked()int{n:=0;for _,s:=range np.sessions{if s.IsDraining()&&!s.IsClosed(){n++}};return n}
func (np *NodePool) evictOldestSafeDrainingLocked()bool{idx:=-1;var oldest time.Duration;for i,s:=range np.sessions{if !s.IsDraining()||s.ActiveStreams()>0{continue};if d:=s.DrainingFor();idx==-1||d>oldest{idx,oldest=i,d}};if idx==-1{return false};_=np.sessions[idx].Close();slog.Info("evicted empty draining connection to free a slot","node",np.Alias,"pool",np.label,"drained_for",oldest.Round(time.Second));np.sessions=append(np.sessions[:idx],np.sessions[idx+1:]...);return true}
func (np *NodePool) executeScaleUp(){np.mu.RLock();n:=np.activeCountLocked();np.mu.RUnlock();if n<np.maxConnections{np.replenishAsync(1)}}
func (np *NodePool) replenishAsync(n int){if n<=0{return};atomicMaxInt32(&np.replenishWanted,int32(n));if !atomic.CompareAndSwapInt32(&np.replenishing,0,1){return};go np.replenishWorker()}
func atomicMaxInt32(dst *int32,v int32){for{old:=atomic.LoadInt32(dst);if old>=v{return};if atomic.CompareAndSwapInt32(dst,old,v){return}}}
func (np *NodePool) replenishWorker(){for{select{case<-np.shutdown:atomic.StoreInt32(&np.replenishing,0);return;default:};wanted:=int(atomic.SwapInt32(&np.replenishWanted,0));if wanted>0{np.replenishPool(wanted);continue};np.mu.RLock();active:=np.activeCountLocked();np.mu.RUnlock();if active<np.minConnections{atomicMaxInt32(&np.replenishWanted,int32(np.minConnections-active));continue};atomic.StoreInt32(&np.replenishing,0);if atomic.LoadInt32(&np.replenishWanted)==0{return};if !atomic.CompareAndSwapInt32(&np.replenishing,0,1){return}}}
func (np *NodePool) compactClosedLocked(){kept:=np.sessions[:0];for _,s:=range np.sessions{if !s.IsClosed(){kept=append(kept,s)}};np.sessions=kept}
func (np *NodePool) replenishPool(needed int){for i:=0;i<needed;i++{np.mu.RLock();before:=np.activeCountLocked();np.mu.RUnlock();if i>0||before>0{select{case<-np.shutdown:return;case<-time.After(staggerDelay):}};raw,m,err:=np.dialer();if err!=nil||raw==nil{slog.Warn("failed to dial new connection","node",np.Alias,"pool",np.label,"err",err);continue};select{case<-np.shutdown:_=raw.Close();return;default:};w:=NewYamuxSession(raw,np.baseLimitMbps,np.jitterMbps,m,np.lifecycle);np.mu.Lock();np.compactClosedLocked();switch{case np.activeCountLocked()>=np.maxConnections:_=w.Close();case len(np.sessions)>=maxTotalFactor*np.maxConnections:if np.evictOldestSafeDrainingLocked(){np.sessions=append(np.sessions,w)}else{_=w.Close();slog.Warn("session safety ceiling reached; preserving live drainers","node",np.Alias,"pool",np.label,"total",len(np.sessions))};default:np.sessions=append(np.sessions,w);slog.Info("scaled up: dialed new connection","node",np.Alias,"pool",np.label,"active",np.activeCountLocked(),"total",len(np.sessions),"max",np.maxConnections)};np.mu.Unlock()}}
func (hm *HubManager) GetStats(alias string)PoolStats{p,e:=hm.lookup(alias);if e!=nil{return PoolStats{}};a,b:=p.tcp.stats(),p.udp.stats();return PoolStats{a.ActiveConns+b.ActiveConns,a.DrainingConns+b.DrainingConns,a.TotalMbps+b.TotalMbps}}
func (np *NodePool) stats()PoolStats{np.mu.RLock();defer np.mu.RUnlock();a,d:=0,0;for _,s:=range np.sessions{if s.IsClosed(){continue};if s.IsActive(){a++}else if s.IsDraining(){d++}};return PoolStats{a,d,int(atomic.LoadInt32(&np.currentMbps))}}
func (np *NodePool) stop(){np.stopOnce.Do(func(){close(np.shutdown)})}
func (hm *HubManager) Close(){hm.mu.Lock();all:=make([]*nodePools,0,len(hm.pools));for _,p:=range hm.pools{all=append(all,p)};hm.pools=make(map[string]*nodePools);hm.mu.Unlock();for _,p:=range all{p.tcp.stop();p.udp.stop()}}
func (np *NodePool) cleanup(){np.mu.Lock();defer np.mu.Unlock();for _,s:=range np.sessions{_=s.Close()};np.sessions=nil}
