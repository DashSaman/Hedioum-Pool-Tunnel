// Package securestream implements Hedioum's authenticated, encrypted wire protocol.
package securestream

import (
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net"
	"sync"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

const (
	saltSize  = 32
	keySize   = chacha20poly1305.KeySize
	tagSize   = 16
	nonceSize = chacha20poly1305.NonceSize
	maxChunk  = 0x3FFF
	maxPad    = 255
	lenHdrLen = 4
	magicSize = 8
	authPlain = magicSize
	hkdfInfo  = "hedioum-aead-v1"
	hsDeadline = 10 * time.Second
)

var magic = [magicSize]byte{'H','E','D','I','O','U','M','1'}
var ErrAuth = errors.New("securestream: authentication failed")

type SecureConn struct {
	net.Conn
	reader io.Reader

	readMu sync.Mutex
	rAEAD cipher.AEAD
	rNonce [nonceSize]byte
	// leftover aliases payBuf. This is safe because Read never calls readChunk again
	// until leftover has been completely consumed; keeping the slice in-place avoids
	// allocating/copying up to ~16 KiB whenever Yamux asks for a small header first.
	leftover []byte
	lenBuf []byte
	payBuf []byte
	padBuf []byte

	writeMu sync.Mutex
	wAEAD cipher.AEAD
	wNonce [nonceSize]byte
	frameBf []byte
}

func deriveKey(psk,salt []byte)([]byte,error){ return hkdf.Key(sha256.New,psk,salt,hkdfInfo,keySize) }
func incrementNonce(n *[nonceSize]byte){ for i:=0;i<nonceSize;i++{ n[i]++; if n[i]!=0{return} } }

func ClientHandshake(conn net.Conn,r io.Reader,token string)(*SecureConn,error){
	_ = conn.SetDeadline(time.Now().Add(hsDeadline)); defer conn.SetDeadline(time.Time{})
	psk:=[]byte(token); saltC:=make([]byte,saltSize); if _,err:=rand.Read(saltC);err!=nil{return nil,err}; if _,err:=conn.Write(saltC);err!=nil{return nil,err}
	wKey,err:=deriveKey(psk,saltC);if err!=nil{return nil,err};wAEAD,err:=chacha20poly1305.New(wKey);if err!=nil{return nil,err}
	sc:=newSecureConn(conn,r);sc.wAEAD=wAEAD;if err:=sc.writeChunk(magic[:]);err!=nil{return nil,fmt.Errorf("send auth frame: %w",err)}
	saltS:=make([]byte,saltSize);if _,err:=io.ReadFull(r,saltS);err!=nil{return nil,fmt.Errorf("read server salt: %w",err)};rKey,err:=deriveKey(psk,saltS);if err!=nil{return nil,err};rAEAD,err:=chacha20poly1305.New(rKey);if err!=nil{return nil,err};sc.rAEAD=rAEAD;return sc,nil
}

func ServerHandshake(conn net.Conn,r io.Reader,token string,filter *ReplayFilter)(*SecureConn,error){
	_ = conn.SetDeadline(time.Now().Add(hsDeadline)); defer conn.SetDeadline(time.Time{})
	psk:=[]byte(token);saltC:=make([]byte,saltSize);if _,err:=io.ReadFull(r,saltC);err!=nil{return nil,fmt.Errorf("read client salt: %w",err)};rKey,err:=deriveKey(psk,saltC);if err!=nil{return nil,err};rAEAD,err:=chacha20poly1305.New(rKey);if err!=nil{return nil,err}
	sc:=newSecureConn(conn,r);sc.rAEAD=rAEAD;auth,err:=sc.readChunk();if err!=nil||len(auth)!=authPlain{return nil,ErrAuth};if subtle.ConstantTimeCompare(auth,magic[:])!=1{return nil,ErrAuth};if filter!=nil&&!filter.Accept(saltC){return nil,ErrAuth}
	saltS:=make([]byte,saltSize);if _,err:=rand.Read(saltS);err!=nil{return nil,err};if _,err:=conn.Write(saltS);err!=nil{return nil,err};wKey,err:=deriveKey(psk,saltS);if err!=nil{return nil,err};wAEAD,err:=chacha20poly1305.New(wKey);if err!=nil{return nil,err};sc.wAEAD=wAEAD;return sc,nil
}

func newSecureConn(conn net.Conn,r io.Reader)*SecureConn{
	if r==nil{r=conn}
	return &SecureConn{Conn:conn,reader:r,lenBuf:make([]byte,lenHdrLen+tagSize),payBuf:make([]byte,maxChunk+tagSize),padBuf:make([]byte,maxPad),frameBf:make([]byte,lenHdrLen+tagSize+maxChunk+tagSize+maxPad)}
}

func (c *SecureConn) writeChunk(plain []byte) error{
	padLen:=randPad();var hdr [lenHdrLen]byte;binary.BigEndian.PutUint16(hdr[0:2],uint16(len(plain)));binary.BigEndian.PutUint16(hdr[2:4],uint16(padLen))
	out:=c.frameBf[:0];out=c.wAEAD.Seal(out,c.wNonce[:],hdr[:],nil);incrementNonce(&c.wNonce);out=c.wAEAD.Seal(out,c.wNonce[:],plain,nil);incrementNonce(&c.wNonce)
	if padLen>0{start:=len(out);out=out[:start+padLen];fillRandom(out[start:])}
	_,err:=c.Conn.Write(out);return err
}

func (c *SecureConn) Write(p []byte)(int,error){
	c.writeMu.Lock();defer c.writeMu.Unlock();total:=0
	for total<len(p){end:=total+maxChunk;if end>len(p){end=len(p)};if err:=c.writeChunk(p[total:end]);err!=nil{return total,err};total=end}
	return total,nil
}

func (c *SecureConn) readChunk()([]byte,error){
	if _,err:=io.ReadFull(c.reader,c.lenBuf);err!=nil{return nil,err};hdr,err:=c.rAEAD.Open(c.lenBuf[:0],c.rNonce[:],c.lenBuf,nil);if err!=nil{return nil,ErrAuth};incrementNonce(&c.rNonce)
	payloadLen:=int(binary.BigEndian.Uint16(hdr[0:2]));padLen:=int(binary.BigEndian.Uint16(hdr[2:4]));if payloadLen==0||payloadLen>maxChunk||padLen>maxPad{return nil,errors.New("securestream: invalid chunk header")}
	enc:=c.payBuf[:payloadLen+tagSize];if _,err:=io.ReadFull(c.reader,enc);err!=nil{return nil,err};plain,err:=c.rAEAD.Open(enc[:0],c.rNonce[:],enc,nil);if err!=nil{return nil,ErrAuth};incrementNonce(&c.rNonce)
	if padLen>0{if _,err:=io.ReadFull(c.reader,c.padBuf[:padLen]);err!=nil{return nil,err}}
	return plain,nil
}

func (c *SecureConn) Read(p []byte)(int,error){
	c.readMu.Lock();defer c.readMu.Unlock()
	if len(p)==0{return 0,nil}
	if len(c.leftover)>0{n:=copy(p,c.leftover);c.leftover=c.leftover[n:];if len(c.leftover)==0{c.leftover=nil};return n,nil}
	plain,err:=c.readChunk();if err!=nil{return 0,err};n:=copy(p,plain);if n<len(plain){c.leftover=plain[n:]}else{c.leftover=nil};return n,nil
}

func randPad()int{return mrand.IntN(maxPad+1)}
func fillRandom(b []byte){i:=0;for ;i+8<=len(b);i+=8{binary.LittleEndian.PutUint64(b[i:],mrand.Uint64())};if i<len(b){var t [8]byte;binary.LittleEndian.PutUint64(t[:],mrand.Uint64());copy(b[i:],t[:])}}
