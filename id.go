// copy from mgo
package mongodb

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"
)

var machineId = readMachineId()
var processId = os.Getpid()
var objectIdCounter = readRandomUint32()

func NewObjectID() primitive.ObjectID {
	var b [12]byte
	// Timestamp, 4 bytes, big endian
	binary.BigEndian.PutUint32(b[:], uint32(time.Now().Unix()))
	// Machine, first 3 bytes of md5(hostname)
	b[4] = machineId[0]
	b[5] = machineId[1]
	b[6] = machineId[2]
	// Pid, 2 bytes, specs don't specify endianness, but we use big endian.
	b[7] = byte(processId >> 8)
	b[8] = byte(processId)
	// Increment, 3 bytes, big endian
	i := atomic.AddUint32(&objectIdCounter, 1)
	b[9] = byte(i >> 16)
	b[10] = byte(i >> 8)
	b[11] = byte(i)
	return b
}

// readMachineId generates and returns a machine id.
// If this function fails to get the hostname it will cause a runtime error.
func readMachineId() []byte {
	var sum [3]byte
	id := sum[:]

	var sourceBuf bytes.Buffer

	// Get hostname
	hostname, err := os.Hostname()
	if err == nil {
		sourceBuf.WriteString(hostname)
	}

	// Get network info
	netInterfaces, err := net.Interfaces()
	if err == nil {
		for _, netInterface := range netInterfaces {
			a, err := netInterface.Addrs()
			if err == nil {
				for _, addr := range a {
					if ip, ok := addr.(*net.IPNet); ok && !ip.IP.IsLoopback() {
						sourceBuf.WriteString(ip.IP.String())
					}
				}
			}
			mac := netInterface.HardwareAddr.String()
			sourceBuf.WriteString(mac)
		}
	}

	hw := md5.New()
	hw.Write(sourceBuf.Bytes())
	copy(id, hw.Sum(nil))
	return id
}

// readRandomUint32 returns a random objectIdCounter.
func readRandomUint32() uint32 {
	var b [4]byte
	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic(fmt.Errorf("cannot read random object id: %v", err))
	}
	return (uint32(b[0]) << 0) | (uint32(b[1]) << 8) | (uint32(b[2]) << 16) | (uint32(b[3]) << 24)
}
