package mcp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type Client interface {
	Connect() error
	Close() error
	Read(deviceName string, offset, numPoints int64) ([]byte, error)
	BitRead(deviceName string, offset, numPoints int64) ([]byte, error)
	Write(deviceName string, offset, numPoints int64, writeData []byte) ([]byte, error)
	HealthCheck() error
}

const (
	defaultConnectTimeout = 5 * time.Second
	defaultReadTimeout    = 5 * time.Second
	defaultWriteTimeout   = 5 * time.Second
)

// client3E is 3E frame mcp client with persistent connection
type client3E struct {
	tcpAddr *net.TCPAddr
	stn     *station

	conn net.Conn
	mu   sync.Mutex

	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
}

func New3EClient(host string, port int, stn *station) (Client, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%v:%v", host, port))
	if err != nil {
		return nil, err
	}
	return &client3E{
		tcpAddr:        tcpAddr,
		stn:            stn,
		connectTimeout: defaultConnectTimeout,
		readTimeout:    defaultReadTimeout,
		writeTimeout:   defaultWriteTimeout,
	}, nil
}

func (c *client3E) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	dialer := &net.Dialer{Timeout: c.connectTimeout}
	conn, err := dialer.Dial("tcp", c.tcpAddr.String())
	if err != nil {
		return fmt.Errorf("connect to plc failed: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *client3E) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

func (c *client3E) sendAndReceive(payload []byte, respSize int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil, errors.New("connection is not established, call Connect() first")
	}

	if c.writeTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	if _, err := c.conn.Write(payload); err != nil {
		return nil, fmt.Errorf("write error: %w", err)
	}

	if c.readTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	readBuff := make([]byte, respSize)
	readLen, err := c.conn.Read(readBuff)
	if err != nil {
		return nil, fmt.Errorf("read error: %w", err)
	}

	return readBuff[:readLen], nil
}

func (c *client3E) HealthCheck() error {
	requestStr := c.stn.BuildHealthCheckRequest()

	payload, err := hex.DecodeString(requestStr)
	if err != nil {
		return err
	}

	resp, err := c.sendAndReceive(payload, 30)
	if err != nil {
		return err
	}

	if len(resp) != 18 {
		return errors.New("plc connect test is fail: return length is [" + fmt.Sprintf("%X", resp) + "]")
	}

	if "0500" != fmt.Sprintf("%X", resp[11:13]) {
		return errors.New("plc connect test is fail: return header is [" + fmt.Sprintf("%X", resp[11:13]) + "]")
	}

	if "4142434445" != fmt.Sprintf("%X", resp[13:18]) {
		return errors.New("plc connect test is fail: return body is [" + fmt.Sprintf("%X", resp[13:18]) + "]")
	}

	return nil
}

// Read is send read as word command to remote plc by mc protocol
// deviceName is device code name like 'D' register.
// offset is device offset addr.
// numPoints is number of read device points.
func (c *client3E) Read(deviceName string, offset, numPoints int64) ([]byte, error) {
	requestStr := c.stn.BuildReadRequest(deviceName, offset, numPoints)

	payload, err := hex.DecodeString(requestStr)
	if err != nil {
		return nil, err
	}

	return c.sendAndReceive(payload, int(22+2*numPoints))
}

// BitRead is send read as bit command to remote plc by mc protocol
// deviceName is device code name like 'D' register.
// offset is device offset addr.
// numPoints is number of read device points.
// results of payload of BitRead will return []byte contains 0, 1, 16 or 17(hex encoded 00, 01, 10, 11)
func (c *client3E) BitRead(deviceName string, offset, numPoints int64) ([]byte, error) {
	requestStr := c.stn.BuildBitReadRequest(deviceName, offset, numPoints)

	payload, err := hex.DecodeString(requestStr)
	if err != nil {
		return nil, err
	}

	return c.sendAndReceive(payload, int(22+2*numPoints))
}

// Write is send write command to remote plc by mc protocol
// deviceName is device code name like 'D' register.
// offset is device offset addr.
// writeData is data to write.
// numPoints is number of write device points.
// writeData is the data to be written. If writeData is larger than 2*numPoints bytes,
// data larger than 2*numPoints bytes is ignored.
func (c *client3E) Write(deviceName string, offset, numPoints int64, writeData []byte) ([]byte, error) {
	requestStr := c.stn.BuildWriteRequest(deviceName, offset, numPoints, writeData)
	payload, err := hex.DecodeString(requestStr)
	if err != nil {
		return nil, err
	}

	return c.sendAndReceive(payload, 22)
}
