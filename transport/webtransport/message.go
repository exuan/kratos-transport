package webtransport

import (
	"bytes"
	"encoding/binary"
)

type Message struct {
	Type MessageType
	Body []byte
}

func (m *Message) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, uint32(m.Type)); err != nil {
		return nil, err
	}
	buf.Write(m.Body)
	return buf.Bytes(), nil
}

func (m *Message) Unmarshal(buf []byte) error {
	network := new(bytes.Buffer)
	network.Write(buf)

	if err := binary.Read(network, binary.LittleEndian, &m.Type); err != nil {
		return err
	}

	m.Body = network.Bytes()

	return nil
}
