package transport

import (
	"bytes"
	"github.com/Shopify/sarama"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/thrift"
	j "github.com/uber/jaeger-client-go/thrift-gen/jaeger"
)

type KafkaConnect struct {
	Addresses []string
	Topic     string
	Spans     []*j.Span
	Process   *j.Process
	//Headers   []sarama.RecordHeader
}

// Append implement Transport
func (k *KafkaConnect) Append(span *jaeger.Span) (int, error) {
	if k.Process == nil {
		k.Process = jaeger.BuildJaegerProcessThrift(span)
	}
	jSpan := jaeger.BuildJaegerThrift(span)
	k.Spans = append(k.Spans, jSpan)
	return 0, nil
}

// Flush implements Transport
func (k *KafkaConnect) Flush() (int, error) {
	count := len(k.Spans)
	if count == 0 {
		return 0, nil
	}
	err := k.Send(k.Spans)
	k.Spans = k.Spans[:0]
	return count, err
}

// Close implements Transport.
func (k *KafkaConnect) Close() error {
	return nil
}

// Send send to kafka
func (k *KafkaConnect) Send(spans []*j.Span) error {

	batch := &j.Batch{
		Spans:   spans,
		Process: k.Process,
	}

	value, err := serializeThrift(batch)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: k.Topic,
		Value: sarama.ByteEncoder(value.String()),
	}

	config := sarama.NewConfig()
	// waiting for all replicas save successfully
	config.Producer.RequiredAcks = sarama.WaitForAll
	// random partition
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// waiting for response
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(k.Addresses, config)
	if err != nil {
		panic(err)
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return err
	}

	defer producer.Close()
	return nil
}

func serializeThrift(obj thrift.TStruct) (*bytes.Buffer, error) {
	t := thrift.NewTMemoryBuffer()
	p := thrift.NewTBinaryProtocolTransport(t)
	if err := obj.Write(p); err != nil {
		return nil, err
	}
	return t.Buffer, nil
}
