package rabbitmq

import (
	"errors"
	"github.com/kelseyhightower/envconfig"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

// QueueConfig is config structure for Queue
type QueueConfig struct {
	ReconnectAMQPDelay		int		`envparse:"RECONNECT_AMQP_DELAY" default:"5"`
	ReconnectAMQPCount		int		`envparse:"RECONNECT_AMQP_COUNT" default:"5"`
	ResendAMQPDelay			int		`envparse:"RESEND_AMQP_DELAY" default:"5"`
	RabbitPrefetchCount		int 	`envrapse:"AMQPR_PREFETCH_COUNT" default:"1"`
	AutoDeclareExchanges	bool	`envparse:"AUTO_DECLARE_EXCHANGES" default:"true"`
}

// GetConfig returns config from ENV with defaults for Queue instance
func GetConfig() (config QueueConfig, err error) {
	err = envconfig.Process("", &config)
	return
}

// QBind is settings for queue binding
type QBind struct {
	Exchange   				string
	ExchangeType			string
	ExchangeArguments  		amqp.Table
	RoutingKey 				string
}

// Errors
var (
	errNotConnected  = errors.New("not connected to the queue")
	errAlreadyClosed = errors.New("already closed: not connected to the queue")
)

// Queue represents a connection to a specific queue.
type Queue struct {
	config			*QueueConfig				// queue instance config
	name          	string 					// queue instance name
	notifyClose   	chan *amqp.Error		// channel for AMQP close error handling
	notifyConfirm	chan amqp.Confirmation	// channel for AMQP confirmations
	IsConnected   	bool					// state of connect

	// connection represents a real TCP connection to the message broker
	connection    	*amqp.Connection

	// channel is a virtual connection (AMQP connection) inside of connection
	channel       	*amqp.Channel
}

// New creates a new queue instance, and automatically
// attempts to connect to the server.
func New(name string, addr string, config QueueConfig, queueArgs amqp.Table,
	binding QBind) (queue *Queue) {
	queue = &Queue{
		config:	&config,
		name:   name,
	}
	// register shutdown handler
	go queue.gracefulShutdown()
	// connect with retries
	go queue.handleReconnect(addr, queueArgs)
	// bind queue to exchange
	go queue.bindQueue(binding)
	return
}

// gracefulShutdown handles close error
func (queue *Queue) gracefulShutdown() {
	select {
	case err := <- queue.notifyClose:
		logrus.Warnf("queue connection closed: %s", err)
		if err := queue.Close(); err != nil {
			logrus.Errorf("err during closing queue connection: %s", err)
		}
	}
}

// bindQueue binds queue ti exchange with given binding.RoutingKey
// if queue.config.AutoDeclareExchanges is true - declares exchange
func (queue *Queue) bindQueue(binding QBind) {
	// If AutoDeclareExchanges set to true, trying to declare exchange.
	// If the exchange does not already exist, the server will create it.
	// If the exchange exists, the server
	// verifies that it is of the provided type, durability and auto-delete flags.
	if queue.config.AutoDeclareExchanges {
		err := queue.channel.ExchangeDeclare(
			queue.name,
			binding.ExchangeType,
			true,
			false,
			false,
			false,
			binding.ExchangeArguments,
			)
		if err != nil {
			logrus.Errorf("err during exchange declaring: %s", err)
			queue.notifyClose <- amqp.ErrClosed
		}
	}
	err := queue.channel.QueueBind(
		queue.name,
		binding.RoutingKey,
		binding.Exchange,
		false,
		amqp.Table{},
		)
	if err != nil {
		logrus.Errorf(
			"err during binding %s queue to %s exchange: %s",
			queue.name, binding.Exchange, err)
		queue.notifyClose <- amqp.ErrClosed
	}
}

// handleReconnect will try to connect for queue.config.ReconnectAMQPCount times
func (queue *Queue) handleReconnect(addr string, queueArgs amqp.Table) {
	for i := 0; i < queue.config.ReconnectAMQPCount; i++ {
		if !queue.IsConnected {
			logrus.Println("Attempting to connect")
			if !queue.connect(addr, queueArgs) {
				logrus.Error("Failed to connect RabbitMQ. Retrying...")
				time.Sleep(time.Duration(queue.config.ReconnectAMQPDelay) * time.Second)
			}
		} else {
			break
		}
	}
}

// connect will make a single attempt to connect to
// RabbitMQ. It sets queue.IsConnected flag to true if success and returns bool.
func (queue *Queue) connect(addr string, queueArgs amqp.Table) bool {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return false
	}
	ch, err := conn.Channel()
	if err != nil {
		return false
	}
	if err := ch.Confirm(false); err != nil {
		return false
	}
	_, err = ch.QueueDeclare(
		queue.name,
		true, 		// Durable
		false, 	// Delete when unused
		false, 	// Exclusive
		false, 		// No-wait
		queueArgs,   		// Arguments
	)
	if err != nil {
		return false
	}
	if err = ch.Qos(queue.config.RabbitPrefetchCount,0,false); err != nil {
		logrus.Errorf("RabbitMQ Qos error: %s\n", err)
		return false
	}
	queue.changeConnection(conn, ch)
	queue.IsConnected = true
	logrus.Info("RabbitMQ connected!")
	return true
}

// changeConnection takes a new connection to the queue,
// and updates the channel listeners to reflect this.
func (queue *Queue) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	queue.connection = connection
	queue.channel = channel
	queue.notifyConfirm = make(chan amqp.Confirmation)
	queue.channel.NotifyClose(queue.notifyClose)
	queue.channel.NotifyPublish(queue.notifyConfirm)
}

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are recieved until within the resendTimeout,
// it continuously resends messages until a confirm is recieved.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (queue *Queue) Push(data []byte) error {
	if !queue.IsConnected {
		return errors.New("failed to push push: not connected")
	}
	for {
		err := queue.UnsafePush(data)
		if err != nil {
			logrus.Error("Push failed. Retrying...")
			continue
		}
		select {
		case confirm := <-queue.notifyConfirm:
			if confirm.Ack {
				logrus.Debug("Push confirmed")
				return nil
			}
		case <-time.After(time.Duration(queue.config.ResendAMQPDelay) * time.Second):
			logrus.Println("Push didn't confirm. Retrying...")
		}
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// recieve the message.
func (queue *Queue) UnsafePush(data []byte) error {
	if !queue.IsConnected {
		return errNotConnected
	}
	return queue.channel.Publish(
		"",         	// Exchange
		queue.name, 			// Routing key
		false,      	// Mandatory
		false,      	// Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
}

// Stream will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (queue *Queue) Stream() (<-chan amqp.Delivery, error) {
	if !queue.IsConnected {
		return nil, errNotConnected
	}
	return queue.channel.Consume(
		queue.name,
		"",    	// Consumer
		false, 	// Auto-Ack
		false, 	// Exclusive
		false, 		// No-local
		false, 		// No-Wait
		nil,   		// Args
	)
}

func (queue *Queue) Get(autoAck bool) (amqp.Delivery, bool, error) {
	if !queue.IsConnected {
		return amqp.Delivery{}, false, errNotConnected
	}
	return queue.channel.Get(queue.name, autoAck)
}

func (queue *Queue) Purge(noWait bool) (int, error) {
	if !queue.IsConnected {
		return 0, errNotConnected
	}
	return queue.channel.QueuePurge(queue.name, noWait)
}

// Close will cleanly shutdown the channel and connection.
func (queue *Queue) Close() error {
	if !queue.IsConnected {
		return errAlreadyClosed
	}
	err := queue.channel.Close()
	if err != nil {
		return err
	}
	err = queue.connection.Close()
	queue.IsConnected = false
	return nil
}
