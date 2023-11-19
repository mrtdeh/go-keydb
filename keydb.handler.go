package keydb

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

type (
	Client struct {
		conn rueidis.Client
	}

	PubData []byte

	ErrorFn func(err error)

	Config struct {
		Host      string
		Port      int
		Pass      string
		DB        int
		TLSConfig *tls.Config
		OnError   ErrorFn
	}

	KeyVal struct {
		Key string
		Val string
		Ex  time.Duration
	}
)

var (
	pub    sync.RWMutex
	ctx    = context.Background()
	client *Client

	Nil = rueidis.Nil
)

var handleError ErrorFn
var lastErr error

func genHandleError(inFn ErrorFn) ErrorFn {
	return func(err error) {
		if inFn != nil {
			if !errors.Is(err, lastErr) {
				if err != nil {
					inFn(err)
				}
			}
			lastErr = err
		}
	}
}

func (c *Client) pingHandler(dur time.Duration) {
	for {
		res := c.conn.Do(ctx, c.conn.B().Ping().Build())
		if res.Error() != nil {
			log.Println("ping err : ", res.Error())
		}

		handleError(res.Error())
		time.Sleep(dur)
	}
}

func Init(opt Config) error {
	pub.Lock()
	defer pub.Unlock()

	if client != nil {
		return nil
	}

	handleError = genHandleError(opt.OnError)

	redisopt := rueidis.ClientOption{
		InitAddress: []string{fmt.Sprintf("%s:%d", opt.Host, opt.Port)},
		Password:    opt.Pass,
		TLSConfig:   opt.TLSConfig,
		SelectDB:    opt.DB,
	}

	rueidisClient, err := rueidis.NewClient(redisopt)
	if err != nil {
		handleError(err)
		return fmt.Errorf("error in connecting to redis: %s", err.Error())
	}

	res := rueidisClient.Do(ctx, rueidisClient.B().Ping().Build())
	if res.Error() != nil {
		handleError(res.Error())
		return fmt.Errorf("redis ping failed : %s", res.Error().Error())
	}

	client = &Client{
		conn: rueidisClient,
	}

	go client.pingHandler(time.Second * 5)

	log.Println("successfuly connect to redis server")
	return nil
}

func Set(key, val string, exp time.Duration) error {
	cmd := client.conn.B().Set().Key(key).Value(val)
	if exp != 0 {
		cmd.Ex(exp)
	}
	resp := client.conn.Do(ctx, cmd.Build())
	if err := resp.Error(); err != nil {
		return err
	}
	return nil
}
func Get(key string) ([]byte, error) {
	cmd := client.conn.B().Get().Key(key).Build()
	resp := client.conn.Do(ctx, cmd)
	if err := resp.Error(); err != nil {
		return nil, err
	}

	data, err := resp.AsBytes()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func Scan(cursor uint64, pattern string) ([]string, error) {
	cmd := client.conn.B().Scan().Cursor(cursor).Match(pattern).Build()
	resp := client.conn.Do(ctx, cmd)
	if err := resp.Error(); err != nil {
		return nil, err
	}

	r, err := resp.AsScanEntry()
	if err != nil {
		return nil, err
	}

	return r.Elements, nil
}

func SetMulti(kvs []KeyVal) error {
	pub.Lock()
	defer pub.Unlock()

	var cmds = make(rueidis.Commands, 0, len(kvs))
	for _, m := range kvs {
		cmd := client.conn.B().Set().Key(m.Key).Value(m.Val)
		if m.Ex != 0 {
			cmd.Ex(m.Ex)
		}
		cmds = append(cmds, cmd.Build())
	}
	for _, resp := range client.conn.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			fmt.Println(err)
		}
	}
	return nil
}

func Publish(chanel string, msgs []PubData) error {
	pub.Lock()
	defer pub.Unlock()

	var cmds = make(rueidis.Commands, 0, len(msgs))

	for _, m := range msgs {
		cmds = append(cmds, client.conn.B().Publish().Channel(chanel).Message(string(m)).Build().Pin())
	}

	for _, resp := range client.conn.DoMulti(ctx, cmds...) {
		if err := resp.Error(); err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func Subscribe(msgChan chan interface{}, chanel ...string) {

	cmd := client.conn.B().Subscribe().Channel(chanel...).Build()
	go func() {
		err := client.conn.Receive(ctx, cmd, func(msg rueidis.PubSubMessage) {
			// msgCh <- msg.Message
			msgChan <- msg.Message
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

}
