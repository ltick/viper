// Copyright © 2015 Steve Francia <spf@spf13.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// Package remote integrates the remote features of Viper.
package remote

import (
	"bytes"
	crypt "github.com/ltick/crypt/config"
	"github.com/samt42/viper"
	"io"
	"os"
)

type remoteConfigProvider struct{}

func (rc remoteConfigProvider) Set(rp viper.RemoteProvider, value []byte) error {
    cm, err := getConfigManager(rp)
    if err != nil {
        return err
    }
    err = cm.Set(rp.Path(), value)
    if err != nil {
        return err
    }
    return nil
}


func (rc remoteConfigProvider) List(rp viper.RemoteProvider) (map[string][]byte, error) {
    cm, err := getConfigManager(rp)
    if err != nil {
        return err
    }
    kvPairs, err := cm.List(rp.Path())
    if err != nil {
        return err
    }
    list := make(map[string][]byte, 0)
    for _, kvPair := range kvPairs {
        list[kvPair.Key] = kvPair.Value
    }
    return list, nil
}

func (rc remoteConfigProvider) Get(rp viper.RemoteProvider) (io.Reader, error) {
	cm, err := getConfigManager(rp)
	if err != nil {
		return nil, err
	}
	b, err := cm.Get(rp.Path())
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

func (rc remoteConfigProvider) Watch(rp viper.RemoteProvider) (io.Reader, error) {
	cm, err := getConfigManager(rp)
	if err != nil {
		return nil, err
	}
	resp, err := cm.Get(rp.Path())
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(resp), nil
}

func (rc remoteConfigProvider) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	cm, err := getConfigManager(rp)
	if err != nil {
		return nil, nil
	}
	quit := make(chan bool)
	quitwc := make(chan bool)
	viperResponsCh := make(chan *viper.RemoteResponse)
	cryptoResponseCh := cm.Watch(rp.Path(), quit)
	// need this function to convert the Channel response form crypt.Response to viper.Response
	go func(cr <-chan *crypt.Response, vr chan<- *viper.RemoteResponse, quitwc <-chan bool, quit chan<- bool) {
		for {
			select {
			case <-quitwc:
				quit <- true
				return
			case resp := <-cr:
				vr <- &viper.RemoteResponse{
					Error: resp.Error,
					Value: resp.Value,
				}

			}

		}
	}(cryptoResponseCh, viperResponsCh, quitwc, quit)

	return viperResponsCh, quitwc
}

func getConfigManager(rp viper.RemoteProvider) (crypt.ConfigManager, error) {
	var cm crypt.ConfigManager
	var err error

	if rp.SecretKeyring() != "" {
		kr, err := os.Open(rp.SecretKeyring())
		defer kr.Close()
		if err != nil {
			return nil, err
		}
		switch rp.Provider() {
		case "etcd":
			cm, err = crypt.NewEtcdConfigManager([]string{rp.Endpoint()}, kr)
		case "consul":
			cm, err = crypt.NewConsulConfigManager([]string{rp.Endpoint()}, kr)
		case "memcache":
			cm, err = crypt.NewMemcacheConfigManager([]string{rp.Endpoint()}, kr)
		case "zookeeper":
			config := rp.Config()
			cm, err = crypt.NewZookeeperConfigManager([]string{rp.Endpoint()}, config["user"], config["password"], kr)
		}
	} else {
		switch rp.Provider() {
		case "etcd":
			cm, err = crypt.NewStandardEtcdConfigManager([]string{rp.Endpoint()})
		case "consul":
			cm, err = crypt.NewStandardConsulConfigManager([]string{rp.Endpoint()})
		case "memcache":
			cm, err = crypt.NewStandardMemcacheConfigManager([]string{rp.Endpoint()})
		case "zookeeper":
			config := rp.Config()
			cm, err = crypt.NewStandardZookeeperConfigManager([]string{rp.Endpoint()}, config["user"], config["password"])
		}
	}
	if err != nil {
		return nil, err
	}
	return cm, nil
}

func init() {
	viper.RemoteConfig = &remoteConfigProvider{}
}
