package config

import (
	"os"
	"bufio"
	"strings"
	"strconv"
	"fmt"
	"path/filepath"
	"errors"
	"crypto/x509"
	"crypto/tls"
	"io/ioutil"
	"time"
)

type Config struct {
	Mode      string
	Listen    string
	Remote    string
	Port      int
	Bwlimit   int

	Log       string

	Ca        string
	Cert      string
	Key       string

	filter    []filterLine

	Storage   string
	Delay     time.Duration

	TlsConfig *tls.Config
}

type filterLine struct {
	included bool   // true == include, false == exclude
	pattern  string // rules: https://golang.org/pkg/path/filepath/#Match
}

func (config *Config) Included(dataset string) bool {
	for _, line := range config.filter {
		if line.pattern == "*" {
			return line.included
		}
		matched, err := filepath.Match(line.pattern, dataset);
		if err != nil {
			panic(fmt.Sprintf("pattern is malformed: '%s'", line.pattern))
		}
		if matched {
			return line.included
		}
	}
	panic(fmt.Sprintf("unexpected end of func config.Included for dataset '%s'", dataset))
}

func New(configName string) (*Config, error) {
	conf := &Config{
		filter: make([]filterLine, 0),
	}
	configFile, err := os.Open(configName)
	if err != nil {
		return nil, err
	}
	defer configFile.Close()

	baseConfigName := filepath.Base(configName)
	baseLogName := strings.Replace(baseConfigName, ".conf", ".log", -1)
	logName := filepath.Join("/opt/autosync/log", baseLogName)
	conf.Log = logName

	scanner := bufio.NewScanner(configFile)
	for scanner.Scan() {
		line := scanner.Text()
		commentPosition := strings.Index(line, "#")
		if commentPosition >= 0 {
			line = line[0:commentPosition]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		line = strings.Replace(line, "\t", " ", -1)
		split := strings.SplitN(line, " ", 2)
		name, value := split[0], split[1]
		name = strings.TrimSpace(name)
		value = strings.TrimSpace(value)

		switch name {
		case "mode":
			conf.Mode = value
		case "listen":
			conf.Listen = value
		case "remote":
			conf.Remote = value
		case "port":
			port, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("bad port value '%s' : %s", value, err)
			}
			conf.Port = port
		case "bwlimit":
			bwlimit, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("bad bwlimit value '%s' : %s", value, err)
			}
			conf.Bwlimit = bwlimit
		case "ca":
			conf.Ca = value
		case "cert":
			conf.Cert = value
		case "key":
			conf.Key = value

		case "include":
			if spacePosition := strings.Index(value, " "); spacePosition >= 0 {
				return nil, fmt.Errorf("spaces not allowed: '%s'", value)
			}
			if _, err := filepath.Match(value, ""); err != nil {
				return nil, fmt.Errorf("pattern is malformed: '%s'", value)
			}
			conf.filter = append(conf.filter, filterLine{true, value})
		case "exclude":
			if spacePosition := strings.Index(value, " "); spacePosition >= 0 {
				return nil, fmt.Errorf("spaces not allowed: '%s'", value)
			}
			if _, err := filepath.Match(value, ""); err != nil {
				return nil, fmt.Errorf("pattern is malformed: '%s'", value)
			}
			conf.filter = append(conf.filter, filterLine{false, value})

		case "storage":
			conf.Storage = value
		case "delay":
			delay, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("bad delay value '%s' : %s", value, err)
			}
			conf.Delay = time.Duration(delay)
		default:
			return nil, fmt.Errorf("unknown directive '%s'", name)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	conf.filter = append(conf.filter, filterLine{true, "*"}) // include all by default

	err = verifyConfig(conf)
	if err != nil {
		return nil, err
	}
	err = tlsConfig(conf)
	if err != nil {
		return nil, err
	}
	return conf, nil
}

func verifyConfig(conf *Config) error {
	switch conf.Mode {
	case "server":
		if conf.Listen == "" {
			return fmt.Errorf("bad listen directive value '%s'", conf.Listen)
		}
		if conf.Remote != "" {
			return fmt.Errorf("bad remote directive value '%s'", conf.Remote)
		}
		if conf.Storage != "" {
			return fmt.Errorf("bad storage directive value '%s'", conf.Storage)
		}
		if conf.Delay != 0 {
			return fmt.Errorf("bad delay directive value '%s'", conf.Delay)
		}
	case "client":
		if conf.Remote == "" {
			return fmt.Errorf("bad remote directive value '%s'", conf.Remote)
		}
		if conf.Listen != "" {
			return fmt.Errorf("bad listen directive value '%s'", conf.Listen)
		}
		if conf.Storage == "" {
			return fmt.Errorf("bad storage directive value '%s'", conf.Storage)
		}
		if conf.Delay <= 0 {
			return fmt.Errorf("bad delay directive value '%s'", conf.Delay)
		}
		if _, err := os.Stat(conf.Storage); os.IsNotExist(err) {
			return fmt.Errorf("bad 'storage' value '%s' : %s", conf.Storage, err)
		}
	default:
		return fmt.Errorf("unknown mode directive value '%s', must be 'server' or 'client'", conf.Mode)
	}

	if _, err := os.Stat(conf.Ca); os.IsNotExist(err) {
		return fmt.Errorf("bad 'ca' value '%s' : %s", conf.Ca, err)
	}
	if _, err := os.Stat(conf.Cert); os.IsNotExist(err) {
		return fmt.Errorf("bad 'cert' value '%s' : %s", conf.Cert, err)
	}
	if _, err := os.Stat(conf.Key); os.IsNotExist(err) {
		return fmt.Errorf("bad 'key' value '%s' : %s", conf.Key, err)
	}

	if conf.Port < 1 || conf.Port > 65535 {
		return errors.New("bad 'port' value: must be between '1' and '65535'")
	}
	if conf.Bwlimit < 0 {
		return fmt.Errorf("bad 'bwlimit' value '%s'", conf.Bwlimit)
	}
	if conf.Delay < 0 {
		return fmt.Errorf("bad 'delay' value '%s'", conf.Delay)
	}

	return nil
}

func tlsConfig(conf *Config) error {

	// http://www.hydrogen18.com/blog/your-own-pki-tls-golang.html
	// Using your own PKI for TLS in Golang

	certificate, err := tls.LoadX509KeyPair(conf.Cert, conf.Key)
	if err != nil {
		return err
	}
	pem, err := ioutil.ReadFile(conf.Ca)
	if err != nil {
		return err
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pem) {
		return fmt.Errorf(" can't append certificate '%s' to pool", conf.Ca)
	}

	tlsConfig := &tls.Config{}
	tlsConfig.Certificates = make([]tls.Certificate, 1)
	tlsConfig.Certificates[0] = certificate
	tlsConfig.RootCAs = certPool
	tlsConfig.ClientCAs = certPool
	tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	//Use only modern ciphers
	tlsConfig.CipherSuites = []uint16{
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	}
	//Use only TLS v1.2
	tlsConfig.MinVersion = tls.VersionTLS12
	//Don't allow session resumption
	tlsConfig.SessionTicketsDisabled = true

	conf.TlsConfig = tlsConfig

	return nil
}
