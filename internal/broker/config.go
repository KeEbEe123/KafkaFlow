package broker

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Broker  BrokerConfig  `yaml:"broker"`
	Cluster ClusterConfig `yaml:"cluster"`
	Storage StorageConfig `yaml:"storage"`
	Consumer ConsumerGroupConfig `yaml:"consumer_group"`
}

type BrokerConfig struct {
	ID       int32  `yaml:"id"`
	GRPCAddr string `yaml:"grpc_addr"`
	RaftAddr string `yaml:"raft_addr"`
	WSAddr   string `yaml:"ws_addr"`
}

type ClusterConfig struct {
	Peers []PeerConfig `yaml:"peers"`
}

type PeerConfig struct {
	ID   int32  `yaml:"id"`
	Addr string `yaml:"addr"`
}

type StorageConfig struct {
	DataDir        string `yaml:"data_dir"`
	SegmentMaxBytes int64 `yaml:"segment_max_bytes"`
	RetentionHours  int   `yaml:"retention_hours"`
}

type ConsumerGroupConfig struct {
	SessionTimeoutMs   int `yaml:"session_timeout_ms"`
	HeartbeatIntervalMs int `yaml:"heartbeat_interval_ms"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Storage.SegmentMaxBytes == 0 {
		cfg.Storage.SegmentMaxBytes = 64 * 1024 * 1024
	}
	if cfg.Consumer.SessionTimeoutMs == 0 {
		cfg.Consumer.SessionTimeoutMs = 30000
	}
	if cfg.Consumer.HeartbeatIntervalMs == 0 {
		cfg.Consumer.HeartbeatIntervalMs = 3000
	}
	return &cfg, nil
}

func DefaultConfig(id int32, grpcAddr, raftAddr, wsAddr string) *Config {
	return &Config{
		Broker: BrokerConfig{
			ID:       id,
			GRPCAddr: grpcAddr,
			RaftAddr: raftAddr,
			WSAddr:   wsAddr,
		},
		Storage: StorageConfig{
			DataDir:        "./data",
			SegmentMaxBytes: 64 * 1024 * 1024,
			RetentionHours:  168,
		},
		Consumer: ConsumerGroupConfig{
			SessionTimeoutMs:    30000,
			HeartbeatIntervalMs: 3000,
		},
	}
}
