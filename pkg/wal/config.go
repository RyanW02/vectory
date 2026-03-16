package wal

type Config struct {
	MaxSegmentBytes int64
}

type Option func(*Config)

func NewConfig(opts ...Option) *Config {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	return cfg
}

func DefaultConfig() *Config {
	return &Config{
		MaxSegmentBytes: 1024 * 1024 * 16, // 16 MB
	}
}

func WithMaxSegmentBytes(maxSegmentBytes int64) Option {
	return func(cfg *Config) {
		cfg.MaxSegmentBytes = maxSegmentBytes
	}
}
