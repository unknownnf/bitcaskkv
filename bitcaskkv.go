package bitcaskkv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"git.mills.io/prologic/bitcask"
)

type KVStore struct {
	db *bitcask.Bitcask
}

var (
	ErrNotFound = errors.New("bitcaskkv: key not found")
	ErrBadValue = errors.New("bitcaskkv: bad value")
)

func Open(path string) (*KVStore, error) {
	if db, err := bitcask.Open(path); err != nil {
		return nil, err
	} else {
		return &KVStore{db: db}, nil
	}
}

func (s *KVStore) Put(key string, value interface{}) error {
	if value == nil {
		return ErrBadValue
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}

	if err := s.db.Put([]byte(key), buf.Bytes()); err != nil {
		return err
	} else {
		return s.db.Sync()
	}
}

func (s *KVStore) Get(key string, value interface{}) error {
	v, err := s.db.Get([]byte(key))
	if err != nil {
		return ErrNotFound
	} else if value == nil {
		return nil
	} else {
		d := gob.NewDecoder(bytes.NewReader(v))
		return d.Decode(value)
	}
}

func (s *KVStore) Delete(key string) error {
	_, err := s.db.Get([]byte(key))
	if err != nil {
		return ErrNotFound
	}
	if err = s.db.Delete([]byte(key)); err != nil {
		return err
	} else {
		return s.db.Sync()
	}
}

func (s *KVStore) Keys() chan []byte {
	return s.db.Keys()
}

func (s *KVStore) Scan(prefix []byte, f func(key []byte) error) error {
	return s.db.Scan(prefix, f)
}

func (s *KVStore) Close() error {
	return s.db.Close()
}
