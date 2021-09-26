package bitcaskkv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"git.mills.io/prologic/bitcask"
)

type Store struct {
	db *bitcask.Bitcask
}

var (
	ErrNotFound = errors.New("bitcaskkv: key not found")
	ErrBadValue = errors.New("bitcaskkv: bad value")
)

func Open(path string) (*Store, error) {
	if db, err := bitcask.Open(path); err != nil {
		return nil, err
	} else {
		return &Store{db: db}, nil
	}
}

func (s *Store) Put(key string, value interface{}) error {
	if value == nil {
		return ErrBadValue
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}

	if err := s.db.Put([]byte(key), buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func (s *Store) Get(key string, value interface{}) error {
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

func (s *Store) Delete(key string) error {
	_, err := s.db.Get([]byte(key))
	if err != nil {
		return ErrNotFound
	}
	if err = s.db.Delete([]byte(key)); err != nil {
		return err
	}
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) GetDb() *bitcask.Bitcask {
	return s.db
}
