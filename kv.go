package simplebolt

import (
	"errors"

	"github.com/boltdb/bolt"
)

type KeyValueB struct {
	*boltBucket
	bkts *bolt.Tx
}

// Create a new key/value if it does not already exist
func NewKeyValueB(db *Database, id string) (*KeyValueB, error) {
	name := []byte(id)
	if err := (*bolt.DB)(db).Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return errors.New("Could not create bucket: " + err.Error())
		}
		return nil // Return from Update function
	}); err != nil {
		return nil, err
	}
	return &KeyValueB{&boltBucket{db, name}, nil}, nil
}

// Set a key and value
func (kv *KeyValueB) Set(key, value string) error {
	if kv.name == nil || kv.bkts == nil {
		return ErrDoesNotExist
	}
	tx := kv.bkts.Bucket(kv.name)

	if tx == nil {
		return ErrBucketNotFound
	}
	return tx.Put([]byte(key), []byte(value))
}

// Get a value given a key
// Returns an error if the key was not found
func (kv *KeyValueB) Get(key string) (val string, err error) {
	if kv.name == nil {
		return "", ErrDoesNotExist
	}
	tx := kv.bkts.Bucket(kv.name)
	byteval := tx.Get([]byte(key))
	if byteval == nil {
		return "", ErrKeyNotFound
	}
	val = string(byteval)
	return val, nil // Return from View function
}

func (kv *KeyValueB) Begin(w bool) error {
	tx, err := (*bolt.DB)(kv.db).Begin(w)
	if err != nil {
		return err
	}
	kv.bkts = tx
	return nil
}

func (kv *KeyValueB) Commit() error {
	err := kv.bkts.Commit()
	kv.bkts = nil
	return err
}
