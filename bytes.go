package filecache

import (
	"encoding/json"
	"time"
)

// ToJsonBytes is the default implementation of encoding a value into JSON.
func ToJsonBytes(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err == nil {
		return b
	}
	return nil
}

// TimeFromBytes is an implementation of decoding json into a time.Time
func TimeFromBytes(b []byte) interface{} {
	if b == nil {
		return nil
	}

	var t time.Time
	err := json.Unmarshal(b, &t)
	if err != nil {
		return nil
	}
	return t
}
