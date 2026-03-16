package utils

import "io"

func ReadN(r io.Reader, n int) ([]byte, int, error) {
	data := make([]byte, n)
	n, err := io.ReadFull(r, data)
	if err != nil {
		return nil, n, err
	}

	return data, n, nil
}
