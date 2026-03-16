package utils

func Contains[T comparable](slice []T, element T) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}

	return false
}

func RemoveElement[T comparable](slice []T, element T) ([]T, bool) {
	for i, v := range slice {
		if v == element {
			return append(slice[:i], slice[i+1:]...), true
		}
	}

	return slice, false
}
