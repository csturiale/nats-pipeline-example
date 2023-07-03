package utils

import "os"

func GetEnv(envName, valueDefault string) string {
	value := os.Getenv(envName)
	if value == "" {
		return valueDefault
	}
	return value
}
