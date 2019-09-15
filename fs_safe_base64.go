package main

import "encoding/base64"

func B64FSEncode(Key string) string {
	Encoded := base64.StdEncoding.EncodeToString([]byte(Key))
	var NewString string
	for _, c := range Encoded {
		if c == '/' {
			NewString += "$"
		} else {
			NewString += string(c)
		}
	}
	return NewString
}

func B64FSDecode(Key string) string {
	var NewString string
	for _, c := range Key {
		if c == '$' {
			NewString += "/"
		} else {
			NewString += string(c)
		}
	}
	b, err := base64.StdEncoding.DecodeString(NewString)
	if err != nil {
		panic(err)
	}
	return string(b)
}
