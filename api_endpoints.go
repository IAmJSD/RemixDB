package main

// All access control information.
type AccessControlInformation struct {
	Admin bool `json:"admin"`
	Create bool `json:"create"`
	Read bool `json:"read"`
	Write bool `json:"write"`
	DBOverrides map[string]*bool `json:"db_overrides"`
	TableOverrides map[string]*map[string]*bool `json:"table_overrides"`
}

// Checks the alleged token. The struct containing token access control information will be returned.
func CheckAllegedToken(Token string, Admin bool) *AccessControlInformation {
	// Checks if a token is safe without using regex (gotta go fast).
	for _, v := range Token {
		IsInt := v >= 48 && v <= 57
		IsLetter := (v >= 65 && v <= 90) || (v >= 97 && v <= 122)
		if !IsInt && !IsLetter && v != 45 {
			return false
		}
	}
	if len(Token) == 0 {
		return false
	}


}
