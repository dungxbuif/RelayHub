package natsbroker

import (
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"strings"
	"unicode/utf8"
)

var ErrInvalidApplicationID = errors.New("invalid application ID")

type AppSubjects struct {
	Deliveries  string
	Callbacks   string
	DeadLetters string
	Observe     string
	Functions   string
}

func AppToken(appID string) (string, error) {
	if appID == "" || appID != strings.TrimSpace(appID) || len(appID) > 256 || !utf8.ValidString(appID) || strings.IndexByte(appID, 0) >= 0 {
		return "", ErrInvalidApplicationID
	}
	digest := sha256.Sum256([]byte(appID))
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(digest[:20])), nil
}

func SubjectsForApp(appID string) (AppSubjects, error) {
	token, err := AppToken(appID)
	if err != nil {
		return AppSubjects{}, err
	}
	return AppSubjects{
		Deliveries:  "rh.deliveries." + token,
		Callbacks:   "rh.callbacks." + token,
		DeadLetters: "rh.dlq." + token,
		Observe:     "rh.observe." + token,
		Functions:   "rh.functions." + token,
	}, nil
}
