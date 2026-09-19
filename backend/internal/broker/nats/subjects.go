package natsbroker

import (
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var ErrInvalidApplicationID = errors.New("invalid application ID")

type AppSubjects struct {
	Deliveries  string
	DeadLetters string
	Realtime    string
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
		Deliveries:  "rh.v1.delivery." + token,
		DeadLetters: "rh.v1.dlq." + token,
		Realtime:    "rh.v1.realtime." + token,
	}, nil
}

func CallbackSubject(shard int) (string, error) {
	if shard < 0 || shard > 1023 {
		return "", errors.New("invalid callback shard")
	}
	return fmt.Sprintf("rh.v1.callback.%03d", shard), nil
}

func FunctionSubject(appID, functionID string) (string, error) {
	appToken, err := AppToken(appID)
	if err != nil {
		return "", err
	}
	functionToken, err := AppToken(functionID)
	if err != nil {
		return "", err
	}
	return "rh.v1.rpc." + appToken + "." + functionToken, nil
}

func ReplySubject(instanceID string) (string, error) {
	token, err := AppToken(instanceID)
	if err != nil {
		return "", err
	}
	return "rh.v1.rpc.reply." + token, nil
}
