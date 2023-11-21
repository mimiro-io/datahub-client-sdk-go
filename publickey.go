package datahub

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"time"
)

func createJWTForTokenRequest(subject string, audience string, privateKey *rsa.PrivateKey) (string, error) {
	uniqueId := uuid.New()

	claims := jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute * 1)),
		ID:        uniqueId.String(),
		Subject:   subject,
		Audience:  jwt.ClaimStrings{audience},
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(privateKey)
	if err != nil {
		return "", err
	}
	return token, nil
}

func generateRsaKeyPair() (*rsa.PrivateKey, *rsa.PublicKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, err
	}
	return key, &key.PublicKey, nil
}

func exportRsaPrivateKeyAsPem(key *rsa.PrivateKey) ([]byte, error) {
	b, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	pemBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: b,
		},
	)
	return pemBytes, nil
}

func parseRsaPrivateKeyFromPem(pemValue []byte) (*rsa.PrivateKey, error) {
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(pemValue)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

func exportRsaPublicKeyAsPem(key *rsa.PublicKey) ([]byte, error) {
	b, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		return nil, err
	}

	pemBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: b,
		},
	)

	return pemBytes, nil
}

func parseRsaPublicKeyFromPem(pemValue []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(pemValue)
	if block == nil {
		return nil, errors.New("failed to parse PEM block containing the key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	switch pub := pub.(type) {
	case *rsa.PublicKey:
		return pub, nil
	default:
		break // fall through
	}
	return nil, errors.New("Key type is not RSA")
}
