//! Compatibility crypto for secrets written by the legacy Go runtime.
//!
//! The format is intentionally byte-for-byte compatible with
//! backend/internal/crypto/secrets.go: AES-256-GCM, the nonce prefixed to the
//! ciphertext, `rhsec:v1:` as associated data and unpadded URL-safe base64.

use aes_gcm::{Aes256Gcm, KeyInit, Nonce, aead::Aead};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};

const PREFIX: &str = "rhsec:v1:";
const NONCE_BYTES: usize = 12;

#[derive(Clone)]
pub struct SecretCipher(Aes256Gcm);

impl SecretCipher {
    pub fn from_base64(encoded_key: &str) -> Result<Self, &'static str> {
        let key = STANDARD
            .decode(encoded_key.trim())
            .map_err(|_| "invalid secret encryption key")?;
        if key.len() != 32 {
            return Err("invalid secret encryption key");
        }
        Ok(Self(
            Aes256Gcm::new_from_slice(&key).map_err(|_| "invalid secret encryption key")?,
        ))
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<String, &'static str> {
        let nonce_bytes: [u8; NONCE_BYTES] = rand_nonce();
        let nonce = Nonce::from_slice(&nonce_bytes);
        let mut sealed = nonce_bytes.to_vec();
        sealed.extend(
            self.0
                .encrypt(
                    nonce,
                    aes_gcm::aead::Payload {
                        msg: plaintext,
                        aad: PREFIX.as_bytes(),
                    },
                )
                .map_err(|_| "secret encryption failed")?,
        );
        Ok(format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(sealed)))
    }

    pub fn decrypt(&self, encoded: &str) -> Result<Vec<u8>, &'static str> {
        let value = encoded
            .strip_prefix(PREFIX)
            .ok_or("unsupported secret ciphertext version")?;
        let sealed = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| "invalid secret ciphertext")?;
        if sealed.len() < NONCE_BYTES + 16 {
            return Err("invalid secret ciphertext");
        }
        let nonce = Nonce::from_slice(&sealed[..NONCE_BYTES]);
        self.0
            .decrypt(
                nonce,
                aes_gcm::aead::Payload {
                    msg: &sealed[NONCE_BYTES..],
                    aad: PREFIX.as_bytes(),
                },
            )
            .map_err(|_| "invalid secret ciphertext")
    }
}

fn rand_nonce() -> [u8; NONCE_BYTES] {
    let mut nonce = [0u8; NONCE_BYTES];
    getrandom::fill(&mut nonce).expect("OS randomness unavailable");
    nonce
}

#[cfg(test)]
mod tests {
    use super::SecretCipher;
    use base64::{Engine, engine::general_purpose::STANDARD};

    #[test]
    fn round_trip_uses_go_ciphertext_prefix() {
        let cipher = SecretCipher::from_base64(&STANDARD.encode([7u8; 32])).unwrap();
        let encoded = cipher.encrypt(b"rhs_super-secret").unwrap();
        assert!(encoded.starts_with("rhsec:v1:"));
        assert_eq!(cipher.decrypt(&encoded).unwrap(), b"rhs_super-secret");
    }

    #[test]
    fn rejects_wrong_associated_data_version() {
        let cipher = SecretCipher::from_base64(&STANDARD.encode([7u8; 32])).unwrap();
        assert!(cipher.decrypt("rhsec:v2:AAAA").is_err());
    }
}
