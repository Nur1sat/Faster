use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, OsRng, rand_core::RngCore},
};
use base64::{Engine, engine::general_purpose::STANDARD};

use crate::error::AppError;

#[derive(Clone)]
pub struct AtRestCipher {
    cipher: Aes256Gcm,
}

impl AtRestCipher {
    pub fn from_base64_key(key_b64: &str) -> Result<Self, AppError> {
        let key = STANDARD
            .decode(key_b64)
            .map_err(|e| AppError::BadRequest(format!("invalid base64 encryption key: {e}")))?;

        if key.len() != 32 {
            return Err(AppError::BadRequest(
                "encryption key must be 32 bytes after base64 decode".to_string(),
            ));
        }

        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|e| AppError::BadRequest(format!("invalid encryption key: {e}")))?;

        Ok(Self { cipher })
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<(Vec<u8>, [u8; 12]), AppError> {
        let mut nonce_bytes = [0_u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| AppError::Storage(format!("encryption failed: {e}")))?;
        Ok((ciphertext, nonce_bytes))
    }

    pub fn decrypt(&self, ciphertext: &[u8], nonce: [u8; 12]) -> Result<Vec<u8>, AppError> {
        let nonce = Nonce::from_slice(&nonce);
        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| AppError::Storage(format!("decryption failed: {e}")))
    }
}
