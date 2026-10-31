use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use reqwest::{Client, Response, Url};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct ObjectStore {
    endpoint: Url,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    client: Client,
}

pub struct ObjectInfo {
    pub size: i64,
    pub sha256: String,
}

impl ObjectStore {
    pub fn from_env() -> Result<Self, &'static str> {
        let endpoint = std::env::var("RELAYHUB_OBJECT_STORAGE_ENDPOINT")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .ok_or("object storage is disabled")?;
        let endpoint = Url::parse(&endpoint).map_err(|_| "invalid object storage endpoint")?;
        if endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || endpoint.path() != "/"
            || endpoint.query().is_some()
        {
            return Err("invalid object storage endpoint");
        }
        let bucket = required_env("RELAYHUB_OBJECT_STORAGE_BUCKET")?;
        let region = required_env("RELAYHUB_OBJECT_STORAGE_REGION")?;
        let access_key = required_env("RELAYHUB_OBJECT_STORAGE_ACCESS_KEY")?;
        let secret_key = required_env("RELAYHUB_OBJECT_STORAGE_SECRET_KEY")?;
        if !(3..=63).contains(&bucket.len()) {
            return Err("invalid object storage bucket");
        }
        Ok(Self {
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|_| "invalid object storage client")?,
        })
    }

    pub fn presign_upload(
        &self,
        key: &str,
        mime_type: &str,
        sha256_hex: &str,
        ttl: Duration,
    ) -> Result<(String, BTreeMap<String, String>), &'static str> {
        let raw = hex_decode(sha256_hex)?;
        let checksum = STANDARD.encode(raw);
        let headers = BTreeMap::from([
            ("content-type".to_owned(), mime_type.to_owned()),
            ("x-amz-checksum-sha256".to_owned(), checksum.clone()),
            (
                "x-amz-meta-relayhub-sha256".to_owned(),
                sha256_hex.to_owned(),
            ),
        ]);
        let url = self.presign("PUT", key, ttl, &headers)?;
        Ok((url, headers))
    }

    pub fn presign_download(&self, key: &str, ttl: Duration) -> Result<String, &'static str> {
        self.presign("GET", key, ttl, &BTreeMap::new())
    }

    pub async fn stat(&self, key: &str) -> Result<ObjectInfo, &'static str> {
        let request = self.signed_request("HEAD", key, &BTreeMap::new())?;
        let response = self
            .client
            .execute(request)
            .await
            .map_err(|_| "stat object")?;
        if !response.status().is_success() {
            return Err("stat object");
        }
        let size = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i64>().ok())
            .ok_or("stat object")?;
        let sha256 = response
            .headers()
            .get("x-amz-checksum-sha256")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| STANDARD.decode(v).ok())
            .map(|value| hex_encode(&value))
            .or_else(|| {
                response
                    .headers()
                    .get("x-amz-meta-relayhub-sha256")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned)
            })
            .ok_or("stat object")?;
        Ok(ObjectInfo { size, sha256 })
    }

    pub async fn get(&self, key: &str) -> Result<Response, &'static str> {
        let request = self.signed_request("GET", key, &BTreeMap::new())?;
        let response = self
            .client
            .execute(request)
            .await
            .map_err(|_| "open object")?;
        if !response.status().is_success() {
            return Err("open object");
        }
        Ok(response)
    }

    fn object_url(&self, key: &str) -> Result<Url, &'static str> {
        let mut url = self.endpoint.clone();
        let path = format!("/{}/{}", encode_path(&self.bucket), encode_path(key));
        url.set_path(&path);
        Ok(url)
    }

    fn presign(
        &self,
        method: &str,
        key: &str,
        ttl: Duration,
        headers: &BTreeMap<String, String>,
    ) -> Result<String, &'static str> {
        let mut url = self.object_url(key)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "clock")?;
        let epoch = now.as_secs();
        let (date, time) = amz_time(epoch);
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let host = url.host_str().ok_or("object storage host")?;
        let mut signed_headers = vec!["host".to_owned()];
        signed_headers.extend(headers.keys().cloned());
        signed_headers.sort();
        let signed_headers = signed_headers.join(";");
        let mut query = BTreeMap::from([
            ("X-Amz-Algorithm".to_owned(), "AWS4-HMAC-SHA256".to_owned()),
            (
                "X-Amz-Credential".to_owned(),
                format!("{}/{}", self.access_key, scope),
            ),
            ("X-Amz-Date".to_owned(), time.clone()),
            (
                "X-Amz-Expires".to_owned(),
                ttl.as_secs().clamp(1, 604800).to_string(),
            ),
            ("X-Amz-SignedHeaders".to_owned(), signed_headers.clone()),
        ]);
        let canonical_query_value = canonical_query(&query);
        let canonical_headers = canonical_headers(host, headers);
        let canonical_request = format!(
            "{method}\n{}\n{canonical_query_value}\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD",
            url.path()
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{time}\n{scope}\n{}",
            hex_encode(&Sha256::digest(canonical_request.as_bytes()))
        );
        query.insert(
            "X-Amz-Signature".to_owned(),
            self.signature(&date, &string_to_sign)?,
        );
        url.set_query(Some(&canonical_query(&query)));
        Ok(url.to_string())
    }

    fn signed_request(
        &self,
        method: &str,
        key: &str,
        headers: &BTreeMap<String, String>,
    ) -> Result<reqwest::Request, &'static str> {
        let url = self.object_url(key)?;
        let host = url.host_str().ok_or("object storage host")?.to_owned();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "clock")?;
        let (date, time) = amz_time(now.as_secs());
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let mut signed_headers = vec!["host".to_owned()];
        signed_headers.extend(headers.keys().cloned());
        signed_headers.sort();
        let signed_headers = signed_headers.join(";");
        let canonical_headers = canonical_headers(&host, headers);
        let canonical_request = format!(
            "{method}\n{}\n\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD",
            url.path()
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{time}\n{scope}\n{}",
            hex_encode(&Sha256::digest(canonical_request.as_bytes()))
        );
        let signature = self.signature(&date, &string_to_sign)?;
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key
        );
        let mut request = self
            .client
            .request(method.parse().map_err(|_| "method")?, url);
        request = request
            .header("host", &host)
            .header("x-amz-date", time)
            .header("authorization", authorization);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        request.build().map_err(|_| "build object request")
    }

    fn signature(&self, date: &str, string_to_sign: &str) -> Result<String, &'static str> {
        let k_date = hmac(
            format!("AWS4{}", self.secret_key).as_bytes(),
            date.as_bytes(),
        )?;
        let k_region = hmac(&k_date, self.region.as_bytes())?;
        let k_service = hmac(&k_region, b"s3")?;
        let k_signing = hmac(&k_service, b"aws4_request")?;
        Ok(hex_encode(&hmac(&k_signing, string_to_sign.as_bytes())?))
    }
}

fn required_env(name: &str) -> Result<String, &'static str> {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .ok_or("incomplete object storage configuration")
}
fn hmac(key: &[u8], value: &[u8]) -> Result<Vec<u8>, &'static str> {
    let mut mac = HmacSha256::new_from_slice(key).map_err(|_| "sign object request")?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}
fn hex_encode(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn hex_decode(value: &str) -> Result<Vec<u8>, &'static str> {
    if value.len() != 64 {
        return Err("invalid checksum");
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| "invalid checksum")
        })
        .collect()
}
fn amz_time(epoch: u64) -> (String, String) {
    let days = epoch / 86_400;
    let seconds = epoch % 86_400;
    let (year, month, day) = civil_from_days(days as i64);
    let hour = seconds / 3_600;
    let minute = (seconds % 3_600) / 60;
    let second = seconds % 60;
    (
        format!("{year:04}{month:02}{day:02}"),
        format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
    )
}
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    (y + if m <= 2 { 1 } else { 0 }, m, d)
}
fn encode_path(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || b"-_.~/".contains(&byte) {
                (byte as char).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}
fn encode_query(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
                (byte as char).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}
fn canonical_query(query: &BTreeMap<String, String>) -> String {
    query
        .iter()
        .map(|(key, value)| format!("{}={}", encode_query(key), encode_query(value)))
        .collect::<Vec<_>>()
        .join("&")
}
fn canonical_headers(host: &str, headers: &BTreeMap<String, String>) -> String {
    let mut values = BTreeMap::new();
    values.insert("host".to_owned(), host.trim().to_ascii_lowercase());
    for (key, value) in headers {
        values.insert(key.to_ascii_lowercase(), value.trim().to_owned());
    }
    values
        .into_iter()
        .map(|(key, value)| format!("{key}:{value}\n"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::ObjectStore;

    #[test]
    fn presigned_upload_binds_content_type_and_checksum() {
        let store = ObjectStore {
            endpoint: reqwest::Url::parse("https://objects.example/").unwrap(),
            bucket: "relayhub-files".into(),
            region: "us-east-1".into(),
            access_key: "access".into(),
            secret_key: "secret".into(),
            client: reqwest::Client::new(),
        };
        let (url, headers) = store
            .presign_upload(
                "realtime/app/file",
                "image/png",
                &"a".repeat(64),
                std::time::Duration::from_secs(900),
            )
            .unwrap();
        assert!(url.starts_with("https://objects.example/relayhub-files/realtime/app/file?"));
        assert_eq!(
            headers.get("content-type").map(String::as_str),
            Some("image/png")
        );
        assert!(url.contains("X-Amz-Signature="));
    }
}
