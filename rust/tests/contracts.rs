use base64::{Engine, engine::general_purpose::STANDARD};
use relayhub::protocol::{
    Accepted, DeliveryLease, EndpointPolicy, EventInput, HttpResponse, Principal, ProtocolError,
    parse_frame,
};
use serde_json::{Value, json};

fn fixtures() -> Value {
    serde_json::from_str(include_str!("../../contracts/v1/fixtures.json")).unwrap()
}

fn schema() -> jsonschema::Validator {
    let schema: Value =
        serde_json::from_str(include_str!("../../contracts/v1/schema.json")).unwrap();
    jsonschema::options()
        .should_validate_formats(true)
        .build(&schema)
        .unwrap()
}

fn parse(value: &Value) -> Result<relayhub::protocol::Frame, ProtocolError> {
    parse_frame(&serde_json::to_vec(value).unwrap())
}

#[test]
fn golden_frames_roundtrip() {
    let fixtures = fixtures();
    let frames = fixtures["frames"].as_array().unwrap();
    let schema = schema();
    assert_eq!(frames.len(), 7); // Six variants, plus delivery-lease heartbeat.
    for expected in frames {
        schema.validate(expected).unwrap();
        let frame = parse(expected).unwrap();
        assert_eq!(serde_json::to_value(&frame).unwrap(), *expected);
        assert_eq!(
            parse_frame(&serde_json::to_vec(&frame).unwrap()).unwrap(),
            frame
        );
    }
    fn roundtrip<T: serde::de::DeserializeOwned + serde::Serialize>(expected: &Value) {
        let typed: T = serde_json::from_value(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(typed).unwrap(), *expected);
    }
    for identity in fixtures["identities"].as_array().unwrap() {
        roundtrip::<Principal>(identity);
    }
    roundtrip::<EventInput>(&fixtures["event_input"]);
    roundtrip::<Accepted>(&fixtures["accepted"]);
    roundtrip::<DeliveryLease>(&fixtures["delivery_lease"]);
    roundtrip::<HttpResponse>(&fixtures["http_response"]);
    for policy in fixtures["endpoint_policies"].as_array().unwrap() {
        roundtrip::<EndpointPolicy>(policy);
        serde_json::from_value::<EndpointPolicy>(policy.clone())
            .unwrap()
            .validate()
            .unwrap();
    }
    for (name, values) in [
        (
            "principal",
            fixtures["identities"].as_array().unwrap().clone(),
        ),
        (
            "endpoint_policy",
            fixtures["endpoint_policies"].as_array().unwrap().clone(),
        ),
        ("event_input", vec![fixtures["event_input"].clone()]),
        ("accepted", vec![fixtures["accepted"].clone()]),
        ("delivery_lease", vec![fixtures["delivery_lease"].clone()]),
        ("http_response", vec![fixtures["http_response"].clone()]),
    ] {
        let mut definition: Value =
            serde_json::from_str(include_str!("../../contracts/v1/schema.json")).unwrap();
        definition.as_object_mut().unwrap().remove("oneOf");
        definition["$ref"] = json!(format!("#/$defs/{name}"));
        let validator = jsonschema::options()
            .should_validate_formats(true)
            .build(&definition)
            .unwrap();
        for value in values {
            validator.validate(&value).unwrap();
        }
    }
}

#[test]
fn reject_unknown_version() {
    let schema = schema();
    for frame in fixtures()["frames"].as_array().unwrap() {
        for version in [json!(0), json!(2), json!(65535)] {
            let mut invalid = frame.clone();
            invalid["version"] = version;
            assert!(!schema.is_valid(&invalid));
            assert_eq!(parse(&invalid), Err(ProtocolError::UnsupportedVersion));
        }
    }
}

#[test]
fn reject_body_over_1048576() {
    let schema = schema();
    for (index, path) in [(1, "/input/body"), (3, "/response/body"), (4, "/body")] {
        for size in [0, 1_048_575, 1_048_576, 1_048_577] {
            let mut frame = fixtures()["frames"][index].clone();
            *frame.pointer_mut(path).unwrap() = json!(STANDARD.encode(vec![0xff; size]));
            let result = parse(&frame);
            if size > 1_048_576 {
                assert!(!schema.is_valid(&frame));
                assert_eq!(result, Err(ProtocolError::BodyTooLarge));
            } else {
                schema.validate(&frame).unwrap();
                let encoded = serde_json::to_value(result.unwrap()).unwrap();
                assert_eq!(encoded, frame);
                assert_eq!(
                    STANDARD
                        .decode(encoded.pointer(path).unwrap().as_str().unwrap())
                        .unwrap()
                        .len(),
                    size
                );
            }
        }
    }
}

#[test]
fn preserve_binary_and_duplicate_headers() {
    for (index, body, headers, expected_headers) in [
        (
            1,
            "/input/body",
            "/input/headers",
            json!([["X-Tag", "one"], ["X-Tag", "two"]]),
        ),
        (
            3,
            "/response/body",
            "/response/headers",
            json!([["Set-Cookie", "a=1"], ["Set-Cookie", "b=2"]]),
        ),
    ] {
        let mut expected = fixtures()["frames"][index].clone();
        let binary: Vec<u8> = (0..=255).collect();
        *expected.pointer_mut(body).unwrap() = json!(STANDARD.encode(&binary));
        *expected.pointer_mut(headers).unwrap() = expected_headers.clone();
        let actual = serde_json::to_value(parse(&expected).unwrap()).unwrap();
        assert_eq!(actual.pointer(headers).unwrap(), &expected_headers);
        assert_eq!(
            STANDARD
                .decode(actual.pointer(body).unwrap().as_str().unwrap())
                .unwrap(),
            binary
        );
        assert_eq!(actual, expected);
    }
}

#[test]
fn reject_malformed_frames() {
    for raw in [b"{}".as_slice(), b"[]", b"null", b"{}{}", b"\xff"] {
        assert_eq!(parse_frame(raw), Err(ProtocolError::InvalidFrame));
    }
    let valid = fixtures()["frames"][0].clone();
    for (field, value) in [
        ("type", json!("unknown")),
        ("request_id", json!("not-a-uuid")),
        ("workspace_id", json!("forged")),
        ("max_in_flight", json!(0)),
        ("max_in_flight", json!(17)),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert_eq!(parse(&invalid), Err(ProtocolError::InvalidFrame));
    }
    for body in [json!("%%%"), json!([0, 1, 255])] {
        let mut invalid = fixtures()["frames"][3].clone();
        invalid["response"]["body"] = body;
        assert_eq!(parse(&invalid), Err(ProtocolError::InvalidFrame));
    }
    for status in [99, 101, 600] {
        let mut invalid = fixtures()["frames"][3].clone();
        invalid["response"]["status"] = json!(status);
        assert_eq!(parse(&invalid), Err(ProtocolError::InvalidFrame));
    }
}
