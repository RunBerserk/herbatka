#[derive(Debug, PartialEq, Eq)]
pub enum Request {
    Produce { topic: String, payload: String },
    Fetch { topic: String, offset: u64 },
}

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    OkOffset(u64),
    Message { offset: u64, payload: String },
    None,
    Error(String),
}

pub fn parse_request(line: &str) -> Result<Request, &'static str> {
    let line = line.trim_end();

    if line.is_empty() {
        return Err("empty command");
    }

    if let Some(rest) = line.strip_prefix("PRODUCE ") {
        let mut parts = rest.splitn(2, ' ');

        let topic = parts.next().ok_or("missing topic")?;
        let payload = parts.next().ok_or("missing payload")?;

        if topic.is_empty() {
            return Err("missing topic");
        }

        if payload.is_empty() {
            return Err("missing payload");
        }

        return Ok(Request::Produce {
            topic: topic.to_string(),
            payload: payload.to_string(),
        });
    }

    if let Some(rest) = line.strip_prefix("FETCH ") {
        let mut parts = rest.split_whitespace();

        let topic = parts.next().ok_or("missing topic")?;
        let offset = parts
            .next()
            .ok_or("missing offset")?
            .parse::<u64>()
            .map_err(|_| "invalid offset")?;

        if parts.next().is_some() {
            return Err("too many arguments");
        }

        return Ok(Request::Fetch {
            topic: topic.to_string(),
            offset,
        });
    }

    Err("unknown command")
}

pub fn format_response(response: &Response) -> String {
    match response {
        Response::OkOffset(offset) => format!("OK {offset}\n"),
        Response::Message { offset, payload } => format!("MSG {offset} {payload}\n"),
        Response::None => "NONE\n".to_string(),
        Response::Error(reason) => format!("ERR {reason}\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::{Request, Response, format_response, parse_request};

    #[test]
    fn parse_produce_with_spaces_in_payload() {
        let req = parse_request("PRODUCE cars speed is 120").expect("parse should succeed");
        assert_eq!(
            req,
            Request::Produce {
                topic: "cars".to_string(),
                payload: "speed is 120".to_string()
            }
        );
    }

    #[test]
    fn parse_fetch() {
        let req = parse_request("FETCH cars 42").expect("parse should succeed");
        assert_eq!(
            req,
            Request::Fetch {
                topic: "cars".to_string(),
                offset: 42
            }
        );
    }

    #[test]
    fn parse_errors() {
        assert!(parse_request("").is_err());
        assert!(parse_request("PING").is_err());
        assert!(parse_request("FETCH cars nope").is_err());
        assert!(parse_request("PRODUCE cars").is_err());
    }

    #[test]
    fn format_responses() {
        assert_eq!(
            format_response(&Response::OkOffset(3)),
            "OK 3\n".to_string()
        );
        assert_eq!(
            format_response(&Response::Message {
                offset: 4,
                payload: "hello".to_string()
            }),
            "MSG 4 hello\n".to_string()
        );
        assert_eq!(format_response(&Response::None), "NONE\n".to_string());
        assert_eq!(
            format_response(&Response::Error("bad".to_string())),
            "ERR bad\n".to_string()
        );
    }
}
