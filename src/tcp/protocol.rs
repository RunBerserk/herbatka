//! TCP line protocol for Herbatka clients.
//! Parses `PRODUCE` / `FETCH` command lines into typed requests.
//! Formats broker responses into single-line wire messages.

use std::fmt;

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

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    EmptyCommand,
    UnknownCommand,
    MissingTopic,
    MissingPayload,
    MissingOffset,
    InvalidOffset,
    TooManyArguments,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            ParseError::EmptyCommand => "empty command",
            ParseError::UnknownCommand => "unknown command",
            ParseError::MissingTopic => "missing topic",
            ParseError::MissingPayload => "missing payload",
            ParseError::MissingOffset => "missing offset",
            ParseError::InvalidOffset => "invalid offset",
            ParseError::TooManyArguments => "too many arguments",
        };
        write!(f, "{message}")
    }
}

pub fn parse_request(line: &str) -> Result<Request, ParseError> {
    let line = line.trim_end();

    if line.is_empty() {
        return Err(ParseError::EmptyCommand);
    }

    if let Some(rest) = line.strip_prefix("PRODUCE ") {
        return parse_produce(rest);
    }

    if let Some(rest) = line.strip_prefix("FETCH ") {
        return parse_fetch(rest);
    }

    Err(ParseError::UnknownCommand)
}

fn parse_produce(rest: &str) -> Result<Request, ParseError> {
    let mut parts = rest.splitn(2, ' ');

    let topic = parts.next().ok_or(ParseError::MissingTopic)?;
    let payload = parts.next().ok_or(ParseError::MissingPayload)?;

    if topic.is_empty() {
        return Err(ParseError::MissingTopic);
    }

    if payload.is_empty() {
        return Err(ParseError::MissingPayload);
    }

    Ok(Request::Produce {
        topic: topic.to_string(),
        payload: payload.to_string(),
    })
}

fn parse_fetch(rest: &str) -> Result<Request, ParseError> {
    let mut parts = rest.split_whitespace();

    let topic = parts.next().ok_or(ParseError::MissingTopic)?;
    let offset = parts
        .next()
        .ok_or(ParseError::MissingOffset)?
        .parse::<u64>()
        .map_err(|_| ParseError::InvalidOffset)?;

    if parts.next().is_some() {
        return Err(ParseError::TooManyArguments);
    }

    Ok(Request::Fetch {
        topic: topic.to_string(),
        offset,
    })
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
