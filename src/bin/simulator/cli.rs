use std::env;

use super::{LoadProfileKind, ScenarioKind, SimulatorArgs, USAGE};

pub(super) fn parse_args() -> Result<SimulatorArgs, String> {
    let args: Vec<String> = env::args().skip(1).collect();
    parse_args_from(&args)
}

pub(super) fn parse_args_from(args: &[String]) -> Result<SimulatorArgs, String> {
    if args.len() < 10 {
        return Err(USAGE.to_string());
    }

    let mut addr: Option<String> = None;
    let mut topic: Option<String> = None;
    let mut vehicles: Option<u64> = None;
    let mut rate: Option<u64> = None;
    let mut duration_secs: Option<u64> = None;
    let mut scenario = ScenarioKind::Steady;
    let mut load_profile = LoadProfileKind::Constant;
    let mut seed: Option<u64> = None;
    let mut quiet = false;

    let mut i = 0usize;
    while i < args.len() {
        let flag = &args[i];
        if flag == "--quiet" {
            quiet = true;
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--addr" => addr = Some(value.clone()),
            "--topic" => topic = Some(value.clone()),
            "--vehicles" => vehicles = Some(parse_positive_u64("--vehicles", value)?),
            "--rate" => rate = Some(parse_positive_u64("--rate", value)?),
            "--duration-secs" => {
                duration_secs = Some(parse_positive_u64("--duration-secs", value)?)
            }
            "--scenario" => {
                scenario = ScenarioKind::parse(value).ok_or_else(|| {
                    "scenario must be one of: steady, burst, idle, reconnect".to_string()
                })?;
            }
            "--load-profile" => {
                load_profile = LoadProfileKind::parse(value).ok_or_else(|| {
                    "load profile must be one of: constant, ramp, spike".to_string()
                })?;
            }
            "--seed" => {
                seed = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| "--seed must be a non-negative integer".to_string())?,
                );
            }
            _ => return Err(format!("unknown flag: {flag}\n{USAGE}")),
        }
        i += 2;
    }

    let parsed = SimulatorArgs {
        addr: addr.ok_or_else(|| format!("missing --addr\n{USAGE}"))?,
        topic: topic.ok_or_else(|| format!("missing --topic\n{USAGE}"))?,
        vehicles: vehicles.ok_or_else(|| format!("missing --vehicles\n{USAGE}"))?,
        rate: rate.ok_or_else(|| format!("missing --rate\n{USAGE}"))?,
        duration_secs: duration_secs.ok_or_else(|| format!("missing --duration-secs\n{USAGE}"))?,
        scenario,
        load_profile,
        seed,
        quiet,
    };

    if parsed.topic.trim().is_empty() {
        return Err("topic must not be empty".to_string());
    }

    Ok(parsed)
}

fn parse_positive_u64(name: &str, raw: &str) -> Result<u64, String> {
    let parsed = raw
        .parse::<u64>()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be > 0"));
    }
    Ok(parsed)
}
