use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    println!("cargo:rustc-check-cfg=cfg(mesh_event_has_atx_metrics)");
    if detect_mesh_atx_metrics().unwrap_or(false) {
        println!("cargo:rustc-cfg=mesh_event_has_atx_metrics");
    }
}

fn detect_mesh_atx_metrics() -> std::io::Result<bool> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let lock_path = manifest_dir.join("Cargo.lock");
    let lock = fs::read_to_string(lock_path)?;

    let mut capture = false;
    let mut version: Option<String> = None;
    for line in lock.lines() {
        let trimmed = line.trim();
        if trimmed == "name = \"chirpstack_api\"" {
            capture = true;
            continue;
        }
        if capture && trimmed.starts_with("version = ") {
            let v = trimmed
                .trim_start_matches("version = ")
                .trim_matches('"')
                .to_string();
            version = Some(v);
            break;
        }
        if trimmed.starts_with("name = ") {
            capture = false;
        }
    }

    let version = match version {
        Some(v) => v,
        None => return Ok(false),
    };

    let cargo_home = env::var("CARGO_HOME").unwrap_or_else(|_| {
        env::var("HOME")
            .map(|home| format!("{}/.cargo", home))
            .unwrap_or_else(|_| ".cargo".into())
    });

    let registry_src = PathBuf::from(cargo_home).join("registry/src");
    if !registry_src.exists() {
        return Ok(false);
    }

    let mut proto_path: Option<PathBuf> = None;
    for entry in fs::read_dir(registry_src)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let candidate = path
            .join(format!("chirpstack_api-{}", version))
            .join("proto/chirpstack/gw/gw.proto");
        if candidate.exists() {
            proto_path = Some(candidate);
            break;
        }
    }

    let proto_path = match proto_path {
        Some(p) => p,
        None => return Ok(false),
    };

    let proto = fs::read_to_string(proto_path)?;
    Ok(proto.contains("advertised_atx_path_cost") || proto.contains("advertised_atx_depth"))
}
