use oppa::config::POLICY_SOURCES;

#[test]
fn every_broker_tool_family_is_session_policy() {
    // A tool family whose file is not hashed could change what the model may
    // call while cached sessions resume under the old policy.
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/broker");
    for entry in std::fs::read_dir(directory).unwrap() {
        let name = format!("broker/{}", entry.unwrap().file_name().to_string_lossy());
        assert!(
            POLICY_SOURCES.iter().any(|(source, _)| *source == name),
            "{name} is missing from config::POLICY_SOURCES"
        );
    }
    assert!(POLICY_SOURCES
        .iter()
        .any(|(source, _)| *source == "broker.rs"));
}

#[test]
fn every_broker_tool_is_one_the_harness_adapters_allow() {
    // A broker tool missing from the adapters' list is never offered to the
    // model, though a test that calls the broker directly passes: the
    // 2026-10-10 introductions drive found the people tools blocked so.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let common = std::fs::read_to_string(root.join("resources/adapters/common.py")).unwrap();
    let allowed = common
        .split("OPPA_TOOLS = (")
        .nth(1)
        .and_then(|rest| rest.split(')').next())
        .unwrap();
    let mut names = Vec::new();
    for entry in std::fs::read_dir(root.join("src/broker")).unwrap() {
        let source = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        for call in source.split("tool(").skip(1) {
            if let Some(quoted) = call.trim_start().strip_prefix('"') {
                names.push(quoted.split('"').next().unwrap().to_owned());
            }
        }
    }
    // The native permission tool is the adapters' own, allowed apart.
    names.retain(|name| name != "permission.decide");
    assert!(
        names.contains(&"memory.read".to_owned()) && names.len() >= 20,
        "{names:?}"
    );
    for name in names {
        assert!(
            allowed.contains(&format!("\"{name}\"")),
            "{name} is not in OPPA_TOOLS in resources/adapters/common.py"
        );
    }
}
