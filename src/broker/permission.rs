//! The harness's permission handler. A full-access run names
//! `permission.decide` as the native CLI's permission prompt tool, so a native
//! call that needs permission asks the host first; the CLI still runs calls it
//! deems read-only, such as `whoami`, without asking. The host answers from the
//! run's access, never from the model, and journals each decision by tool name
//! only. An allowed call has effects the broker cannot see, so it marks the run
//! as unmediated; the adapter marks every other native call the same way.

use super::{atomic_json, read_json, unavailable, Broker, ToolCall};
use crate::harness::Access;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// At `connectors` access, every MCP tool the owner configured in the harness
/// (claude.ai connectors, their own MCP servers, Claude in Chrome) and the web
/// tools, never the shell or files.
const MCP_TOOL_PREFIX: &str = "mcp__";
const WEB_TOOLS: [&str; 2] = ["WebFetch", "WebSearch"];

#[derive(Default, Serialize, Deserialize)]
pub(super) struct Decisions {
    pub allowed: Vec<String>,
    pub denied: Vec<String>,
}

#[derive(Deserialize)]
struct Request {
    tool_name: String,
    input: Value,
}

pub(super) fn definitions(access: Access) -> Vec<Value> {
    if access.is_read() {
        return vec![];
    }
    vec![json!({"name":"permission.decide",
        "description":"The harness's permission handler. The host decides from the owner's access setting; calling it yourself grants nothing.",
        "inputSchema":{"type":"object","properties":{"tool_name":{"type":"string","maxLength":256},"input":{"type":"object"}},
            "required":["tool_name","input"]},
        "annotations":{"readOnlyHint":true,"destructiveHint":false,"idempotentHint":true,"openWorldHint":false}})]
}

impl Broker {
    pub(super) fn permission_decide(&self, args: Value) -> ToolCall {
        let request: Request = serde_json::from_value(args).map_err(|_| super::invalid())?;
        let grant = &self.launch.grant;
        // The checks `authorize` makes, except the context the harness cannot send.
        let current = Utc::now().timestamp() < grant.expires_at
            && !self.forgot()
            && self.validate_paths().is_ok()
            && self.validate_binding().is_ok()
            && self.validate_input().is_ok();
        let allowed = current
            && match grant.access {
                Access::Read => false,
                Access::Connectors => {
                    request.tool_name.starts_with(MCP_TOOL_PREFIX)
                        || WEB_TOOLS.contains(&request.tool_name.as_str())
                }
                Access::Computer => true,
            };
        {
            let _lock = self.lock().map_err(|_| unavailable())?;
            let mut decisions = self.decisions().map_err(|_| unavailable())?;
            let list = if allowed {
                &mut decisions.allowed
            } else {
                &mut decisions.denied
            };
            list.push(request.tool_name.clone());
            atomic_json(&self.run_directory.join("permissions.json"), &decisions)
                .map_err(|_| unavailable())?;
        }
        Ok(if allowed {
            json!({"behavior":"allow","updatedInput":request.input})
        } else {
            json!({"behavior":"deny","message":"The owner's access setting for this assistant does not allow this tool."})
        })
    }

    pub(super) fn decisions(&self) -> anyhow::Result<Decisions> {
        let path = self.run_directory.join("permissions.json");
        if path.exists() {
            read_json(&path)
        } else {
            Ok(Decisions::default())
        }
    }
}
