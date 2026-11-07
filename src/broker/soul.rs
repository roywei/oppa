//! The assistant's face, set in the owner's own turn: a picture they sent or
//! one drawn for them and picked. What the soul note says about it stays the
//! model's to write; the host only keeps the picture.

use super::{denied, invalid, tool, unavailable, Broker, Fault, RunGrant, TaskAccess, ToolCall};
use crate::{config::Role, harness::Access};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::Path;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SoulAvatar {
    context: String,
    file: String,
}

/// Only the owner changes who the assistant is, in a turn that may act.
fn may_set(grant: &RunGrant) -> bool {
    grant.role == Role::Owner && grant.task_access == TaskAccess::Manage
}

pub(super) fn definitions(grant: &RunGrant, context: &Value) -> Vec<Value> {
    if !may_set(grant) {
        return vec![];
    }
    vec![tool(
        "soul.avatar",
        "Make a picture your face, when the owner asks for a new look and picks it: a picture they sent, or one of the options you drew and showed them. Give its absolute path; below full access it must be a picture sent in this chat that your harness can open, or an earlier face. The host makes it square and keeps your earlier faces, so going back to one is this call with that face's path. Then describe the look in words on the Look line of assistant/soul.md, and send your contact card with messages.contact_card where the channel takes one.",
        json!({"context":context,"file":{"type":"string","minLength":1,"maxLength":4096}}),
        &["context", "file"],
        false,
    )]
}

impl Broker {
    pub(super) fn soul_avatar(&self, args: SoulAvatar) -> ToolCall {
        self.authorize(&args.context)?;
        if !may_set(&self.launch.grant) {
            return Err(denied());
        }
        let source = Path::new(&args.file);
        if !source.is_absolute() || args.file.contains('\0') {
            return Err(invalid());
        }
        let paths = self.paths().map_err(|_| unavailable())?;
        // Below full access a run opens no file but its own, so neither does
        // its face: a picture sent in this chat, which the host copied for
        // this run, or an earlier face. The face leaves with contact cards.
        let grant = &self.launch.grant;
        if grant.access != Access::Computer {
            let allowed = [
                paths.runtime.join("attachments").join(&grant.run_id),
                paths.avatars(),
            ];
            let real = source.canonicalize().ok();
            let inside = real.as_ref().is_some_and(|real| {
                allowed.iter().any(|folder| {
                    folder
                        .canonicalize()
                        .is_ok_and(|folder| real.starts_with(folder))
                })
            });
            if !inside {
                return Err(Fault {
                    code: "not_allowed_here",
                    message: "Below full access, a face can only be a picture sent in this chat or an earlier face. Nothing changed.",
                });
            }
        }
        // The face is made from the file the check resolved, not the path again.
        let source = source
            .canonicalize()
            .unwrap_or_else(|_| source.to_path_buf());
        self.journaled_effect("soul.avatar", "reversible", &args, || {
            let face = crate::soul::set_avatar(&paths, &source).map_err(|_| Fault {
                code: "not_a_picture",
                message: "That file is not a picture the host can read, or it is too large (over 20 MiB or 60 million pixels). Nothing changed.",
            })?;
            Ok(json!({"avatar":face,"earlier":crate::soul::earlier(&paths)}))
        })
    }
}
