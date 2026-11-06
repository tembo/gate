//! Single-snippet TypeScript transformation. No module resolution or type checking.
use oxc::{
    allocator::Allocator,
    ast::AstKind,
    codegen::{Codegen, CodegenOptions},
    diagnostics::OxcDiagnostic,
    parser::Parser,
    semantic::SemanticBuilder,
    span::SourceType,
    transformer::{TransformOptions, Transformer},
};
use std::{path::Path, sync::LazyLock};

const PREFIX: &str = "(async () => {\n";
pub const GENERATED_FILENAME: &str = "gate-script.js";

#[derive(Debug)]
pub struct CompiledScript {
    pub code: String,
    map: oxc_sourcemap::SourceMap,
    source_lines: u32,
}

impl CompiledScript {
    /// Rewrite only frames belonging to this compilation, never error messages or
    /// locations from the runtime, dynamic eval, or the generated async wrapper.
    pub fn map_stack(&self, stack: &str) -> String {
        static FRAME: LazyLock<regex::Regex> = LazyLock::new(|| {
            regex::Regex::new(
                r"(?m)^([ \t]+at (?:[^\r\n]* \()?)gate-script\.js:([0-9]+):([0-9]+)(\)?[ \t]*)$",
            )
            .expect("valid stack frame pattern")
        });
        let lookup = self.map.generate_lookup_table();
        FRAME
            .replace_all(stack, |frame: &regex::Captures<'_>| {
                let location = frame[2]
                    .parse::<u32>()
                    .ok()
                    .zip(frame[3].parse::<u32>().ok())
                    .and_then(|(line, column)| {
                        self.map
                            .lookup_token(&lookup, line.checked_sub(1)?, column.checked_sub(1)?)
                    })
                    .filter(|token| {
                        token.get_source_id().is_some()
                            && (1..=self.source_lines).contains(&token.get_src_line())
                    });
                match location {
                    // Source-map lines are zero-based. Removing the wrapper's first
                    // line makes these the snippet's one-based line numbers.
                    Some(token) => format!(
                        "{}script.ts:{}:{}{}",
                        &frame[1],
                        token.get_src_line(),
                        token.get_src_col() + 1,
                        &frame[4]
                    ),
                    None => frame[0].to_owned(),
                }
            })
            .into_owned()
    }
}

pub fn compile(code: &str) -> Result<CompiledScript, String> {
    let source = format!("{PREFIX}{code}\n}})()");
    let allocator = Allocator::default();
    let parsed = Parser::new(&allocator, &source, SourceType::ts().with_script(true)).parse();
    if let Some(error) = parsed.errors.first() {
        return Err(diagnostic(error, &source, code.len()));
    }
    let mut program = parsed.program;
    let semantic = SemanticBuilder::new()
        .with_check_syntax_error(true)
        .build(&program);
    if let Some(error) = semantic.errors.first() {
        return Err(diagnostic(error, &source, code.len()));
    }
    if semantic
        .semantic
        .nodes()
        .iter()
        .any(|node| matches!(node.kind(), AstKind::Decorator(_)))
    {
        return Err("unsupported script syntax: decorators are not supported".into());
    }
    if semantic
        .semantic
        .nodes()
        .iter()
        .any(|node| node.kind().is_module_declaration())
    {
        return Err("unsupported script syntax: static imports and exports are not available in function bodies".into());
    }
    let options = TransformOptions::default();
    let transformed = Transformer::new(&allocator, Path::new("snippet.ts"), &options)
        .build_with_scoping(semantic.semantic.into_scoping(), &mut program);
    if let Some(error) = transformed.errors.first() {
        return Err(diagnostic(error, &source, code.len()));
    }
    // QuickJS has no module loader or Babel helper library. Fail before execution
    // rather than emitting code which depends on an unavailable runtime package.
    #[allow(deprecated)]
    let needs_helpers = !transformed.helpers_used.is_empty();
    if needs_helpers
        || program
            .body
            .iter()
            .any(|statement| statement.is_module_declaration())
    {
        return Err("unsupported script syntax: external modules and transform runtime helpers are not available".into());
    }
    let output = Codegen::new()
        .with_options(CodegenOptions {
            source_map_path: Some("script.ts".into()),
            ..Default::default()
        })
        .build(&program);
    Ok(CompiledScript {
        code: output.code,
        map: output.map.expect("source maps are enabled"),
        source_lines: code.bytes().filter(|byte| *byte == b'\n').count() as u32 + 1,
    })
}

fn diagnostic(error: &OxcDiagnostic, source: &str, code_len: usize) -> String {
    let offset = error
        .labels
        .as_ref()
        .and_then(|labels| labels.first())
        .map_or(0, |label| label.offset())
        .min(PREFIX.len() + code_len);
    let prefix = &source[..source.floor_char_boundary(offset)];
    // The async wrapper occupies the first line; report the caller's source location.
    let line = prefix.bytes().filter(|b| *b == b'\n').count().max(1);
    let column = prefix.rsplit('\n').next().unwrap_or("").chars().count() + 1;
    format!("SyntaxError at line {line}, column {column}: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn error_positions_exclude_the_async_wrapper() {
        assert!(compile("return (").unwrap_err().contains("line 1"));
        assert!(
            compile("const x = 1;\nconst y: = 2;")
                .unwrap_err()
                .contains("line 2")
        );
    }
    #[test]
    fn rejects_module_and_jsx_syntax() {
        for source in ["import x from 'package'; return x;", "return <div />;"] {
            assert!(compile(source).is_err(), "{source}");
        }
    }

    #[test]
    fn rejects_decorators_before_execution() {
        for source in [
            "@decorate class Example {}",
            "class Example { @decorate method() {} }",
            "class Example { @decorate field = 1; }",
            "return class { @decorate method() {} };",
        ] {
            assert_eq!(
                compile(source).unwrap_err(),
                "unsupported script syntax: decorators are not supported",
                "{source}"
            );
        }
    }

    #[test]
    fn leaves_unmapped_stack_locations_unchanged() {
        let script = compile("throw new Error('boom');").unwrap();
        for stack in [
            "    at <anonymous> (eval_script:2:11)\n",
            "    at <eval> (<input>:1:11)\n",
            "    at <eval> (gate-script.js:1:1)\n",
            "    at <eval> (gate-script.js:999:1)\n",
            "    at <eval> (gate-script.js:0:0)\n",
            "    at <eval> (gate-script.js:999999999999999999999:1)\n",
            "    at native (native)\n",
            "a message mentioning gate-script.js:2:11",
        ] {
            assert_eq!(script.map_stack(stack), stack);
        }
    }
}
