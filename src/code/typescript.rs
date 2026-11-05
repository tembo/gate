//! Single-snippet TypeScript transformation. No module resolution or type checking.
use oxc::{
    allocator::Allocator,
    codegen::Codegen,
    diagnostics::OxcDiagnostic,
    parser::Parser,
    semantic::SemanticBuilder,
    span::SourceType,
    transformer::{TransformOptions, Transformer},
};
use std::path::Path;

const PREFIX: &str = "(async () => {\n";

pub fn compile(code: &str) -> Result<String, String> {
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
    Ok(Codegen::new().build(&program).code)
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
}
