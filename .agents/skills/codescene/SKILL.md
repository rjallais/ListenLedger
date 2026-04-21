---
name: codescene
description: Guide for setting up and using the CodeScene MCP server for AI-assisted Code Health refactoring.
---

# CodeScene MCP Server Integration

This skill provides instructions for configuring and using the CodeScene MCP server to monitor and improve Code Health in the ListenLedger project.

## Setup Instructions

1. **Install the MCP Server**:
   Follow the instructions at the official repository:
   [https://github.com/codescene-oss/codescene-mcp-server](https://github.com/codescene-oss/codescene-mcp-server)

2. **Configuration**:
   You need to set up a Personal Access Token (PAT) for CodeScene.
   When the user requests CodeScene analysis, verify or set the PAT using the `set_config` tool:
   ```json
   {
     "key": "access_token",
     "value": "pat_..."
   }
   ```
   *Note: For the current project, use the token provided by the user in the initial request or prompt them for it.*

3. **ACE Access (Auto-Refactoring)**:
   To use the `code_health_auto_refactor` tool, you must configure the ACE access token:
   ```json
   {
     "key": "ace_access_token",
     "value": "<ACE_TOKEN>"
   }
   ```

## Workflow for Improving Code Health

When asked to improve the CodeScene health score for a file, follow this structured approach:

1. **Analyze the File**:
   Use `mcp_codescene_code_health_review` to get a detailed breakdown of code smells in the target file.
   Use `mcp_codescene_code_health_score` to establish a baseline score.

2. **Identify Target Smells**:
   Prioritize smells with high severity (e.g., "Bumpy Road Ahead", "Complex Method", "Code Duplication").
   - **Bumpy Road**: Look for deep nesting (2+ levels of conditionals) inside functions. Extract nested blocks into helper functions.
   - **Complex Method**: Decompose large functions (High Cyclomatic Complexity or LoC) into smaller, single-responsibility helpers.
   - **Code Duplication**: Identify structural duplication across functions and extract shared logic into parameterized helpers.

3. **Refactor Iteratively**:
   - Make small, targeted extractions to flatten loops and conditionals.
   - Ensure you run `GOEXPERIMENT=jsonv2 go build ./...` and `GOEXPERIMENT=jsonv2 go test ./...` after each refactor to verify build and test integrity.

4. **Verify Improvement**:
   Re-run `mcp_codescene_code_health_score` after refactoring. The score should increase (target is >7.0 for Green Code, 10.0 for Optimal Code).

## Common CodeScene Terminology

- **Code Health Score**: A numeric value from 1.0 (Red Code, severe technical debt) to 10.0 (Optimal Code).
- **Bumpy Road**: Multiple chunks of nested conditional logic inside a single function. A "bump" is 2 levels of conditionals.
- **Brain Method**: A large, complex function that centralizes too much logic.
- **Low Cohesion**: A module or file that contains multiple unrelated behaviors (often resolved by splitting the file).

## Important Considerations for ListenLedger

- Always preserve the `//go:build goexperiment.jsonv2` directive at the top of files.
- The project relies heavily on `context.Context` and specialized error handling. When extracting helpers, pass `ctx` if network or IO is involved.
- For `internal/handlers`, watch out for duplicated Datastar/Templ rendering patterns.
- For `internal/worker` and `internal/spotify`, watch out for duplicated retry loops and provider fallback logic.
