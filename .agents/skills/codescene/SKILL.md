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
   The MCP server should be configured via environment variables injected from
   CI/secret manager. **Never store PAT/ACE tokens in local config files or
   check them into version control.**

   Set the canonical environment variables before running:

   | Config Key      | Environment Variable   | Description                          |
   |-----------------|------------------------|--------------------------------------|
   | `access_token`  | `CS_ACCESS_TOKEN`      | CodeScene PAT or standalone license |
   | `ace_access_token` | `CS_ACE_ACCESS_TOKEN` | ACE token for auto-refactoring      |
   | `default_project_id` | `CS_DEFAULT_PROJECT_ID` | Default CodeScene project ID     |

   *Never ask users to paste live PATs or ACE tokens into chat. Store them in
   the secret manager / CI settings and pass them at runtime. Rotate tokens
   periodically and use short-lived credentials when possible.*

3. **ACE Access (Auto-Refactoring)**:
   To use the `code_health_auto_refactor` tool, set the `CS_ACE_ACCESS_TOKEN`
   environment variable from your secret manager.

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
   Re-run `mcp_codescene_code_health_score` after refactoring. The score should increase (project target is >8.0; 10.0 is Optimal Code).

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
