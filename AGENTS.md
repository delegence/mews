The project is under active development. therefore no migrations or backward compatibility logic, because breaking changes are fine as long as feature is implemented and doesnt break anything else. just mention user to restart / cleanup `~/.mews/` if required for real testing.
Prefer the smallest typed solution. No overengineering and overoptimizing.
Design the system wiht building blocks principle.
Comment non-obvious product behavior and important invariants, not ordinary control flow.
Add focused tests for changed happy paths, customer-visible behavior, and bugs. Avoid speculative edge-case matrices and test-count padding.

Preserve unrelated working-tree changes.
Always work in the main branch no need for seperate worktrees.
Use Conventional Commits for commit messages.
