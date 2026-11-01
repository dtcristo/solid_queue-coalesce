# Agent instructions

- Keep code in the style of upstream Solid Queue.
- Don't add too much complexity.
- Optimise the implementation for correctness, simple implementation and performance, all three of them, while still in the style of Solid Queue.
- When there are multiple ways to implement something, implement it the way Solid Queue does.
- APIs should look very Rails-like.
- Never alter Solid Queue's existing tables. Only add new ones.
- Add user-visible changes to the top section of `CHANGELOG.md` in the same PR, and add steps to `UPGRADING.md` if an upgrade needs them. Only record what's on `main`, not what an open issue proposes.
- Update the Compatibility section of the README when the gemspec or CI matrix changes.
- Run only scoped tests locally, such as `TARGET_DB=sqlite bin/rails test test/models/solid_queue/coalesce_simple_test.rb`. CI runs the full suite and the matrix.
