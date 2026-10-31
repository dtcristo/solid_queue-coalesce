# Agent instructions

- Keep code in the style of upstream Solid Queue.
- Don't add too much complexity.
- Optimise the implementation for correctness, simple implementation and performance, all three of them, while still in the style of Solid Queue.
- When there are multiple ways to implement something, implement it the way Solid Queue does.
- APIs should look very Rails-like.
- Never alter Solid Queue's existing tables. Only add new ones.
- Run only scoped tests locally, such as `TARGET_DB=sqlite bin/rails test test/models/solid_queue/coalesce_simple_test.rb`. CI runs the full suite and the matrix.
