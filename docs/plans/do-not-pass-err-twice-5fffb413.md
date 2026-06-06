# Do not pass err twice

## Implementation Steps

### Task 1: Do not pass err twice

- [ ] In the calls like this: `framework.ExpectNoError(err, "failed to start Container: %v", err)` do not pass the `err` twice. Just the first one should be enough.
