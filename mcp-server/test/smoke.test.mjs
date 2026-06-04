// Smoke test that node:test infrastructure works.
import { test } from 'node:test';
import { strict as assert } from 'node:assert';

test('node:test infrastructure works', () => {
  assert.equal(1 + 1, 2);
});
