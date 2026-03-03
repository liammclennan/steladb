import assert from 'node:assert';
import { test } from 'node:test';

test("fail", () => {
    assert.fail("gaaaahh");
});