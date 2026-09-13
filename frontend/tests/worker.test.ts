import assert from "node:assert/strict";
import test from "node:test";

import { registeredTasks } from "../src/lib/worker.ts";

test("registeredTasks sorts an advertised capability inventory", () => {
  assert.deepEqual(
    registeredTasks({ registered_tasks: ["tag.apply", "parse.parse_document", "embed.batch"] }),
    ["embed.batch", "parse.parse_document", "tag.apply"],
  );
});

test("registeredTasks handles an older engine that omitted capabilities", () => {
  assert.deepEqual(registeredTasks({}), []);
});
