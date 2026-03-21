import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export const AUTOPILOT_ROOT = resolve(__dirname, "..");
export const PROJECT_ROOT = resolve(AUTOPILOT_ROOT, "..");
