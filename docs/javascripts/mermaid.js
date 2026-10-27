import mermaid from "../vendor/mermaid/dist/mermaid.esm.min.mjs";

// Zensical renders diagrams with this global, and only falls back to a CDN
// copy when it's missing.
globalThis.mermaid = mermaid;
