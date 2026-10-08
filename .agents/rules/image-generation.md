# Image Generation: Built-In Antigravity Tooling vs Hermes Butler

This rule codifies the operational contract for image generation, visual rendering, and asset design in Golem / First Mate.

---

## 1. Native Built-In Tooling (`generate_image`)

- **Built-in First:** Antigravity (`agy`) includes native, built-in multimodal image generation via the `generate_image` tool.
- **Direct Invocation:** Whenever Carl or Golem requests an image, diagram, UI mockup, concept art, logo, or visual asset (e.g., "nano banana", architecture diagrams, illustrations):
  1. Call Antigravity's native `generate_image` tool directly in a single step.
  2. Specify:
     - `Prompt`: Detailed, rich visual description covering subject, lighting, composition, textures, and style.
     - `ImageName`: Concise snake_case identifier (e.g., `nano_banana`, `architecture_diagram`).
     - `AspectRatio`: Target ratio (e.g. `1:1`, `16:9`, `4:3`, `3:2`).
  3. The resulting image is saved directly into the session's artifact directory.

---

## 2. Strict Prohibition on External Tools & Filesystem Probing

- **Zero Filesystem Wandering:** NEVER run filesystem searches (`grep_search`, `list_dir`, `find`, `ls`) searching for "banana", "nano", "image-generator", or model catalogs.
- **Hermes Butler Isolation:** NEVER inspect, import, or execute tools from `/Users/hermes/.hermes/hermes-agent/tools/` (including `image_generation_catalog.py` or `image_generation_managed.py`). Those scripts belong to the Hermes Butler homelab daemon, NOT Antigravity.
- **No Third-Party Scratch Scripts:** Do not create ad-hoc Python scripts calling external APIs (Fal.ai, Replicate, OpenAI) or invoke subagent sandboxes for image generation. Antigravity's built-in `generate_image` handles image generation natively.

---

## 3. Slack Exfiltration Directive (`[SEND_FILE: <path>]`)

- Once `generate_image` completes, extract the saved file path from the tool response (e.g., `/Users/hermes/.../nano_banana.jpg` or relative artifact path).
- Always include the Slack file directive in the final response:
  ```
  [SEND_FILE: <path_to_generated_image>]
  ```
- Golem's transport layer detects `[SEND_FILE: <path>]` and automatically uploads the rendered image into Carl's Slack thread.
