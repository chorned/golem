---
name: image-generation
description: Operational guide for generating images, visual mockups, diagrams, and assets using Antigravity's native built-in generate_image tool. Use whenever asked to create, render, or illustrate visuals (including 'nano banana', UI mockups, logos, diagrams).
---

# Native Antigravity Image Generation Guide

Antigravity provides a first-class, built-in tool for multimodal image generation: `generate_image`.

## Operational Workflow

When asked to generate or render an image:

1. **Invoke Built-in Tool:** Call `generate_image` directly.
   - **`Prompt`**: Rich, descriptive prompt specifying subject details, environment, lighting, artistic style, and color palette.
   - **`ImageName`**: Clean, snake_case basename (e.g. `cybernetic_nano_banana`, `fleet_topology_diagram`).
   - **`AspectRatio`**: Supported values include `'1:1'` (default), `'16:9'`, `'4:3'`, `'3:2'`, `'9:16'`.

2. **Prohibited Behaviors:**
   - DO NOT search the filesystem for "banana", "nano", or external image scripts.
   - DO NOT inspect or execute scripts in `/Users/hermes/.hermes/hermes-agent/tools/` (e.g. `image_generation_catalog.py`). Hermes Butler is an external daemon; Antigravity already possesses native image generation.
   - DO NOT write scratch Python scripts to invoke external model APIs.

3. **Slack Delivery:**
   - Once the image is generated, emit the upload directive:
     ```
     [SEND_FILE: <path_to_saved_image>]
     ```
   - Golem will automatically upload the image file directly to the conversation thread.
