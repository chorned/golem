---
name: Retro 16-Bit Garage
colors:
  surface: '#131315'
  surface-dim: '#131315'
  surface-bright: '#39393b'
  surface-container-lowest: '#0e0e10'
  surface-container-low: '#1b1b1d'
  surface-container: '#201f21'
  surface-container-high: '#2a2a2c'
  surface-container-highest: '#353437'
  on-surface: '#e5e1e4'
  on-surface-variant: '#e9bcb5'
  inverse-surface: '#e5e1e4'
  inverse-on-surface: '#303032'
  outline: '#b08781'
  outline-variant: '#5f3f3a'
  surface-tint: '#ffb4a8'
  primary: '#ffb4a8'
  on-primary: '#690000'
  primary-container: '#e60000'
  on-primary-container: '#fff7f5'
  inverse-primary: '#c00000'
  secondary: '#fff9ef'
  on-secondary: '#3a3000'
  secondary-container: '#ffdb3c'
  on-secondary-container: '#725f00'
  tertiary: '#00e475'
  on-tertiary: '#003918'
  tertiary-container: '#008441'
  on-tertiary-container: '#e8ffe7'
  error: '#ffb4ab'
  on-error: '#690005'
  error-container: '#93000a'
  on-error-container: '#ffdad6'
  primary-fixed: '#ffdad4'
  primary-fixed-dim: '#ffb4a8'
  on-primary-fixed: '#410000'
  on-primary-fixed-variant: '#930100'
  secondary-fixed: '#ffe16d'
  secondary-fixed-dim: '#e9c400'
  on-secondary-fixed: '#221b00'
  on-secondary-fixed-variant: '#544600'
  tertiary-fixed: '#62ff96'
  tertiary-fixed-dim: '#00e475'
  on-tertiary-fixed: '#00210b'
  on-tertiary-fixed-variant: '#005226'
  background: '#131315'
  on-background: '#e5e1e4'
  surface-variant: '#353437'
  crimson-field: '#8a0c14'
  crimson-dark: '#4a0509'
  crimson-bright: '#e60000'
  steel-rim-light: '#8e929b'
  steel-plate: '#494d55'
  steel-shadow: '#1e2024'
  stone-plaque-face: '#585b62'
  stone-plaque-bevel: '#8a8f98'
  gold-active: '#ffd700'
  gold-active-dim: '#a38900'
  goblin-green: '#00e676'
  goblin-shadow: '#006622'
  thruster-cyan: '#00f0ff'
  thruster-blue: '#1a4fff'
  black-void: '#08080a'
  pip-active: '#ffd700'
  pip-empty: '#2c2e33'
typography:
  display:
    fontFamily: Space Grotesk
    fontSize: 36px
    fontWeight: '700'
    lineHeight: 44px
    letterSpacing: 0.08em
  display-mobile:
    fontFamily: Space Grotesk
    fontSize: 24px
    fontWeight: '700'
    lineHeight: 32px
    letterSpacing: 0.06em
  headline-lg:
    fontFamily: Space Grotesk
    fontSize: 26px
    fontWeight: '700'
    lineHeight: 34px
    letterSpacing: 0.06em
  headline-lg-mobile:
    fontFamily: Space Grotesk
    fontSize: 20px
    fontWeight: '700'
    lineHeight: 26px
    letterSpacing: 0.05em
  headline-md:
    fontFamily: Space Grotesk
    fontSize: 18px
    fontWeight: '700'
    lineHeight: 24px
    letterSpacing: 0.05em
  headline-sm:
    fontFamily: Space Grotesk
    fontSize: 15px
    fontWeight: '700'
    lineHeight: 20px
    letterSpacing: 0.04em
  body-lg:
    fontFamily: Space Mono
    fontSize: 15px
    fontWeight: '400'
    lineHeight: 22px
    letterSpacing: 0.02em
  body-md:
    fontFamily: Space Mono
    fontSize: 13px
    fontWeight: '400'
    lineHeight: 18px
    letterSpacing: 0.01em
  body-sm:
    fontFamily: Space Mono
    fontSize: 11px
    fontWeight: '400'
    lineHeight: 16px
    letterSpacing: 0em
  label-lg:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: '700'
    lineHeight: 16px
    letterSpacing: 0.08em
  label-md:
    fontFamily: JetBrains Mono
    fontSize: 10px
    fontWeight: '700'
    lineHeight: 14px
    letterSpacing: 0.06em
  label-sm:
    fontFamily: JetBrains Mono
    fontSize: 9px
    fontWeight: '700'
    lineHeight: 12px
    letterSpacing: 0.05em
spacing:
  gutter: 1rem
  gutter-mobile: 0.5rem
  gutter-desktop: 1.5rem
  margin: 1.5rem
  margin-mobile: 0.5rem
  margin-desktop: 2rem
  space-xs: 0.25rem
  space-sm: 0.5rem
  space-md: 0.75rem
  space-lg: 1.25rem
  space-xl: 2rem
---

## Brand & Style

This design system channels the visceral energy and tactile weight of golden-era 16-bit Super Nintendo (SNES) game interfaces, drawing direct inspiration from high-octane vehicular combat garage screens and classic arcade RPG management menus. The visual experience is immediate, mechanical, and uncompromising: structured metal-castings, rough speckled crimson carpeting, chisel-beveled stone plaques, and CRT phosphor luminance.

The design movement is **Tactile 16-Bit Skeuomorphic Arcade**:
- **Industrial Metagame Framing**: Heavy mechanical track rails, cold steel border girders, and inset beveled plaques delineate interface boundaries.
- **High-Density Tactile Modules**: Recessed inventory racks with illuminated bounding boxes (crimson for dormant, gold for active selection), discrete segmented pip meters, and chunky arcade hardware inputs.
- **CRT Phosphor Presence**: Deep pixel black voids contrasted with vivid primary accents (acid goblin green, cherry enamel red, cobalt thruster blue, incandescent gold, and cyan mana sparks).
- **Physical Weight**: Every UI element delivers the perception of mass, tactile depression, and analog switch engagement.

## Colors

The color architecture is derived from the high-contrast, limited-index palettes characteristic of 16-bit arcade hardware. Surfaces are anchored by industrial neutrals, vivid enamel primaries, and radiant phosphor indicators.

### Key Palettes
- **Primary (Crimson Enamel - `#e60000`)**: High-urgency alert lines, structural sub-panel highlights, item bracket frames, and hot machine telemetry.
- **Secondary (Trophy Gold - `#ffd700`)**: The active cursor index, selected equipment frames, currency tallies, and filled stat pips.
- **Tertiary (Goblin / Bio Green - `#00e676`)**: Mascot avatar accents, operational status confirmations, and bio-mechanical telemetry bars.
- **Neutral (Basalt & Cold Steel - `#121214`)**: Foundational chassis metal, recessed slot wells, and mechanical tread borders spanning from `#08080a` (deepest well) through `#494d55` (pressed plate) to `#8e929b` (chiseled edge highlight).

### Textured Ground & Canvases
- **Crimson Field (`#8a0c14` / `#4a0509`)**: A dense, dithered crimson texture forming the master workshop bay backdrop.
- **Stone Plaque Neutral (`#585b62`)**: Heavy granite and weathered cast-composite surface utilized exclusively for floating currency, pilot profile data, and vehicle specification headers.
- **Thruster Blue / Cyan (`#1a4fff` / `#00f0ff`)**: Auxiliary cooling lines, engine booster items, and secondary resource gauges.

## Typography

The typographic hierarchy replicates the bite, rhythm, and structural alignment of late-generation 16-bit arcade dialog screens and status matrices while ensuring production legibility across modern displays.

- **Display & Section Titles (`Space Grotesk`)**: Rendered in pure all-caps with generous horizontal tracking (`0.06em` to `0.08em`). Emulates broad-pixel marquee fonts, with hard-edge glyph footprints and instant readability against dithered backdrops.
- **Narrative Logs & Specifications (`Space Mono`)**: Monospaced terminal presentation reproducing classic SNES system text and upgrade descriptions. Maintains rigid vertical glyph columns so statistics and item descriptions align predictably.
- **Telemetry Readouts & Hardware Badges (`JetBrains Mono`)**: Heavyweight, compact technical monospaced figures tailored for monetary counts (`$40,000`), item tier counters, level brackets, and arcade controller button bindings.

### Text Styling Rules
- **Text Shadows**: High-prominence headers employ a pure hard drop shadow (`2px 2px 0px #08080a` or `1px 1px 0px #000000`) without blur, replicating rasterized hardware character generators.
- **Case**: Component headers, currency labels, vehicle titles, and action tags must be formatted in strict uppercase.
- **Numerics**: Numerical readouts enforce tabular figures (`tnum`) to eliminate layout jitter during currency accumulation and stat scaling.

## Layout & Spacing

The layout operates under a **Fixed 16:9 Arcade Staging Frame** enclosed by dual industrial tread columns on the outer left and right flanks.

### Screen Architecture
- **Flanking Girders**: Permanent vertical mechanical tracks (64px wide on desktop) anchor the screen edges, containing bolt patterns, interlocking metal teeth, and structural ribs.
- **Main Stage Area**: Enclosed within the flanking girders, using a structured 12-column grid.
  - **Upper Command Tier (Top 35% height)**: Split into the circular pilot/mascot badge on the left and the chiseled stone vehicle telemetry plaque on the right (`MONEY`, `CHASSIS CLASS`, `SELECTED UPGRADE`, `COST`).
  - **Lower Bay Matrix (Bottom 65% height)**: A recessed dark inspection stage framing the centerpiece machine, encircled by modular 1:1 hardware upgrade slots and an urgent exit escape trigger in the top-left corner.
- **Grid Adaptations**:
  - *Desktop (1024px+)*: Strict 16:9 aspect container centered in viewport, retaining authentic arcade composition.
  - *Tablet & Mobile*: Reflows vertically; the mascot and plaque consolidate into a top banner, and the hardware slots stack into a 2-column or 3-column equipment shelf beneath the primary unit display.

## Elevation & Depth

Depth is expressed through physical 16-bit skeuomorphism: directional bevels, chiseled relief, and hard-edged pixel contrast. Soft Gaussian blurs are strictly prohibited.

### 1. Inset Wells (Recessed Stage Depth)
- Main display bay interiors and empty module slots are countersunk into the structural floor.
- Rendered with hard inset edges: `inset 3px 3px 0px #08080a, inset -3px -3px 0px #494d55`.

### 2. Chiseled Stone Plaques (Raised Midground)
- Information plaques feature a 45-degree stone bevel simulating cut granite:
  - Top and left border: `#8a8f98` (illuminated stone).
  - Bottom and right border: `#1e2024` (cast shadow edge).
  - Face fill: `#585b62` with a subtle dithered noise overlay.

### 3. Active Phosphor Luminescence (Foreground Focus)
- Active inventory slots and interactive selectors do not use diffuse drop shadows. Instead, they produce a sharp double-stroke frame:
  - Selected state: `0 0 0 2px #ffd700, inset 0 0 0 1px #ffd700`.
  - Alert / Dormant state: `0 0 0 2px #e60000, inset 0 0 0 1px #8a0c14`.

### 4. Mechanical Tread Struts (Background Anchoring)
- Flanking vertical borders feature alternating 4px light and dark horizontal stepping bands to produce simulated metallic caterpillar tracks and chassis columns.

## Shapes

The design system enforces a strict **Sharp (`roundedness: 0`)** standard. All standard UI containers, button faces, item matrices, and status badges feature unrounded 90-degree corners to maintain authentic pixel grid fidelity.

### Shape Exceptions & Specialty Contours
- **Mascot / Pilot Emblem**: The solitary non-rectangular form factor is the circular cast medallion housing the pilot portrait (e.g., alien, goblin, or mechanical avatar), rimmed with a 3px stepped metallic border (`#1a4fff` cobalt ring with steel rivet notches).
- **Stepped Pixel Chamfers**: Selected modal windows and header tags utilize 2px stair-stepped corner notches (`clip-path` polygon cuts) to emulate 16-bit bitmap window borders.
- **Divider Scoring**: Interior container separators use 2px dual-tone carved grooves (`1px solid #1e2024` stacked over `1px solid #8e929b`).

## Components

### Hardware Slots (Inventory & Equipment Grid)
- **Geometry**: Strict square tiles (`64x64px` on desktop, `48x48px` on mobile) embedded within the vehicle staging bay.
- **Background**: Solid black void (`#08080a`) with faint metallic item silhouettes or module icons.
- **Frame States**:
  - *Standard / Dormant*: 2px solid boundary in `#e60000` (Crimson).
  - *Active / Cursor Hover*: 2px solid boundary in `#ffd700` (Trophy Gold) with rapid 2-frame yellow/white pixel border blinking.
  - *Empty / Locked*: 1px boundary in `#33373d` with an embossed central padlock icon.
- **Stat Pip Bar**: Positioned at the bottom-left or bottom-edge of each slot; a horizontal row of up to 7 discrete square pip blocks.
  - Active Level: `#ffd700` filled square.
  - Available Upgrade: `#e60000` filled square.
  - Unfilled / Cap: `#2c2e33` dark square.

### Exit / Action Buttons
- **Geometry**: Sharp rectangular block with inset text.
- **Visuals**: High-contrast white lettering (`Space Grotesk` bold) overlaid directly on a solid vermilion plate with a heavy 2px `#ffffff` top/left rim and `#08080a` bottom/right shadow.
- **Active State**: 1px downward and rightward translation (`transform: translate(1px, 1px)`) with inverted bevel.

### Stone Telemetry Plaque
- **Structure**: Multi-row specification sheet presenting vehicle name, funds, selected item, and upgrade fee.
- **Styling**: Weathered stone plate (`#585b62`) framed by a 3px chiseled rim (`#8a8f98` and `#1e2024`).
- **Typography Layout**:
  - Label keys (`MONEY:`, `UPGRADE:`) in crisp white monospaced uppercase with a 1px solid black drop shadow.
  - Value tallies (`$40,000`, `COBRA MARK VII`) positioned with deliberate indentations directly below or adjacent to keys.

### Pilot / Mascot Emblems
- Circular viewport containing pixel art character bust.
- Enclosed by a 3px deep blue/steel composite bezel.
- Flanked by twin mounting horns or metallic rivet lugs on the outer rim.

### Segmented Level / Stat Bars
- Horizontal gauge channels countersunk into stone or steel surfaces.
- Segmented into physical mechanical chunks (4px wide blocks separated by 1px gaps).
- Color coding:
  - *Armor / Structural Integrity*: Emerald Green (`#00e676`).
  - *Boost / Nitro Reserves*: Thruster Cyan (`#00f0ff`).
  - *Weapon Overheat / Engine Heat*: Hot Crimson (`#e60000`).

### Input Fields & Terminal Console
- Inset black well (`#08080a`) with 2px steel shadow rim.
- Text rendered in pure quartz white or phosphor yellow.
- Solid non-antialiased vertical block cursor (`█`) blinking at 500ms intervals.

### Arcade Controller Key Legends
- Miniaturized visual tags simulating arcade cabinet button caps (red, blue, yellow, green circles) and D-pad arrows.
- Displayed alongside action labels (e.g., `[A] BUY`, `[B] CANCEL`, `[X] TEST FIRE`) along the lower border of the chassis.