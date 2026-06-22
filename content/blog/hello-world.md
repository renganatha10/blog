+++
title = "Hello, World!"
date = 2026-06-22
description = "The first post on this blog."
[taxonomies]
tags = ["meta"]
+++

Welcome to my blog! This is the first post, written in Markdown.

## Getting Started

Posts live in `content/blog/`. Each file starts with a TOML front matter block between `+++` delimiters.

```markdown
+++
title = "My Post"
date = 2026-06-22
description = "A short summary."
[taxonomies]
tags = ["example"]
+++

Your content here...
```

## Running Locally

```bash
zola serve
```

Then open `http://127.0.0.1:1111`.
