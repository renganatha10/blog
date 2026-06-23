# Blog

A static blog built with [Zola](https://www.getzola.org/).

## Prerequisites

Zola is installed at `~/.local/bin/zola`. If you need to reinstall:

```bash
curl -sL https://github.com/getzola/zola/releases/download/v0.19.2/zola-v0.19.2-x86_64-unknown-linux-gnu.tar.gz \
  | tar -xz -C ~/.local/bin
```

## Run locally

```bash
~/.local/bin/zola serve
```

Open [http://127.0.0.1:1111](http://127.0.0.1:1111). The site live-reloads on file changes.

## Build for production

```bash
~/.local/bin/zola build
```

Output goes to the `public/` directory.

## Writing a post

Create a Markdown file under `content/blog/`:

```bash
touch content/blog/my-new-post.md
```

Minimal front matter:

```markdown
+++
title = "My New Post"
date = 2026-06-22
description = "One-line summary shown in listings."
[taxonomies]
tags = ["example"]
+++

Your content here...
```

## Project structure

```
content/       # Markdown source files
  blog/        # blog posts
  about.md     # about page
sass/          # stylesheets (compiled automatically)
templates/     # Tera HTML templates
config.toml    # site settings (title, base_url, etc.)
public/        # build output — do not commit
```
