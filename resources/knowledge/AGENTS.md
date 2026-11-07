# Knowledge wiki

Reusable knowledge the owner and their assistant collect: sources read in
full, the ideas and organizations in them, and analyses and theses built on
them, for work, investing and anything else worth knowing again. Any agent the
owner works with may read it; write when the owner asks to keep or digest
something, following the workflow below.

What belongs elsewhere: facts about the owner, their family and the people
around them, their accounts, holdings, health and private conversations live in
the assistant's personal memory, never here. A thesis about a company belongs
here; how much of it the owner holds does not.

## Layout

```
raw/              sources as saved (articles, papers, filings); never edited
wiki/index.md     one line per page, by section; read it first
wiki/log.md       one line per ingest or change, newest last
wiki/sources/     one page per source
wiki/entities/    people in public life, companies, products, tools
wiki/concepts/    ideas, techniques, terms
wiki/syntheses/   analyses, comparisons and theses across sources
```

## Page format

```markdown
---
title: Page Title
type: source | entity | concept | synthesis
created: YYYY-MM-DD
updated: YYYY-MM-DD
sources: [source-slug]
---

# Page Title

Content, with [[wikilinks]] to related pages.
```

File names are lowercase slugs; a source page's slug ends with its year.

## Ingest

1. Save the source under `raw/` when it is a file or a page worth keeping.
2. Write its page in `wiki/sources/`: who published it and when, where it is,
   and what it says. Give each figure and claim with the source beside it, keep
   what the source claims apart from what you conclude, state your confidence
   (low, medium or high) and why, and name the source's interest in the matter
   when it has one, such as a seller praising its own product.
3. Update or create the entity, concept and synthesis pages it bears on, and
   note where it agrees with or contradicts what the wiki already says.
4. Add its line to `wiki/index.md` and to `wiki/log.md`, and commit.

## Query

Read `wiki/index.md`, open the pages that bear on the question, and cite them.
Pages here weigh more because the owner chose their sources, but answers may
draw on any source found elsewhere, cited the same way so the owner can check.
