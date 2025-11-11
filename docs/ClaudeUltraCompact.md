# Open-Stage - Ultra-Compact Reference
## For token-constrained contexts

---

## CORE INFO
- **ETL Framework** (Python 3.8+, MIT License)
- **29 components**: 5 base + 24 specialized
- **Architecture**: Pipes & Filters
- **Version**: 2.4 (Jan 2025)
- **Style**: 2-space indent

---

## BASE CLASSES (src/core/base.py)
```
DataPackage → Pipe → Origin (0→1) → Destination (1→0) → Node (M→N)
```

---

## COMPONENTS BY CATEGORY

**Origins (0→1)**: Generator, CSVOrigin, APIRestOrigin, OpenOrigin, MySQLOrigin, PostgresOrigin, GCPBigQueryOrigin

**Destinations (1→0)**: Printer, CSVDestination, MySQLDestination, PostgresDestination, GCPBigQueryDestination

**Routers**: Funnel (N→1), Switcher (1→N), Copy (1→N)

**Transformers (1→1)**: Filter, Aggregator, DeleteColumns, RemoveDuplicates, Joiner, Transformer

**AI (1→1)**: OpenAI, Anthropic, Gemini, DeepSeek transformers

---

## v2.4 ENHANCEMENTS

**All DB components**: before_query, after_query, timeout
**DB Origins**: table, max_results, query_parameters
**Transformer**: transformer_kwargs ✨

---

## 4-STEP WORKFLOW

1. **Enhance code** → Add features, validations, logging
2. **Create guide** → Follow existing pattern (see guides/)
3. **Update KB** → Catalog + Roadmap + Latest Updates
4. **Update README** → Table + Examples + Roadmap

---

## GUIDE TEMPLATE
```
# Title
🎯 Features | 📦 Install | 🚀 Basic | 🔧 Advanced
📊 Output | 📋 Params | ✅ Practices | ⚠️ Notes
```

---

## CODE PATTERNS

**Init**: super().__init__() → store params → validate → client=None
**Main**: lazy init → before_query → operation → after_query → cleanup
**Log**: ===70=== separators + emojis (✅❌📊⏱️📋)
**Validate**: check empty/type/positive → raise ValueError

---

## LOCATIONS
- Code: `src/{module}/`
- Guides: `/mnt/user-data/uploads/*.md`
- Output: `/mnt/user-data/outputs/`
- Work: `/home/claude/`

---

## HAS FULL DOCS
MySQL, Postgres, BigQuery (Origin+Dest), Transformer

## NEEDS DOCS
Filter, Aggregator, DeleteColumns, Joiner, RemoveDuplicates, Copy, Funnel, Switcher, Generator, CSVOrigin, CSVDestination, APIRestOrigin, OpenOrigin, Printer, 4x AI transformers

---

## NEXT: Filter component (most used, high value)

---

**Quick Start**: Read full KB first, read existing guides, propose, execute 4-step workflow