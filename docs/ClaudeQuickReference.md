# Open-Stage Framework - Quick Reference for Claude
## Version 2.4 | January 2025

---

## PROJECT ESSENTIALS

**What**: Enterprise ETL framework (Python) inspired by IBM DataStage
**Architecture**: Pipes & Filters pattern
**Components**: 29 total (5 base + 24 specialized)
**Authors**: Bernardo Colorado Dubois, Saul Hernandez Cordova
**License**: MIT
**Python**: 3.8+
**Style**: 2-space indentation

---

## CORE ARCHITECTURE (5 BASE CLASSES)

```
src/core/base.py:
├── DataPackage      # Encapsulates data + metadata
├── Pipe             # Connects components, transports data
├── Origin           # Abstract: data sources (0→1)
├── Destination      # Abstract: data sinks (1→0)
└── Node             # Abstract: transformers (M→N, inherits Origin + Destination)
```

**Key Pattern**: Origin.pump() → Pipe.flow() → Destination.sink() → (if Node) → Node.pump()

---

## COMPONENT CATEGORIES (24 SPECIALIZED)

### Origins (0→1) - 7 components
```
Core: Generator, CSVOrigin, APIRestOrigin, OpenOrigin
DB: MySQLOrigin, PostgresOrigin, GCPBigQueryOrigin
```

### Destinations (1→0) - 3 components
```
Core: Printer, CSVDestination
DB: MySQLDestination, PostgresDestination, GCPBigQueryDestination
```

### Routers (N↔M) - 3 components
```
Funnel (N→1), Switcher (1→N), Copy (1→N)
```

### Transformers (1→1) - 7 components
```
Core: Filter, Aggregator, DeleteColumns, RemoveDuplicates, Joiner, Transformer ✨
AI: OpenAIPromptTransformer, AnthropicPromptTransformer, 
    GeminiPromptTransformer, DeepSeekPromptTransformer
```

---

## RECENT ENHANCEMENTS (v2.4)

### Database Components ✅
**Pattern**: All DB components now support:
- `before_query`: SQL before main operation
- `after_query`: SQL after main operation  
- `timeout`: Query timeout in seconds
- Enhanced logging with emojis (✅ ❌ 📊 ⏱️ 📋)

**MySQL/PostgreSQL Origins also have**:
- `table`: Direct table read (no SELECT *)
- `max_results`: Automatic LIMIT
- `query_parameters`: Parameterized queries (dict with :param_name)

### Transformer Component ✅ NEW
**Enhancement**: Added `transformer_kwargs` (like Airflow's op_kwargs)
```python
Transformer(
    name="calc",
    transformer_function=my_func,
    transformer_kwargs={'param1': val1, 'param2': val2}  # ✨ NEW
)
```
**Features**: Function signature validation, enhanced logging, detailed errors

---

## ENHANCEMENT WORKFLOW (FOLLOW THIS)

When improving components, use this 4-step process:

### Step 1: Enhance Component
- Read existing code
- Add new features (follow DB enhancement pattern as reference)
- Add validations
- Enhance logging (use emojis)
- Update docstring with examples

### Step 2: Create Guide
**Pattern** (see existing guides in /guides/):
```markdown
# ComponentName - Usage Guide
## 🎯 Features
## 📦 Installation  
## 🚀 Basic Usage (Examples 1-3)
## 🔧 Advanced Features (Examples 4-10)
## 📊 Example Output
## 📋 Complete Parameters
## ✅ Best Practices
## ⚠️ Important Considerations
## 🔗 See Also
```

### Step 3: Update Knowledge Base
Add to appropriate section:
- Component catalog with parameters
- Core Features list
- Roadmap (Completed section)
- Latest Updates (version history)

### Step 4: Update README
- Component table (if description changed)
- Add usage example if significant
- Update roadmap section

---

## FILE LOCATIONS

```
src/
├── core/
│   ├── base.py           # 5 base classes
│   └── common.py         # 15 core components
├── mysql/common.py       # 2 MySQL components
├── postgres/common.py    # 2 PostgreSQL components
├── google/
│   ├── cloud.py          # 2 BigQuery components
│   └── gemini.py         # 1 Gemini transformer
├── anthropic/claude.py   # 1 Claude transformer
├── deepseek/deepseek.py  # 1 DeepSeek transformer
└── open_ai/chat_gpt.py   # 1 OpenAI transformer
```

---

## CODE PATTERNS TO FOLLOW

### Component Structure
```python
class MyComponent(Origin/Destination/Node):
  def __init__(self, name, required_params, optional_param=None):
    super().__init__()
    self.name = name
    # Store params
    # Validate params
    # Initialize as None (lazy init)
  
  def _initialize_client(self):  # Lazy initialization
    # Create external clients here
    pass
  
  def pump(self) / sink(self):   # Main logic
    try:
      # Initialize if needed
      # Execute before_query if applicable
      # Main operation with logging
      # Execute after_query if applicable
    except Exception as e:
      # Detailed error handling
    finally:
      # Cleanup
```

### Validation Pattern
```python
# Check not empty
if not param or not param.strip():
  raise ValueError(f"{self.name}: param cannot be empty")

# Check type
if not isinstance(param, expected_type):
  raise ValueError(f"{self.name}: param must be {expected_type}")

# Check positive numbers
if numeric_param <= 0:
  raise ValueError(f"{self.name}: param must be positive")
```

### Logging Pattern
```python
print(f"{'='*70}")
print(f"ComponentName '{self.name}' description...")
print(f"{'='*70}")
print(f"  📊 Statistics:")
print(f"     - Detail: {value}")
print(f"  ⏱️  Performance:")
print(f"     - Duration: {duration:.2f}s")
print(f"✅ ComponentName '{self.name}' completed successfully")
```

---

## QUICK REFERENCE: DB ENHANCEMENT PATTERN

This is the gold standard pattern (from MySQL/PostgreSQL v2.4):

**Origin Parameters to Add**:
```python
before_query: str = None      # SQL before extraction
after_query: str = None       # SQL after extraction  
table: str = None            # Direct table read
max_results: int = None      # Auto LIMIT
timeout: float = None        # Query timeout
query_parameters: dict = {}  # Parameterized queries
```

**Destination Parameters to Add**:
```python
before_query: str = None      # SQL before load
after_query: str = None       # SQL after load
timeout: float = None        # Query timeout
```

**Implementation**:
- Add `_execute_query(sql, description)` method
- Add `_build_query()` if supporting both query and table
- Use structured logging with separators and emojis
- Validate params in __init__
- Execute in order: before → main → after

---

## COMPONENT IMPROVEMENT PRIORITIES

**Ready to Enhance** (no guide yet):
1. Filter - Add custom operators
2. Aggregator - Add custom agg functions
3. DeleteColumns - Add pattern matching  
4. Joiner - Add more join types
5. RemoveDuplicates - Add more strategies
6. Copy - Add filtering
7. Funnel - Add merge strategies
8. Switcher - Add default routing
9. Generator - Add more patterns
10. CSVOrigin - Add encoding options
11. CSVDestination - Add compression
12. APIRestOrigin - Add auth methods
13. Printer - Add format options
14. OpenOrigin - Add validation

**AI Components** (need consistency review):
- All 4 use similar patterns, could add streaming

---

## USEFUL COMMANDS FOR CLAUDE

### To find component code:
```bash
grep -n "class ComponentName" /path/to/file.py
```

### To see existing guides:
```bash
ls -lh /mnt/user-data/uploads/*.md
```

### To copy for editing:
```bash
cp /mnt/user-data/uploads/file.md /home/claude/file.md
```

### To save outputs:
```bash
cp /home/claude/file.ext /mnt/user-data/outputs/file.ext
```

---

## CURRENT STATE SUMMARY

### Has Full Documentation (Guide + Knowledge Base)
- MySQLOrigin, MySQLDestination
- PostgresOrigin, PostgresDestination  
- GCPBigQueryOrigin, GCPBigQueryDestination
- Transformer ✨ NEW

### Has Code Only (No Guide)
- All other 21 components

### Next Suggested Enhancement
**Filter** - Most used transformer, adds value to document
- Current: 9 operators (<, >, <=, >=, !=, =, in, not in, between)
- Could add: regex, null checks, custom functions

---

## CRITICAL REMINDERS

1. **Always read existing guides first** to match format/style
2. **Follow 2-space indentation** consistently
3. **Use emojis in logging** for visual clarity (✅ ❌ 📊 ⏱️ 📋)
4. **Validate at initialization** (fail fast)
5. **Lazy initialize clients** (don't create in __init__)
6. **Clean resources** in finally blocks
7. **Keep guides concise** - focus on examples
8. **One component at a time** - avoid token saturation
9. **Test examples mentally** before documenting
10. **Update all 4 docs** (code + guide + KB + README)

---

## EXAMPLE INVOCATION FOR NEXT CHAT

**User will likely say**:
"Improve the [ComponentName] component"

**Claude should**:
1. Read this knowledge base
2. Read existing component code
3. Read 2-3 existing guides for pattern
4. Propose enhancements
5. Follow 4-step workflow
6. Generate all files
7. Update all documentation

**Key**: Ask for confirmation before starting work!

---

## CONTACT POINTS

- **Guides location**: `/mnt/user-data/uploads/*.md`
- **Output location**: `/mnt/user-data/outputs/`
- **Working directory**: `/home/claude/`
- **Skills available**: Check `/mnt/skills/` if needed

---

**STATUS**: Ready for next component enhancement
**LAST ENHANCED**: Transformer (v2.4)
**FRAMEWORK VERSION**: 2.4
**DATE**: January 2025

---

END OF QUICK REFERENCE