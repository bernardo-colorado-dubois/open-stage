# Open-Stage Knowledge Bases - Usage Guide
## How to use each knowledge base effectively

---

## 📚 AVAILABLE KNOWLEDGE BASES

### 1. OPEN_STAGE_KNOWLEDGE_BASE.md (Full)
**Size**: ~800 lines
**When to use**: 
- Starting new major features
- Need complete component specifications
- Reference for advanced patterns
- Understanding architecture deeply

**Contains**:
- Complete component catalog with all parameters
- Detailed examples for each component
- Design principles and patterns
- Full roadmap and version history
- Database enhancement patterns
- Error handling details

**Best for**: Initial context loading, comprehensive reference

---

### 2. CLAUDE_QUICK_REFERENCE.md (Optimized)
**Size**: ~400 lines
**When to use**: ⭐ **RECOMMENDED FOR MOST TASKS**
- Improving existing components
- Creating new component guides
- Following established patterns
- Day-to-day development

**Contains**:
- Essential project info
- Component categories overview
- 4-step enhancement workflow
- Code patterns to follow
- Quick command references
- Component priority list
- Current state summary

**Best for**: Regular component enhancement work

---

### 3. CLAUDE_ULTRA_COMPACT.md (Minimal)
**Size**: ~80 lines
**When to use**: 
- Token budget is critical
- Quick status check
- Simple updates
- Mid-conversation refresh

**Contains**:
- Core info only
- Component list
- Latest enhancements
- Workflow steps
- Key locations

**Best for**: Token-constrained contexts, quick reference

---

## 🎯 DECISION TREE

```
START
  │
  ├─ New to project? 
  │   └─ YES → Use FULL KB
  │   └─ NO → Continue
  │
  ├─ Enhancing component?
  │   └─ YES → Use QUICK REFERENCE ⭐
  │   └─ NO → Continue
  │
  ├─ Creating new component?
  │   └─ YES → Use FULL KB
  │   └─ NO → Continue
  │
  ├─ Token budget low?
  │   └─ YES → Use ULTRA COMPACT
  │   └─ NO → Use QUICK REFERENCE
  │
  └─ Simple question?
      └─ YES → Use ULTRA COMPACT
      └─ NO → Use QUICK REFERENCE
```

---

## 📋 TASK-SPECIFIC RECOMMENDATIONS

### Task: "Improve the Filter component"
**Load**: QUICK REFERENCE → Read existing guides → Start work
**Reason**: Has workflow, patterns, and component priorities

### Task: "Create a new MongoDB Origin"
**Load**: FULL KB → Read DB enhancement patterns → Start work
**Reason**: Need complete architecture understanding and patterns

### Task: "Update README with new feature"
**Load**: ULTRA COMPACT → Check locations → Make updates
**Reason**: Simple task, minimal context needed

### Task: "Review all AI transformers for consistency"
**Load**: FULL KB → Analyze patterns → Create plan
**Reason**: Need complete component specs for comparison

### Task: "Fix bug in Transformer"
**Load**: QUICK REFERENCE → Read component code → Fix
**Reason**: Has essential patterns and code structure

### Task: "Create guide for existing component"
**Load**: QUICK REFERENCE → Read existing guides → Follow pattern
**Reason**: Has guide template and workflow steps

---

## 💡 OPTIMIZATION STRATEGIES

### Strategy 1: Progressive Loading
1. Start with ULTRA COMPACT (get oriented)
2. If need more → Load QUICK REFERENCE
3. If still need more → Load FULL KB

### Strategy 2: Reference Swapping
- Load QUICK REFERENCE initially
- If hit knowledge gap → Ask user for specific section from FULL KB
- Continue with minimal context

### Strategy 3: Targeted Loading
- Use ULTRA COMPACT to identify what you need
- Ask user to provide only that specific section from FULL KB
- More efficient than loading entire document

---

## 🔄 VERSION SYNC

All three knowledge bases are synced to **v2.4** (January 2025).

**When updated**: All three must be updated together:
1. Component enhanced → Update FULL KB details
2. Extract essentials → Update QUICK REFERENCE
3. Update summary → Update ULTRA COMPACT

**Consistency check**:
- Component count: 29
- Latest enhancement: Transformer (transformer_kwargs)
- Version: 2.4

---

## 📊 COMPARISON TABLE

| Feature | Full KB | Quick Ref | Ultra Compact |
|---------|---------|-----------|---------------|
| **Size** | ~800 lines | ~400 lines | ~80 lines |
| **Load Time** | ~30K tokens | ~15K tokens | ~3K tokens |
| **Detail Level** | Complete | Essential | Minimal |
| **Examples** | Extensive | Key ones | None |
| **Patterns** | All | Critical | Summary |
| **Use Frequency** | 10% | 80% ⭐ | 10% |
| **Best For** | Deep work | Daily tasks | Quick checks |

---

## ✅ RECOMMENDATIONS FOR USERS

When starting a new chat with Claude about Open-Stage:

**For component enhancement** (most common):
```
"I have the Open-Stage project. Read CLAUDE_QUICK_REFERENCE.md 
and let's improve the [ComponentName] component."
```

**For new features**:
```
"I have the Open-Stage project. Read OPEN_STAGE_KNOWLEDGE_BASE.md 
and I want to add [new capability]."
```

**For quick updates**:
```
"I have the Open-Stage project. Read CLAUDE_ULTRA_COMPACT.md
and help me update [specific thing]."
```

---

## 🎯 EFFICIENCY TIPS

1. **Start small**: Load ULTRA COMPACT first, upgrade if needed
2. **Be specific**: Tell Claude which KB to use
3. **Context matters**: Choose based on task complexity
4. **Token awareness**: Monitor usage, swap to smaller KB if approaching limit
5. **Incremental loading**: Load QUICK REF → Ask for specific sections from FULL KB if needed

---

## 📝 FOR CLAUDE IN FUTURE CHATS

When user says "improve component X":
1. Ask: "Should I load QUICK REFERENCE (recommended) or FULL KB?"
2. If no answer → Default to QUICK REFERENCE
3. If need more info → Request specific section from FULL KB

When user says "create new component":
1. Load FULL KB (need architecture understanding)
2. Read relevant existing components
3. Proceed with development

When user says "quick update":
1. Load ULTRA COMPACT
2. Make changes
3. Done

---

## 🔗 FILES LOCATION

All knowledge bases are in: `/mnt/user-data/outputs/`

```
OPEN_STAGE_KNOWLEDGE_BASE.md    ← Full version
CLAUDE_QUICK_REFERENCE.md        ← Recommended ⭐
CLAUDE_ULTRA_COMPACT.md          ← Minimal version
```

---

**Created**: January 2025
**Purpose**: Optimize Claude's context loading for Open-Stage project
**Result**: 60-80% token savings for routine tasks

---

END OF USAGE GUIDE