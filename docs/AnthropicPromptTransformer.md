# AnthropicPromptTransformer - Usage Guide

Node that sends a DataFrame to a Claude model and returns the transformed result as a new DataFrame. The entire cycle — DataFrame → CSV → LLM → CSV → DataFrame — is handled automatically.

---

## Features

- Sends the DataFrame as CSV to the model along with a natural language prompt
- Parses the model's CSV response back into a DataFrame
- Handles truncated responses by dropping the last incomplete row
- Strips markdown code blocks if the model returns them despite instructions

---

## Connectivity

```
1 input → 1 output
```

---

## Installation

```bash
pip install -e ".[anthropic]"
# or
pip install anthropic
```

---

## Basic Usage

### Example 1: Classify rows

```python
from open_stage.core.common import CSVOrigin, CSVDestination
from open_stage.anthropic.claude import AnthropicPromptTransformer
from open_stage.core.base import Pipe

origin = CSVOrigin("products", filepath_or_buffer="products.csv")

transformer = AnthropicPromptTransformer(
    name="classifier",
    model="claude-sonnet-4-6",
    api_key="YOUR_ANTHROPIC_API_KEY",
    prompt="Add a column 'segment' classifying each product as 'budget', 'mid-range', or 'premium' based on the price column."
)

dest = CSVDestination("out", path_or_buf="classified.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(transformer)
transformer.add_output_pipe(Pipe("p2")).set_destination(dest)
origin.pump()
```

---

### Example 2: Translate a column

```python
transformer = AnthropicPromptTransformer(
    name="translator",
    model="claude-sonnet-4-6",
    api_key="YOUR_ANTHROPIC_API_KEY",
    prompt="Translate the 'description' column from Spanish to English. Keep all other columns unchanged."
)
```

---

### Example 3: Extract structured data from free text

```python
transformer = AnthropicPromptTransformer(
    name="extractor",
    model="claude-sonnet-4-6",
    api_key="YOUR_ANTHROPIC_API_KEY",
    prompt=(
        "The 'notes' column contains free text. "
        "Extract two new columns: 'mentioned_product' and 'sentiment' (positive/negative/neutral). "
        "If the information is not present, use empty string."
    )
)
```

---

### Example 4: Use a lighter model to reduce cost

```python
transformer = AnthropicPromptTransformer(
    name="tagger",
    model="claude-haiku-4-5-20251001",
    api_key="YOUR_ANTHROPIC_API_KEY",
    prompt="Add a 'category' column based on the 'title' column.",
    max_tokens=4096
)
```

---

### Example 5: Load API key from environment

```python
import os

transformer = AnthropicPromptTransformer(
    name="classifier",
    model="claude-sonnet-4-6",
    api_key=os.environ["ANTHROPIC_API_KEY"],
    prompt="Classify each row..."
)
```

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | Yes | — | Component name |
| `model` | str | Yes | — | Claude model ID (e.g. `'claude-sonnet-4-6'`) |
| `api_key` | str | Yes | — | Anthropic API key |
| `prompt` | str | Yes | — | Natural language instruction for the transformation |
| `max_tokens` | int | No | `16000` | Maximum tokens in the response |

---

## How it works

1. Receives a DataFrame and serializes it to CSV
2. Sends the CSV + `prompt` to the model as the user message
3. A fixed system message instructs the model to always respond in raw CSV format
4. The response CSV is parsed back into a DataFrame and sent downstream

The model runs at `temperature=0.0` for deterministic output.

---

## Recommended models

| Model | Best for |
|-------|----------|
| `claude-opus-4-7` | Most complex reasoning, highest accuracy |
| `claude-sonnet-4-6` | Best balance of quality and cost |
| `claude-haiku-4-5-20251001` | Simple tasks, lowest cost and latency |

---

## Writing effective prompts

- Reference column names explicitly
- Describe the output columns and their expected values
- Specify what to do when the transformation doesn't apply to a row

**Good prompt:**
```
Add a column 'risk_level' with values 'low', 'medium', or 'high' based on the 'amount' column:
low < 100, medium 100–1000, high > 1000. Keep all other columns unchanged.
```

---

## Considerations

- **Input size**: the entire DataFrame is sent as CSV in a single request — for large DataFrames, split rows first using `Filter` or `Switcher`
- **Truncated responses**: if the model hits `max_tokens`, the last incomplete row is automatically dropped and a warning is logged
- **Output schema**: the response shape depends on the prompt and the model — validate output columns downstream if needed
- **API key**: never hardcode keys in source files — use environment variables or a secrets manager

---

## See Also

- [OpenAIPromptTransformer](./OpenAIPromptTransformer.md) - Same interface using OpenAI models
- [DeepSeekPromptTransformer](./DeepSeekPromptTransformer.md) - Same interface using DeepSeek models
- [GeminiPromptTransformer](./GeminiPromptTransformer.md) - Same interface using Gemini models
- [Open-Stage Documentation](../README.md) - Complete documentation
