# GeminiPromptTransformer - Usage Guide

Node that sends a DataFrame to a Gemini model and returns the transformed result as a new DataFrame. The entire cycle — DataFrame → CSV → LLM → CSV → DataFrame — is handled automatically.

---

## Features

- Sends the DataFrame as CSV to the model along with a natural language prompt
- Parses the model's CSV response back into a DataFrame
- Strips markdown code blocks if the model returns them despite instructions
- Uses `temperature=0.0`, `top_p=0.95`, `top_k=40` for consistent output

---

## Connectivity

```
1 input → 1 output
```

---

## Installation

```bash
pip install -e ".[gemini]"
# or
pip install google-generativeai
```

---

## Basic Usage

### Example 1: Classify rows

```python
from open_stage.core.common import CSVOrigin, CSVDestination
from open_stage.google.gemini import GeminiPromptTransformer
from open_stage.core.base import Pipe

origin = CSVOrigin("products", filepath_or_buffer="products.csv")

transformer = GeminiPromptTransformer(
    name="classifier",
    model="gemini-2.0-flash",
    api_key="YOUR_GEMINI_API_KEY",
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
transformer = GeminiPromptTransformer(
    name="translator",
    model="gemini-2.0-flash",
    api_key="YOUR_GEMINI_API_KEY",
    prompt="Translate the 'description' column from Spanish to English. Keep all other columns unchanged."
)
```

---

### Example 3: Extract structured data from free text

```python
transformer = GeminiPromptTransformer(
    name="extractor",
    model="gemini-2.0-flash",
    api_key="YOUR_GEMINI_API_KEY",
    prompt=(
        "The 'notes' column contains free text. "
        "Extract two new columns: 'mentioned_product' and 'sentiment' (positive/negative/neutral). "
        "If the information is not present, use empty string."
    )
)
```

---

### Example 4: Use a more powerful model for complex tasks

```python
transformer = GeminiPromptTransformer(
    name="reasoner",
    model="gemini-1.5-pro",
    api_key="YOUR_GEMINI_API_KEY",
    prompt="Analyze the 'description' column and infer the most likely industry sector for each row.",
    max_tokens=8192
)
```

---

### Example 5: Load API key from environment

```python
import os

transformer = GeminiPromptTransformer(
    name="classifier",
    model="gemini-2.0-flash",
    api_key=os.environ["GEMINI_API_KEY"],
    prompt="Classify each row..."
)
```

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | Yes | — | Component name |
| `model` | str | Yes | — | Gemini model ID (e.g. `'gemini-2.0-flash'`) |
| `api_key` | str | Yes | — | Gemini API key |
| `prompt` | str | Yes | — | Natural language instruction for the transformation |
| `max_tokens` | int | No | `16000` | Maximum output tokens (`max_output_tokens`) |

---

## How it works

1. Receives a DataFrame and serializes it to CSV
2. Sends the CSV + `prompt` to the model as the user message
3. The system instruction — telling the model to always respond in raw CSV — is set at client initialization
4. The response CSV is parsed back into a DataFrame and sent downstream

Generation config: `temperature=0.0`, `top_p=0.95`, `top_k=40`.

---

## Recommended models

| Model | Best for |
|-------|----------|
| `gemini-2.0-flash` | Fast, cost-effective, good general performance |
| `gemini-1.5-pro` | Complex reasoning, large context windows |
| `gemini-1.5-flash` | Simple tasks, lowest latency |

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

- **Truncation detection**: unlike other transformers, Gemini does not report a truncation signal — responses are treated as complete regardless of length
- **Input size**: the entire DataFrame is sent as CSV in a single request — for large DataFrames, split rows first using `Filter` or `Switcher`
- **Output schema**: the response shape depends on the prompt and the model — validate output columns downstream if needed
- **API key**: never hardcode keys in source files — use environment variables or a secrets manager

---

## See Also

- [AnthropicPromptTransformer](./AnthropicPromptTransformer.md) - Same interface using Claude models
- [OpenAIPromptTransformer](./OpenAIPromptTransformer.md) - Same interface using OpenAI models
- [DeepSeekPromptTransformer](./DeepSeekPromptTransformer.md) - Same interface using DeepSeek models
- [Open-Stage Documentation](../README.md) - Complete documentation
